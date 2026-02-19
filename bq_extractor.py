# bq_extractor.py - BigQuery Data Extraction

import os
import time
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.compute as pc
from google.cloud import bigquery_storage
import gcsfs


class BqExtractor:
    def __init__(self, worker_id, config, table_cfg, log_queue, file_queue):
        self.worker_id = worker_id
        self.config = config
        self.table_cfg = table_cfg
        self.log_queue = log_queue
        self.file_queue = file_queue

        self.client = bigquery_storage.BigQueryReadClient(
            credentials=config.BQ_CREDENTIALS if hasattr(config, 'BQ_CREDENTIALS') else None
        )

        self.gcs = gcsfs.GCSFileSystem(
            project=self.config.PROJECT_ID,
            token=config.BQ_CREDENTIALS if hasattr(config, 'BQ_CREDENTIALS') else None,
            asynchronous=False,
            block_size=32 * 1024 * 1024
        )

        self.write_options = pv.WriteOptions(
            include_header=False,
            delimiter='|',
            quoting_style="none"
        )

        self.limit_bytes = self.config.TARGET_FILE_SIZE_MB * 1024 * 1024

        self.stats = {
            'rows_processed': 0,
            'bytes_written': 0,
            'files_created': 0,
            'start_time': None
        }

    def _log(self, msg):
        self.log_queue.put(
            f"[Extractor-{self.table_cfg.target_table}-W{self.worker_id}] {msg}"
        )

    def _clean_batch(self, arrow_batch):
        """Clean and transform Arrow RecordBatch for SQL Server compatibility."""
        new_columns = []
        for field in arrow_batch.schema:
            col = arrow_batch[field.name]

            if pa.types.is_date(field.type):
                col = pc.strftime(col, format='%Y-%m-%d')
            elif pa.types.is_timestamp(field.type):
                col = pc.strftime(col, format='%Y-%m-%d %H:%M:%S')
            elif pa.types.is_boolean(field.type):
                col = pc.if_else(col, "1", "0")
            elif pa.types.is_decimal(field.type):
                pass
            elif pa.types.is_time(field.type):
                col = pc.strftime(col, format='%H:%M:%S')

            col = col.cast(pa.string())
            col = pc.replace_substring(col, pattern='"', replacement='')
            col = pc.replace_substring(col, pattern='|', replacement='')
            col = pc.replace_substring(col, pattern='\n', replacement=' ')
            col = pc.replace_substring(col, pattern='\r', replacement=' ')
            col = pc.fill_null(col, "")
            new_columns.append(col)

        return pa.RecordBatch.from_arrays(new_columns, names=arrow_batch.schema.names)

    def _open_new_gcs_file(self, stream_idx, part_idx, schema):
        """Open a new GCS file for writing. Returns (file_obj, writer, gcs_path)."""
        gcs_path = (
            f"gs://{self.config.GCS_BUCKET}/{self.table_cfg.dataset_id}/{self.table_cfg.target_table}/"
            f"pipeline_exports/{self.table_cfg.target_table}_w{self.worker_id:03d}_"
            f"s{stream_idx:03d}_p{part_idx:03d}.csv"
        )
        file_obj = self.gcs.open(gcs_path, 'wb', block_size=32 * 1024 * 1024)
        writer = pv.CSVWriter(file_obj, schema, write_options=self.write_options)
        return file_obj, writer, gcs_path

    def _close_and_queue_file(self, file_obj, writer, gcs_path, stream_idx, part_idx, written_bytes):
        """Close current GCS file and queue it for loading."""
        writer.close()
        file_obj.close()

        size_mb = written_bytes / (1024 * 1024)
        self._log(
            f"Stream {stream_idx}: Queued {os.path.basename(gcs_path)} "
            f"({size_mb:.1f} MB, part {part_idx})"
        )
        self.file_queue.put((gcs_path, self.table_cfg))
        self.stats['files_created'] += 1
        self.stats['bytes_written'] += written_bytes

    def _process_stream(self, stream_name, stream_idx):
        """
        Process a single BQ Storage API stream using ARROW format correctly.

        IMPORTANT: Uses direct gRPC response iteration with response.arrow_record_batch()
        NOT reader.rows().pages - that pattern is for AVRO and causes gRPC hangs
        on ARROW streams when waiting for page boundaries that never arrive.

        Uses self-tracked byte counter (NOT file_obj.tell()) since gcsfs
        resets the buffer pointer to 0 after close().
        """
        # Timeout per stream: 10 minutes max - prevents indefinite hangs
        STREAM_TIMEOUT = 600

        file_obj = None
        writer = None
        gcs_path = None
        bytes_in_current_file = 0
        rows_in_stream = 0
        response_count = 0
        part_idx = 1

        stream_start = time.time()
        last_log_time = time.time()

        self._log(f"Stream {stream_idx}: START reading")

        try:
            # CORRECT: iterate gRPC responses directly with timeout
            # DataFormat.ARROW -> use response.arrow_record_batch()
            # DO NOT use reader.rows().pages (AVRO pattern - hangs on ARROW streams)
            responses = self.client.read_rows(stream_name, timeout=STREAM_TIMEOUT)

            for response in responses:
                # Extract Arrow batch directly from gRPC response
                raw_batch = response.arrow_record_batch()

                if raw_batch.num_rows == 0:
                    continue

                # Clean the batch for SQL Server compatibility
                batch = self._clean_batch(raw_batch)
                response_count += 1
                rows_in_stream += batch.num_rows
                page_bytes = batch.nbytes  # Arrow in-memory size (proxy for CSV size)

                # Open GCS file on first response
                if file_obj is None:
                    file_obj, writer, gcs_path = self._open_new_gcs_file(
                        stream_idx, part_idx, batch.schema
                    )
                    bytes_in_current_file = 0
                    self._log(
                        f"Stream {stream_idx}: Opened part {part_idx} -> "
                        f"{os.path.basename(gcs_path)}"
                    )

                # Write to CSV
                writer.write_batch(batch)
                bytes_in_current_file += page_bytes

                # Progress log every 5 seconds
                if time.time() - last_log_time >= 5:
                    elapsed = time.time() - stream_start
                    rps = rows_in_stream / elapsed if elapsed > 0 else 0
                    file_mb = bytes_in_current_file / (1024 * 1024)
                    self._log(
                        f"Stream {stream_idx} | Response {response_count} | "
                        f"Rows: {rows_in_stream:,} ({rps:,.0f} r/s) | "
                        f"File: {file_mb:.1f}/{self.config.TARGET_FILE_SIZE_MB} MB | "
                        f"Part: {part_idx}"
                    )
                    last_log_time = time.time()

                # Rotate file when size threshold crossed
                if bytes_in_current_file >= self.limit_bytes:
                    self._close_and_queue_file(
                        file_obj, writer, gcs_path,
                        stream_idx, part_idx, bytes_in_current_file
                    )
                    part_idx += 1
                    file_obj, writer, gcs_path = self._open_new_gcs_file(
                        stream_idx, part_idx, batch.schema
                    )
                    bytes_in_current_file = 0
                    self._log(
                        f"Stream {stream_idx}: Rotated -> part {part_idx} "
                        f"{os.path.basename(gcs_path)}"
                    )
                    last_log_time = time.time()

            # Queue final file (always - even if small)
            if file_obj is not None and writer is not None:
                self._close_and_queue_file(
                    file_obj, writer, gcs_path,
                    stream_idx, part_idx, bytes_in_current_file
                )

        except Exception as e:
            self._log(f"ERROR in stream {stream_idx}: {str(e)}")
            if writer:
                try:
                    writer.close()
                except:
                    pass
            if file_obj:
                try:
                    file_obj.close()
                except:
                    pass
            raise

        stream_elapsed = time.time() - stream_start
        throughput = rows_in_stream / stream_elapsed if stream_elapsed > 0 else 0
        self._log(
            f"Stream {stream_idx}: DONE | {rows_in_stream:,} rows | "
            f"{response_count} responses | {part_idx} file(s) | "
            f"{stream_elapsed:.1f}s | {throughput:,.0f} rows/s"
        )
        return rows_in_stream

    def run(self, stream_chunk):
        """Main worker method - processes assigned streams sequentially."""
        self.stats['start_time'] = time.time()
        self._log(f"Worker started. Processing {len(stream_chunk)} streams.")

        total_rows = 0

        for i, stream_name in enumerate(stream_chunk, 1):
            try:
                rows = self._process_stream(stream_name, i)
                total_rows += rows
                self.stats['rows_processed'] += rows
                self._log(f"Completed stream {i}/{len(stream_chunk)}: {rows:,} rows")
            except Exception as e:
                self._log(f"CRITICAL ERROR on stream {i}: {str(e)}")
                continue

        elapsed = time.time() - self.stats['start_time']
        throughput = total_rows / elapsed if elapsed > 0 else 0
        summary = (
            f"Worker {self.worker_id} COMPLETE: "
            f"{total_rows:,} rows | "
            f"{self.stats['files_created']} files | "
            f"{self.stats['bytes_written'] / (1024 ** 3):.2f} GB | "
            f"{elapsed:.1f}s | "
            f"{throughput:,.0f} rows/s"
        )
        self._log(summary)
        return summary


def extractor_worker(args):
    """Multiprocessing worker entry point."""
    stream_chunk, worker_id, config, table_cfg, file_queue, log_queue = args
    extractor = BqExtractor(
        worker_id=worker_id,
        config=config,
        table_cfg=table_cfg,
        log_queue=log_queue,
        file_queue=file_queue
    )
    return extractor.run(stream_chunk)
