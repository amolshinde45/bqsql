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

        # Initialize BigQuery Storage client
        self.client = bigquery_storage.BigQueryReadClient(
            credentials=config.BQ_CREDENTIALS if hasattr(config, 'BQ_CREDENTIALS') else None
        )

        # Initialize GCS filesystem with optimizations
        self.gcs = gcsfs.GCSFileSystem(
            project=self.config.PROJECT_ID,
            token=config.BQ_CREDENTIALS if hasattr(config, 'BQ_CREDENTIALS') else None,
            asynchronous=False,
            block_size=32 * 1024 * 1024  # 32MB blocks for better throughput
        )

        # CSV write options - optimized for speed
        self.write_options = pv.WriteOptions(
            include_header=False,
            delimiter='|',
            quoting_style="none"
        )

        # Target file size
        self.limit_bytes = self.config.TARGET_FILE_SIZE_MB * 1024 * 1024

        # Performance tracking
        self.stats = {
            'rows_processed': 0,
            'bytes_written': 0,
            'files_created': 0,
            'start_time': None
        }

    def _log(self, msg):
        """Send log message to queue"""
        self.log_queue.put(
            f"[Extractor-{self.table_cfg.target_table}-W{self.worker_id}] {msg}"
        )

    def _clean_batch(self, arrow_batch):
        """
        Clean and transform Arrow batch for SQL Server compatibility
        Optimized for performance
        """
        new_columns = []

        for field in arrow_batch.schema:
            col = arrow_batch[field.name]

            # Type conversions
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

            # Cast everything to string for CSV
            col = col.cast(pa.string())

            # Remove double quotes (they can break pipe-delimited format)
            col = pc.replace_substring(col, pattern='"', replacement='')

            # Replace pipe characters if they exist in data
            col = pc.replace_substring(col, pattern='|', replacement='')

            # Replace newlines and carriage returns
            col = pc.replace_substring(col, pattern='\n', replacement=' ')
            col = pc.replace_substring(col, pattern='\r', replacement=' ')

            # Handle nulls - replace with empty string
            col = pc.fill_null(col, "")

            new_columns.append(col)

        return pa.RecordBatch.from_arrays(
            new_columns,
            names=arrow_batch.schema.names
        )

    def _open_new_gcs_file(self, stream_idx, part_idx, schema):
        """
        Open new GCS file for writing
        Returns: (file_object, csv_writer, gcs_path)
        """
        gcs_path = (
            f"gs://{self.config.GCS_BUCKET}/{self.table_cfg.dataset_id}/{self.table_cfg.target_table}/"
            f"pipeline_exports/{self.table_cfg.target_table}_w{self.worker_id:03d}_"
            f"s{stream_idx:03d}_p{part_idx:03d}.csv"
        )

        file_obj = self.gcs.open(
            gcs_path,
            'wb',
            block_size=32 * 1024 * 1024  # 32MB buffer
        )

        writer = pv.CSVWriter(
            file_obj,
            schema,
            write_options=self.write_options
        )

        return file_obj, writer, gcs_path

    def _process_stream(self, stream_name, stream_idx):
        """
        Process a single BigQuery Storage API stream
        Writes data to GCS in chunks
        """
        reader = self.client.read_rows(stream_name)

        rows_in_stream = 0
        part_idx = 1
        file_obj, writer, gcs_path = None, None, None
        last_log_time = time.time()

        try:
            for page in reader.rows().pages:
                batch = self._clean_batch(page.to_arrow())

                if file_obj is None:
                    file_obj, writer, gcs_path = self._open_new_gcs_file(
                        stream_idx, part_idx, batch.schema
                    )

                writer.write_batch(batch)
                rows_in_stream += batch.num_rows

                if time.time() - last_log_time > 10:
                    current_size_mb = file_obj.tell() / (1024 * 1024)
                    self._log(f"Stream {stream_idx}: {current_size_mb:.2f}MB written to {os.path.basename(gcs_path)}")
                    last_log_time = time.time()

                current_size = file_obj.tell()
                if current_size >= self.limit_bytes:
                    writer.close()
                    file_obj.close()
                    self._log(f"Completed file: {os.path.basename(gcs_path)}")

                    self.file_queue.put((gcs_path, self.table_cfg))

                    self.stats['files_created'] += 1
                    self.stats['bytes_written'] += current_size

                    part_idx += 1
                    file_obj, writer, gcs_path = self._open_new_gcs_file(
                        stream_idx, part_idx, batch.schema
                    )
                    last_log_time = time.time()

            if writer and file_obj:
                writer.close()
                file_obj.close()
                self._log(f"Completed file: {os.path.basename(gcs_path)}")

                self.file_queue.put((gcs_path, self.table_cfg))

                self.stats['files_created'] += 1
                final_size = file_obj.tell() if hasattr(file_obj, 'tell') else 0
                self.stats['bytes_written'] += final_size

        except Exception as e:
            self._log(f"ERROR processing stream {stream_idx}: {str(e)}")

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

        return rows_in_stream

    def run(self, stream_chunk):
        """
        Main worker method - processes assigned streams
        """
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
                self._log(f"CRITICAL ERROR on stream {stream_name}: {str(e)}")
                continue

        elapsed = time.time() - self.stats['start_time']
        throughput = total_rows / elapsed if elapsed > 0 else 0

        summary = (
            f"Worker {self.worker_id} COMPLETE: "
            f"{total_rows:,} rows, "
            f"{self.stats['files_created']} files, "
            f"{self.stats['bytes_written'] / (1024 ** 3):.2f} GB, "
            f"{elapsed:.1f}s, "
            f"{throughput:,.0f} rows/s"
        )

        self._log(summary)
        return summary


def extractor_worker(args):
    """
    Multiprocessing worker entry point
    """
    stream_chunk, worker_id, config, table_cfg, file_queue, log_queue = args

    extractor = BqExtractor(
        worker_id=worker_id,
        config=config,
        table_cfg=table_cfg,
        log_queue=log_queue,
        file_queue=file_queue
    )

    return extractor.run(stream_chunk)
