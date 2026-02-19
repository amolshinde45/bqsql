# main_orchestrator.py - Pipeline Orchestration

import os
import json
import time
import multiprocessing
import sys
import queue
from datetime import datetime
from collections import defaultdict
from config_manager import ConfigManager
from bq_extractor import extractor_worker
from sql_loader import loader_worker
from google.cloud import bigquery_storage
import gcsfs


class MainOrchestrator:
    def __init__(self, config_path):
        self.config_manager = ConfigManager(config_path)
        self.global_cfg = self.config_manager.global_config
        self.limits_cfg = self.config_manager.limits_config
        self.table_configs = self.config_manager.table_configs

        # Shared queues
        self.manager = multiprocessing.Manager()
        self.file_queue = self.manager.Queue(maxsize=1000)
        self.log_queue = self.manager.Queue()
        self.archive_queue = self.manager.Queue()

        # Statistics tracking
        self.stats = self.manager.dict()
        self.stats_lock = self.manager.Lock()
        self.stats['total_files_extracted'] = 0
        self.stats['total_files_loaded'] = 0
        self.stats['total_rows'] = 0
        self.stats['tables_completed'] = 0
        self.stats['total_extraction_time'] = 0.0
        self.stats['total_load_time'] = 0.0

    def _log(self, msg, prefix="ORCHESTRATOR"):
        """Send log message to queue"""
        self.log_queue.put(f"[{prefix}] {msg}")

    def run(self):
        """Main pipeline execution"""
        start_time = datetime.now()

        print("=" * 80)
        print(f"ULTRA-FAST BIGQUERY TO SQL SERVER PIPELINE")
        print(f"Started at: {start_time}")
        print("=" * 80)

        self._log("--- Pipeline Initialized ---")
        self._log(f"Tables to process: {len(self.table_configs)}")
        self._log(f"Max concurrent extractors: {self.limits_cfg.MAX_CONCURRENT_EXTRACT}")
        self._log(f"Max concurrent loaders: {self.limits_cfg.MAX_CONCURRENT_LOAD}")

        # Start monitoring processes
        log_process = multiprocessing.Process(
            target=logger_listener,
            args=(self.log_queue, self.global_cfg)
        )
        archive_process = multiprocessing.Process(
            target=archive_listener,
            args=(self.archive_queue, self.log_queue, self.global_cfg)
        )
        monitor_process = multiprocessing.Process(
            target=progress_monitor,
            args=(self.log_queue, self.stats, len(self.table_configs))
        )

        log_process.start()
        archive_process.start()
        monitor_process.start()

        # Resume from previous run if necessary
        self._resume_from_previous_run()

        # Prepare SQL Server before loading
        self._prepare_sql_server()

        # Run main extraction and loading workers
        self._run_workers()

        # Post-load optimization
        self._post_load_optimize()

        # Shutdown
        self._shutdown(log_process, archive_process, monitor_process)

        end_time = datetime.now()
        duration = end_time - start_time

        print("=" * 80)
        print(f"PIPELINE COMPLETE")
        print(f"Finished at: {end_time}")
        print(f"Total duration: {duration}")
        print(f"Tables processed: {self.stats['tables_completed']}/{len(self.table_configs)}")
        print(f"Files extracted: {self.stats.get('total_files_extracted', 0)}")
        print(f"Files loaded: {self.stats.get('total_files_loaded', 0)}")
        print(f"Total extraction time: {self.stats.get('total_extraction_time', 0.0):.2f} seconds")
        print(f"Total load time: {self.stats.get('total_load_time', 0.0):.2f} seconds")
        print("=" * 80)

    def _resume_from_previous_run(self):
        """
        Load any files left over from a previous failed run.
        Also re-queues files that were recorded as failed in _failed_files.json.
        """
        self._log("Checking for files from a previous run...")
        gcs = gcsfs.GCSFileSystem(
            project=self.global_cfg.PROJECT_ID,
            token=self.global_cfg.BQ_CREDENTIALS if hasattr(self.global_cfg, 'BQ_CREDENTIALS') else None
        )

        for table_cfg in self.table_configs:
            export_path = (
                f"gs://{self.global_cfg.GCS_BUCKET}/{table_cfg.dataset_id}/"
                f"{table_cfg.target_table}/pipeline_exports/"
            )

            # Resume unprocessed export files (skip the rollback tracking file)
            if gcs.exists(export_path):
                leftover_files = [
                    f for f in gcs.ls(export_path)
                    if not f.endswith('_failed_files.json')
                ]
                if leftover_files:
                    self._log(
                        f"Found {len(leftover_files)} leftover files for "
                        f"table {table_cfg.target_table}. Resuming load."
                    )
                    for f in leftover_files:
                        self.file_queue.put((f, table_cfg))

            # ROLLBACK: Re-queue previously failed files
            failed_files_path = (
                f"gs://{self.global_cfg.GCS_BUCKET}/{table_cfg.dataset_id}/"
                f"{table_cfg.target_table}/pipeline_exports/_failed_files.json"
            )
            if gcs.exists(failed_files_path):
                try:
                    with gcs.open(failed_files_path, 'r') as f:
                        failed_files = json.load(f)
                    if failed_files:
                        self._log(
                            f"Found {len(failed_files)} previously failed files "
                            f"for table {table_cfg.target_table}. Re-queuing."
                        )
                        for f in failed_files:
                            if gcs.exists(f):
                                self.file_queue.put((f, table_cfg))
                            else:
                                self._log(f"WARNING: Failed file no longer exists in GCS: {f}")
                except Exception as e:
                    self._log(f"WARNING: Could not read failed files for {table_cfg.target_table}: {e}")

        # Wait for the queue to drain before proceeding with fresh extraction
        while not self.file_queue.empty():
            self._log("Waiting for leftover/failed files to be loaded...")
            time.sleep(5)

    def _prepare_sql_server(self):
        """Prepare SQL Server for bulk loading"""
        self._log("Preparing SQL Server for bulk load...")

        try:
            import pyodbc
            conn = pyodbc.connect(self.global_cfg.CONN_STR)
            cursor = conn.cursor()

            # Set database to simple recovery for speed
            cursor.execute(f"ALTER DATABASE {self.global_cfg.DATABASE_NAME} SET RECOVERY SIMPLE;")

            for table_cfg in self.table_configs:
                target_table = f"{self.global_cfg.DATABASE_NAME}.dbo.{table_cfg.target_table}"

                try:
                    cursor.execute(f"ALTER INDEX ALL ON {target_table} DISABLE;")
                    cursor.execute(f"ALTER TABLE {target_table} NOCHECK CONSTRAINT ALL;")
                    cursor.execute(f"DISABLE TRIGGER ALL ON {target_table};")
                    cursor.execute(f"TRUNCATE TABLE {target_table};")

                    self._log(f"Prepared table: {table_cfg.target_table}")

                except Exception as e:
                    self._log(f"Warning preparing {table_cfg.target_table}: {e}")

            conn.commit()
            cursor.close()
            conn.close()

            self._log("SQL Server preparation complete!")

        except Exception as e:
            self._log(f"ERROR preparing SQL Server: {e}")

    def _run_workers(self):
        """Main worker coordination - runs extractors and loaders in parallel"""

        extractor_pool = multiprocessing.Pool(
            processes=self.limits_cfg.MAX_CONCURRENT_EXTRACT,
            maxtasksperchild=1
        )

        loader_processes = []
        for i in range(1, self.limits_cfg.MAX_CONCURRENT_LOAD + 1):
            p = multiprocessing.Process(
                target=loader_worker,
                args=(i, self.file_queue, self.log_queue, self.archive_queue, self.global_cfg, self.stats, self.stats_lock),
                name=f"Loader-{i}"
            )
            p.start()
            loader_processes.append(p)

        self._log(f"Started {len(loader_processes)} loader workers")

        pending_tables = list(self.table_configs)
        running_tables = {}
        active_extractors = 0

        last_progress_log = time.time()
        extraction_start_time = time.time()

        while pending_tables or running_tables:
            completed_tables = []
            for table_name, results in list(running_tables.items()):
                if all(r.ready() for r in results):
                    table_cfg = next(
                        t for t in self.table_configs
                        if t.target_table == table_name
                    )

                    active_extractors -= table_cfg.extract_workers
                    self.stats['tables_completed'] = self.stats.get('tables_completed', 0) + 1

                    self._log(
                        f"EXTRACTION COMPLETE: {table_name} "
                        f"(freed {table_cfg.extract_workers} workers, "
                        f"active: {active_extractors})",
                        prefix=f"TABLE-{table_name}"
                    )

                    completed_tables.append(table_name)

            for table_name in completed_tables:
                del running_tables[table_name]

            while pending_tables:
                next_table = pending_tables[0]

                if active_extractors + next_table.extract_workers <= self.limits_cfg.MAX_CONCURRENT_EXTRACT:
                    pending_tables.pop(0)

                    results = self._launch_table_extraction(next_table, extractor_pool)
                    running_tables[next_table.target_table] = results
                    active_extractors += next_table.extract_workers

                    self._log(
                        f"LAUNCHED: {next_table.target_table} "
                        f"(workers: {next_table.extract_workers}, "
                        f"active: {active_extractors}, "
                        f"pending: {len(pending_tables)})"
                    )
                else:
                    break

            if time.time() - last_progress_log > 10:
                queue_size = self.file_queue.qsize()
                self._log(
                    f"PROGRESS: Running tables: {len(running_tables)}, "
                    f"Pending: {len(pending_tables)}, "
                    f"Queue size: {queue_size}, "
                    f"Active extractors: {active_extractors}/{self.limits_cfg.MAX_CONCURRENT_EXTRACT}"
                )
                last_progress_log = time.time()

            time.sleep(0.5 if running_tables else 2)

        extraction_end_time = time.time()
        self.stats['total_extraction_time'] = extraction_end_time - extraction_start_time
        self._log(f"Total extraction time: {self.stats['total_extraction_time']:.2f} seconds")

        self._log("All table extractions complete. Shutting down extractor pool.")
        extractor_pool.close()
        extractor_pool.join()

        self._log("Waiting for file queue to drain...")
        queue_empty_count = 0
        while queue_empty_count < 5:
            queue_size = self.file_queue.qsize()
            if queue_size == 0:
                queue_empty_count += 1
            else:
                queue_empty_count = 0
            time.sleep(2)

        self._log("Sending stop signals to loaders...")
        for _ in loader_processes:
            self.file_queue.put(None)

        self._log("Waiting for loaders to finish...")
        for p in loader_processes:
            p.join(timeout=300)
            if p.is_alive():
                self._log(f"WARNING: Loader {p.name} still running, terminating...")
                p.terminate()

    def _launch_table_extraction(self, table_cfg, pool):
        """Launch extraction workers for a single table"""
        self._log(
            f"Starting extraction: {table_cfg.source_table} -> {table_cfg.target_table}",
            prefix=f"TABLE-{table_cfg.target_table}"
        )

        try:
            client = bigquery_storage.BigQueryReadClient(
                credentials=self.global_cfg.BQ_CREDENTIALS
            )

            table_path = (
                f"projects/{self.global_cfg.PROJECT_ID}/"
                f"datasets/{table_cfg.dataset_id}/"
                f"tables/{table_cfg.source_table}"
            )

            session = client.create_read_session(
                parent=f"projects/{self.global_cfg.PROJECT_ID}",
                read_session=bigquery_storage.types.ReadSession(
                    table=table_path,
                    data_format=bigquery_storage.types.DataFormat.ARROW,
                ),
                max_stream_count=self.global_cfg.TARGET_STREAM_COUNT,
            )

            streams = [s.name for s in session.streams]
            actual_stream_count = len(streams)

            self._log(
                f"Created {actual_stream_count} streams for {table_cfg.target_table}",
                prefix=f"TABLE-{table_cfg.target_table}"
            )

            num_workers = min(table_cfg.extract_workers, actual_stream_count)

            chunks = [[] for _ in range(num_workers)]
            for i, stream in enumerate(streams):
                chunks[i % num_workers].append(stream)

            worker_args = [
                (chunk, i + 1, self.global_cfg, table_cfg, self.file_queue, self.log_queue)
                for i, chunk in enumerate(chunks)
                if chunk
            ]

            results = [
                pool.apply_async(extractor_worker, args=(args,))
                for args in worker_args
            ]

            self._log(
                f"Submitted {len(results)} extractor tasks for {table_cfg.target_table}",
                prefix=f"TABLE-{table_cfg.target_table}"
            )

            return results

        except Exception as e:
            self._log(
                f"ERROR launching extraction for {table_cfg.target_table}: {e}",
                prefix=f"TABLE-{table_cfg.target_table}"
            )
            return []

    def _post_load_optimize(self):
        """Rebuild indexes and restore SQL Server to normal state"""
        self._log("Starting post-load optimization...")

        try:
            import pyodbc
            conn = pyodbc.connect(self.global_cfg.CONN_STR)
            cursor = conn.cursor()

            for table_cfg in self.table_configs:
                target_table = f"{self.global_cfg.DATABASE_NAME}.dbo.{table_cfg.target_table}"

                try:
                    self._log(f"Optimizing {table_cfg.target_table}...")

                    cursor.execute(
                        f"ALTER INDEX ALL ON {target_table} "
                        f"REBUILD WITH (DATA_COMPRESSION = PAGE, ONLINE = OFF);"
                    )

                    cursor.execute(f"UPDATE STATISTICS {target_table} WITH FULLSCAN;")
                    cursor.execute(f"ALTER TABLE {target_table} CHECK CONSTRAINT ALL;")
                    cursor.execute(f"ENABLE TRIGGER ALL ON {target_table};")

                    self._log(f"Optimized {table_cfg.target_table}")

                except Exception as e:
                    self._log(f"Warning optimizing {table_cfg.target_table}: {e}")

            cursor.execute(
                f"ALTER DATABASE {self.global_cfg.DATABASE_NAME} SET RECOVERY FULL;"
            )

            conn.commit()
            cursor.close()
            conn.close()

            self._log("Post-load optimization complete!")

        except Exception as e:
            self._log(f"ERROR in post-load optimization: {e}")

    def _shutdown(self, log_process, archive_process, monitor_process):
        """Graceful shutdown of all processes"""
        self._log("Shutting down pipeline...")

        if monitor_process.is_alive():
            monitor_process.terminate()
            monitor_process.join(timeout=5)

        self._log("Shutting down archiver...")
        self.archive_queue.put(None)
        archive_process.join(timeout=30)
        if archive_process.is_alive():
            archive_process.terminate()

        time.sleep(2)
        self._log("--- PIPELINE COMPLETE ---")
        self.log_queue.put(None)
        log_process.join(timeout=10)
        if log_process.is_alive():
            log_process.terminate()


def logger_listener(log_queue, global_cfg):
    """Centralized logging process"""
    log_files = {}
    base_log_dir = global_cfg.LOG_DIR
    os.makedirs(base_log_dir, exist_ok=True)

    while True:
        try:
            record = log_queue.get(timeout=1)
            if record is None:
                break

            if ']' not in record:
                continue

            prefix, message = record.split(']', 1)
            log_prefix = prefix[1:]

            log_dir, log_filename = get_log_path(base_log_dir, log_prefix)
            os.makedirs(log_dir, exist_ok=True)

            file_key = os.path.join(log_dir, log_filename)
            if file_key not in log_files:
                log_files[file_key] = open(file_key, 'a', buffering=1)

            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            log_files[file_key].write(f"[{timestamp}] {message.strip()}\n")
            print(f"[{log_prefix}] [{timestamp}] {message.strip()}")

        except queue.Empty:
            continue
        except Exception as e:
            print(f"Logger error: {e}")

    for f in log_files.values():
        f.close()


def get_log_path(base_dir, prefix):
    """Determine log directory and filename"""
    if prefix.startswith("Loader-"):
        worker_id = prefix.split('-')[1] if len(prefix.split('-')) > 1 else "unknown"
        return os.path.join(base_dir, "loaders"), f"loader_{worker_id}.log"

    if prefix.startswith("Extractor-"):
        parts = prefix.split('-')
        table_name = parts[1] if len(parts) > 1 else "unknown"
        return os.path.join(base_dir, "extractors", table_name), "extractor.log"

    if prefix.startswith("TABLE-"):
        table_name = prefix.split('-', 1)[1] if '-' in prefix else "unknown"
        return os.path.join(base_dir, "tables", table_name), "table.log"

    if prefix == "MONITOR":
        return base_dir, "monitor.log"

    return base_dir, "orchestrator.log"


def archive_listener(archive_queue, log_queue, global_cfg):
    """Archive or delete processed files from GCS"""
    gcs = gcsfs.GCSFileSystem(
        project=global_cfg.PROJECT_ID,
        token=global_cfg.BQ_CREDENTIALS if hasattr(global_cfg, 'BQ_CREDENTIALS') else None
    )

    while True:
        try:
            item = archive_queue.get(timeout=5)
            if item is None:
                break

            action, gcs_path, table_cfg = item
            filename = os.path.basename(gcs_path)

            log_queue.put(f"[ARCHIVER] Received: {action} for {filename}")

            if action == 'DELETE':
                gcs.rm(gcs_path)
                log_queue.put(f"[ARCHIVER] Deleted: {filename}")

            elif action == 'ARCHIVE':
                archive_path = (
                    f"gs://{global_cfg.GCS_BUCKET}/{table_cfg.dataset_id}/{table_cfg.target_table}/"
                    f"pipeline_archive/{datetime.now().strftime('%Y-%m-%d')}/{filename}"
                )
                archive_dir = os.path.dirname(archive_path)
                if not gcs.exists(archive_dir):
                    gcs.mkdirs(archive_dir, exist_ok=True)

                gcs.mv(gcs_path, archive_path)
                log_queue.put(f"[ARCHIVER] Archived {filename} to {archive_path}")

        except queue.Empty:
            continue
        except Exception as e:
            log_queue.put(f"[ARCHIVER] ERROR: {e}")


def progress_monitor(log_queue, stats, total_tables):
    """Background progress monitoring"""
    start_time = time.time()

    while True:
        try:
            time.sleep(30)

            elapsed = time.time() - start_time
            tables_done = stats.get('tables_completed', 0)
            files_extracted = stats.get('total_files_extracted', 0)
            files_loaded = stats.get('total_files_loaded', 0)

            log_queue.put(
                f"[MONITOR] Progress: {tables_done}/{total_tables} tables, "
                f"{files_extracted} files extracted, "
                f"{files_loaded} files loaded, "
                f"{elapsed / 60:.1f} minutes elapsed"
            )

            if tables_done >= total_tables:
                break

        except Exception as e:
            log_queue.put(f"[MONITOR] ERROR: {e}")
            break


def main():
    """Entry point"""
    config_path = sys.argv[1] if len(sys.argv) > 1 else "pipeline_config.json"

    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)

    try:
        orchestrator = MainOrchestrator(config_path)
        orchestrator.run()
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user!")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
