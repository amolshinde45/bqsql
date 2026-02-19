# sql_loader.py - SQL Server Data Loading

import os
import json
import time
import random
import psutil
import pyodbc
import subprocess
from sqlalchemy import create_engine, text
import gcsfs
from pathlib import Path


class SqlLoader:
    def __init__(self, worker_id, config, log_queue, archive_queue, shared_stats=None, stats_lock=None):
        self.worker_id = worker_id
        self.config = config
        self.log_queue = log_queue
        self.archive_queue = archive_queue
        self.shared_stats = shared_stats      # shared multiprocessing dict from orchestrator
        self.stats_lock = stats_lock          # shared lock for updating stats

        # Initialize SQL connection
        self.db_engine = self._init_sql_engine()

        # Initialize GCS with optimizations
        self.gcs = gcsfs.GCSFileSystem(
            project=self.config.PROJECT_ID,
            token=config.BQ_CREDENTIALS if hasattr(config, 'BQ_CREDENTIALS') else None,
            block_size=16 * 1024 * 1024  # 16MB blocks for faster downloads
        )

        # Local temp directory (should be on local SSD)
        self.local_temp_dir = self.config.LOCAL_TEMP_DIR
        os.makedirs(self.local_temp_dir, exist_ok=True)

        # Configure worker affinity
        self._configure_worker()

        # Local performance stats
        self.stats = {
            'files_loaded': 0,
            'total_rows': 0,
            'total_bytes': 0,
            'download_time': 0,
            'load_time': 0,
            'start_time': None,
            'total_load_time': 0.0
        }

    def _log(self, msg):
        """Send log message to queue"""
        self.log_queue.put(f"[Loader-{self.worker_id}] {msg}")

    def _init_sql_engine(self):
        """Initialize SQL Server connection with optimized settings"""
        try:
            engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={self.config.CONN_STR}",
                pool_size=2,
                max_overflow=3,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
                connect_args={
                    'timeout': 30,
                    'autocommit': True
                }
            )
            self._log("Successfully initialized SQL engine.")
            return engine
        except Exception as e:
            self._log(f"CRITICAL: Failed to initialize SQL engine: {e}")
            return None

    def _configure_worker(self):
        """Pin worker to specific CPU core for better cache locality"""
        try:
            p = psutil.Process(os.getpid())
            available_cores = list(range(psutil.cpu_count(logical=True)))
            cpu_to_pin = available_cores[(self.worker_id - 1) % len(available_cores)]
            p.cpu_affinity([cpu_to_pin])

            if os.name == 'nt':
                p.nice(psutil.HIGH_PRIORITY_CLASS)
            else:
                try:
                    p.nice(-10)
                except:
                    pass
        except Exception as e:
            self._log(f"Warning: CPU pinning/priority failed: {e}")

    # ============================================================
    # ROLLBACK HELPERS - Track failed files per table in GCS
    # ============================================================

    def _get_failed_files_path(self, table_cfg):
        """GCS path for the failed files tracking JSON for a table"""
        return (
            f"gs://{self.config.GCS_BUCKET}/{table_cfg.dataset_id}/"
            f"{table_cfg.target_table}/pipeline_exports/_failed_files.json"
        )

    def _record_failure(self, gcs_path, table_cfg):
        """Append a failed file path to the table's _failed_files.json in GCS"""
        try:
            failed_path = self._get_failed_files_path(table_cfg)
            failed = []
            if self.gcs.exists(failed_path):
                with self.gcs.open(failed_path, 'r') as f:
                    failed = json.load(f)
            if gcs_path not in failed:
                failed.append(gcs_path)
            with self.gcs.open(failed_path, 'w') as f:
                json.dump(failed, f)
            self._log(f"Recorded failure for {os.path.basename(gcs_path)}")
        except Exception as e:
            self._log(f"WARNING: Could not record failure for {os.path.basename(gcs_path)}: {e}")

    def _clear_failure(self, gcs_path, table_cfg):
        """Remove a successfully loaded file from the table's _failed_files.json"""
        try:
            failed_path = self._get_failed_files_path(table_cfg)
            if not self.gcs.exists(failed_path):
                return
            with self.gcs.open(failed_path, 'r') as f:
                failed = json.load(f)
            if gcs_path in failed:
                failed.remove(gcs_path)
                with self.gcs.open(failed_path, 'w') as f:
                    json.dump(failed, f)
        except Exception as e:
            self._log(f"WARNING: Could not clear failure record for {os.path.basename(gcs_path)}: {e}")

    # ============================================================

    def _use_bcp_instead(self, file_path, target_table, table_cfg):
        """Use BCP for loading"""
        if not os.path.exists(file_path):
            raise Exception(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        if file_size == 0:
            raise Exception(f"File is empty: {file_path}")

        conn_parts = {}
        for part in self.config.CONN_STR.split(';'):
            if '=' in part:
                key, val = part.split('=', 1)
                conn_parts[key.strip()] = val.strip()

        server = conn_parts.get('SERVER', 'localhost')
        database = self.config.DATABASE_NAME
        uid = conn_parts.get('UID', '')
        pwd = conn_parts.get('PWD', '')

        error_file = f"{file_path}.err"

        cmd = [
            'bcp', target_table, 'in', file_path,
            '-S', server,
            '-d', database,
            '-U', uid,
            '-P', pwd,
            '-c',
            '-t', '|',
            '-r', '\\n',
            '-F', '1',
            '-b', str(self.config.BATCH_SIZE),
            '-a', '65535',
            '-h', 'TABLOCK',
            '-m', '10',
            '-e', error_file
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

            if result.returncode != 0:
                error_details = [f"Exit code: {result.returncode}"]
                if result.stdout:
                    error_details.append(f"STDOUT: {result.stdout}")
                if result.stderr:
                    error_details.append(f"STDERR: {result.stderr}")
                if os.path.exists(error_file):
                    try:
                        with open(error_file, 'r') as f:
                            err_content = f.read()
                            if err_content:
                                error_details.append(f"ERROR FILE: {err_content}")
                    except:
                        pass
                raise Exception("\n".join(error_details))

            rows_loaded = 0
            for line in result.stdout.split('\n'):
                if 'rows copied' in line.lower():
                    try:
                        rows_loaded = int(line.split()[0])
                        break
                    except:
                        pass

            if os.path.exists(error_file):
                try:
                    os.remove(error_file)
                except:
                    pass

            return rows_loaded

        except subprocess.TimeoutExpired:
            raise Exception("BCP timeout after 30 minutes")

    def _bulk_insert(self, file_path, target_table, format_file):
        """Standard BULK INSERT method"""
        abs_file_path = os.path.abspath(file_path)
        full_table_name = f"{self.config.DATABASE_NAME}.{target_table}"

        sql = text(f"""
            BULK INSERT {full_table_name}
            FROM '{abs_file_path}'
            WITH (
                FIELDTERMINATOR = '|',
                ROWTERMINATOR = '\\n',
                TABLOCK,
                BATCHSIZE = {self.config.BATCH_SIZE},
                FIRE_TRIGGERS = OFF
            );
        """)

        with self.db_engine.connect() as connection:
            result = connection.execute(sql)
            connection.commit()
            return result.rowcount if hasattr(result, 'rowcount') else 0

    def _download_from_gcs(self, gcs_path, local_path):
        """Download file from GCS with retry logic"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                start = time.time()
                self.gcs.get(gcs_path, local_path)
                download_time = time.time() - start
                file_size = os.path.getsize(local_path)
                self.stats['download_time'] += download_time
                self.stats['total_bytes'] += file_size
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    self._log(f"Download failed (attempt {attempt + 1}), retrying: {e}")
                    time.sleep(2 ** attempt)
                else:
                    self._log(f"CRITICAL: Failed to download after {max_retries} attempts: {e}")
                    return False

        return False

    def _load_to_sql(self, file_path, target_table, table_cfg):
        """Load file to SQL Server using best available method"""
        use_bcp = getattr(self.config, 'USE_BCP', True)
        max_retries = 3

        for attempt in range(max_retries):
            try:
                start = time.time()

                if use_bcp:
                    rows_loaded = self._use_bcp_instead(file_path, target_table, table_cfg)
                else:
                    rows_loaded = self._bulk_insert(file_path, target_table, table_cfg.format_file)

                load_time = time.time() - start
                self.stats['load_time'] += load_time
                self.stats['total_rows'] += rows_loaded
                self.stats['files_loaded'] += 1

                # Update shared stats for orchestrator monitoring
                if self.shared_stats is not None and self.stats_lock is not None:
                    with self.stats_lock:
                        self.shared_stats['total_files_loaded'] = self.shared_stats.get('total_files_loaded', 0) + 1
                        self.shared_stats['total_rows'] = self.shared_stats.get('total_rows', 0) + rows_loaded

                return True

            except pyodbc.Error as e:
                error_code = str(e)
                if '4861' in error_code or 'timeout' in error_code.lower():
                    if attempt < max_retries - 1:
                        self._log(f"DB error (attempt {attempt + 1}), retrying: {e}")
                        time.sleep(2 ** attempt)
                        continue
                self._log(f"CRITICAL: SQL error on {os.path.basename(file_path)}: {e}")
                return False

            except Exception as e:
                self._log(f"CRITICAL: Load error on {os.path.basename(file_path)}: {e}")
                return False

        return False

    def _upload_to_db(self, gcs_path, table_cfg):
        """
        Main method: download from GCS and load to SQL Server.
        On failure: records file in _failed_files.json for retry on next run.
        On success: removes file from _failed_files.json if previously failed.
        """
        filename = os.path.basename(gcs_path)
        local_path = os.path.join(self.local_temp_dir, f"loader_{self.worker_id}_{filename}")
        target_name = f"dbo.{table_cfg.target_table}"

        try:
            # Step 1: Download from GCS
            if not self._download_from_gcs(gcs_path, local_path):
                self._record_failure(gcs_path, table_cfg)
                return

            # Step 2: Load to SQL Server
            success = self._load_to_sql(local_path, target_name, table_cfg)

            # Step 3: Archive/delete on success, record failure otherwise
            if success:
                self._clear_failure(gcs_path, table_cfg)
                self._queue_for_archiving(gcs_path, table_cfg)
            else:
                self._record_failure(gcs_path, table_cfg)

        except Exception as e:
            self._log(f"CRITICAL: Unexpected error processing {filename}: {e}")
            self._record_failure(gcs_path, table_cfg)

        finally:
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass

    def _queue_for_archiving(self, gcs_path, table_cfg):
        """Queue file for archiving or deletion"""
        action = 'DELETE' if self.config.DELETE_AFTER_LOAD else 'ARCHIVE'
        self.archive_queue.put((action, gcs_path, table_cfg))

    def run(self, file_queue):
        """Main worker loop - processes files from queue"""
        self.stats['start_time'] = time.time()
        self._log("Started and waiting for files.")

        if not self.db_engine:
            self._log("CRITICAL: No DB engine, exiting.")
            return

        processed_count = 0

        while True:
            try:
                item = file_queue.get(timeout=300)

                if item is None:
                    self._log("Received stop signal. Exiting.")
                    break

                gcs_path, table_cfg = item
                self._upload_to_db(gcs_path, table_cfg)
                processed_count += 1

                if processed_count % 10 == 0:
                    elapsed = time.time() - self.stats['start_time']
                    throughput = self.stats['total_rows'] / elapsed if elapsed > 0 else 0
                    self._log(
                        f"Progress: {processed_count} files, "
                        f"{self.stats['total_rows']:,} rows, "
                        f"{throughput:,.0f} rows/s"
                    )

            except Exception as e:
                self._log(f"ERROR in main loop: {e}")
                continue

        total_time = time.time() - self.stats['start_time']
        avg_throughput = self.stats['total_rows'] / total_time if total_time > 0 else 0

        self._log(
            f"COMPLETE: {self.stats['files_loaded']} files, "
            f"{self.stats['total_rows']:,} rows, "
            f"{self.stats['total_bytes'] / (1024 ** 3):.2f} GB, "
            f"{total_time:.1f}s total, "
            f"{avg_throughput:,.0f} rows/s avg"
        )


def loader_worker(worker_id, file_queue, log_queue, archive_queue, config, shared_stats=None, stats_lock=None):
    """
    Multiprocessing worker entry point.
    Accepts optional shared_stats and stats_lock for orchestrator-level tracking.
    """
    loader = SqlLoader(worker_id, config, log_queue, archive_queue, shared_stats, stats_lock)
    loader.run(file_queue)
