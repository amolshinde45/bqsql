# sql_loader.py - SQL Server Data Loading

import os
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
        # FIX: accept shared_stats and stats_lock as optional — orchestrator passes them,
        # but signature stays backward-compatible if called without them.
        self.shared_stats = shared_stats
        self.stats_lock = stats_lock

        self.db_engine = self._init_sql_engine()

        self.gcs = gcsfs.GCSFileSystem(
            project=self.config.PROJECT_ID,
            token=config.BQ_CREDENTIALS if hasattr(config, 'BQ_CREDENTIALS') else None,
            block_size=16 * 1024 * 1024
        )

        self.local_temp_dir = self.config.LOCAL_TEMP_DIR
        os.makedirs(self.local_temp_dir, exist_ok=True)

        self._configure_worker()

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
        self.log_queue.put(f"[Loader-{self.worker_id}] {msg}")

    def _init_sql_engine(self):
        try:
            engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={self.config.CONN_STR}",
                pool_size=2,
                max_overflow=3,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
                connect_args={'timeout': 30, 'autocommit': True}
            )
            self._log("Successfully initialized SQL engine.")
            return engine
        except Exception as e:
            self._log(f"CRITICAL: Failed to initialize SQL engine: {e}")
            return None

    def _configure_worker(self):
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

    def _use_bcp_instead(self, file_path, target_table, table_cfg):
        if not os.path.exists(file_path):
            raise Exception(f"File not found: {file_path}")
        if os.path.getsize(file_path) == 0:
            raise Exception(f"File is empty: {file_path}")

        conn_parts = {}
        for part in self.config.CONN_STR.split(';'):
            if '=' in part:
                key, val = part.split('=', 1)
                conn_parts[key.strip()] = val.strip()

        server   = conn_parts.get('SERVER', 'localhost')
        database = self.config.DATABASE_NAME
        uid      = conn_parts.get('UID', '')
        pwd      = conn_parts.get('PWD', '')
        error_file = f"{file_path}.err"

        cmd = [
            'bcp', target_table, 'in', file_path,
            '-S', server, '-d', database,
            '-U', uid, '-P', pwd,
            '-c', '-t', '|', '-r', '\\n',
            '-F', '1', '-b', str(self.config.BATCH_SIZE),
            '-a', '65535', '-h', 'TABLOCK',
            '-m', '10', '-e', error_file
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            if result.returncode != 0:
                details = [f"Exit code: {result.returncode}"]
                if result.stdout: details.append(f"STDOUT: {result.stdout}")
                if result.stderr: details.append(f"STDERR: {result.stderr}")
                if os.path.exists(error_file):
                    try:
                        with open(error_file, 'r') as f:
                            c = f.read()
                            if c: details.append(f"ERROR FILE: {c}")
                    except: pass
                raise Exception("\n".join(details))

            rows_loaded = 0
            for line in result.stdout.split('\n'):
                if 'rows copied' in line.lower():
                    try:
                        rows_loaded = int(line.split()[0])
                        break
                    except: pass

            try:
                if os.path.exists(error_file): os.remove(error_file)
            except: pass

            return rows_loaded
        except subprocess.TimeoutExpired:
            raise Exception("BCP timeout after 30 minutes")

    def _bulk_insert(self, file_path, target_table, format_file):
        abs_file_path = os.path.abspath(file_path)
        full_table_name = f"{self.config.DATABASE_NAME}.{target_table}"
        sql = text(f"""
            BULK INSERT {full_table_name}
            FROM '{abs_file_path}'
            WITH (FIELDTERMINATOR='|', ROWTERMINATOR='\\n',
                  TABLOCK, BATCHSIZE={self.config.BATCH_SIZE}, FIRE_TRIGGERS=OFF);
        """)
        with self.db_engine.connect() as connection:
            result = connection.execute(sql)
            connection.commit()
            return result.rowcount if hasattr(result, 'rowcount') else 0

    def _download_from_gcs(self, gcs_path, local_path):
        for attempt in range(3):
            try:
                start = time.time()
                self.gcs.get(gcs_path, local_path)
                self.stats['download_time'] += time.time() - start
                self.stats['total_bytes'] += os.path.getsize(local_path)
                return True
            except Exception as e:
                if attempt < 2:
                    self._log(f"Download failed (attempt {attempt+1}), retrying: {e}")
                    time.sleep(2 ** attempt)
                else:
                    self._log(f"CRITICAL: Failed to download after 3 attempts: {e}")
        return False

    def _load_to_sql(self, file_path, target_table, table_cfg):
        use_bcp = getattr(self.config, 'USE_BCP', True)
        for attempt in range(3):
            try:
                start = time.time()
                if use_bcp:
                    rows_loaded = self._use_bcp_instead(file_path, target_table, table_cfg)
                else:
                    rows_loaded = self._bulk_insert(file_path, target_table, table_cfg.format_file)
                self.stats['load_time'] += time.time() - start
                self.stats['total_rows'] += rows_loaded
                self.stats['files_loaded'] += 1

                # Update shared stats if provided
                if self.shared_stats is not None and self.stats_lock is not None:
                    with self.stats_lock:
                        self.shared_stats['total_files_loaded'] = self.shared_stats.get('total_files_loaded', 0) + 1
                        self.shared_stats['total_rows'] = self.shared_stats.get('total_rows', 0) + rows_loaded

                return True
            except pyodbc.Error as e:
                error_code = str(e)
                if '4861' in error_code or 'timeout' in error_code.lower():
                    if attempt < 2:
                        self._log(f"DB error (attempt {attempt+1}), retrying: {e}")
                        time.sleep(2 ** attempt)
                        continue
                self._log(f"CRITICAL: SQL error on {os.path.basename(file_path)}: {e}")
                return False
            except Exception as e:
                self._log(f"CRITICAL: Load error on {os.path.basename(file_path)}: {e}")
                return False
        return False

    def _get_target_table(self, table_cfg):
        if hasattr(table_cfg, 'temp_table_ids') and table_cfg.temp_table_ids:
            return f"dbo.{random.choice(table_cfg.temp_table_ids)}"
        return f"dbo.{table_cfg.target_table}"

    def _queue_for_archiving(self, gcs_path, table_cfg):
        action = 'DELETE' if self.config.DELETE_AFTER_LOAD else 'ARCHIVE'
        self.archive_queue.put((action, gcs_path, table_cfg))

    def _upload_to_db(self, gcs_path, table_cfg):
        filename = os.path.basename(gcs_path)
        local_path = os.path.join(self.local_temp_dir, f"loader_{self.worker_id}_{filename}")
        target_name = self._get_target_table(table_cfg)
        if not target_name:
            self._log(f"ERROR: No target table for {filename}")
            return
        try:
            if not self._download_from_gcs(gcs_path, local_path):
                return
            success = self._load_to_sql(local_path, target_name, table_cfg)
            if success:
                self._queue_for_archiving(gcs_path, table_cfg)
        except Exception as e:
            self._log(f"CRITICAL: Unexpected error processing {filename}: {e}")
        finally:
            if os.path.exists(local_path):
                try: os.remove(local_path)
                except: pass

    def run(self, file_queue):
        self.stats['start_time'] = time.time()
        self._log("Started and waiting for files.")
        if not self.db_engine:
            self._log("CRITICAL: No DB engine, exiting.")
            return

        processed_count = 0
        load_start_time = time.time()

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
                    self._log(f"Progress: {processed_count} files, {self.stats['total_rows']:,} rows, {throughput:,.0f} rows/s")
            except Exception as e:
                self._log(f"ERROR in main loop: {e}")
                continue

        self.stats['total_load_time'] = time.time() - load_start_time
        total_time = time.time() - self.stats['start_time']
        avg = self.stats['total_rows'] / total_time if total_time > 0 else 0
        self._log(
            f"COMPLETE: {self.stats['files_loaded']} files | "
            f"{self.stats['total_rows']:,} rows | "
            f"{self.stats['total_bytes']/(1024**3):.2f} GB | "
            f"{total_time:.1f}s | {avg:,.0f} rows/s"
        )


# FIX: accept shared_stats and stats_lock as optional kwargs
# Production orchestrator doesn't pass them; fixed orchestrator does.
def loader_worker(worker_id, file_queue, log_queue, archive_queue, config,
                  shared_stats=None, stats_lock=None):
    loader = SqlLoader(worker_id, config, log_queue, archive_queue, shared_stats, stats_lock)
    loader.run(file_queue)
