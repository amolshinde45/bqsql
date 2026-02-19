# BigQuery to SQL Server Pipeline

A high-performance, parallel data pipeline for extracting data from BigQuery and loading it into SQL Server.

## Files

| File | Description |
|---|---|
| `main_orchestrator.py` | Main entry point and pipeline coordinator |
| `bq_extractor.py` | BigQuery Storage API reader and GCS writer |
| `sql_loader.py` | GCS downloader and SQL Server bulk loader |
| `config_manager.py` | JSON config parser and credentials handler |
| `pipeline_config.json` | Sample configuration file |

## Setup

### Prerequisites
```bash
pip install google-cloud-bigquery-storage gcsfs pyarrow pyodbc sqlalchemy psutil
```

### Authentication
```bash
gcloud auth application-default login
```

### Environment Variables
```bash
export MSSQL_UID=your_sql_username
export MSSQL_PWD=your_sql_password
```

## Usage
```bash
python main_orchestrator.py pipeline_config.json
```

## Rollback / Resume on Failure

- If a file **fails to load**, it is recorded in `_failed_files.json` inside the table's GCS export folder.
- On the **next pipeline run**, `_resume_from_previous_run` automatically re-queues all failed files for retry.
- On **successful retry**, the file is removed from `_failed_files.json`.
- Leftover unprocessed export files from a crashed run are also automatically resumed.

## Architecture

```
MainOrchestrator
    ├── BqExtractor workers  (multiprocessing.Pool)
    │     └── BQ Storage API → PyArrow → pipe-delimited CSV → GCS
    ├── SqlLoader workers    (multiprocessing.Process)
    │     └── GCS download → BCP / BULK INSERT → SQL Server
    │         ├── On failure: write to _failed_files.json in GCS
    │         └── On success: clear from _failed_files.json
    ├── logger_listener      (dedicated log process)
    ├── archive_listener     (DELETE or ARCHIVE files post-load)
    └── progress_monitor     (stats every 30s)
```
