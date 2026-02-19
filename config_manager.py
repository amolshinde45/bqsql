# config_manager.py - Configuration Management

import json
import os
from collections import namedtuple
from google.auth import default

# from google.oauth2 import service_account  # Uncomment for service account auth

GlobalConfig = namedtuple("GlobalConfig", [
    "KEY_PATH", "PROJECT_ID", "DATABASE_NAME", "DATASET_ID", "OUTPUT_DIR",
    "ARCHIVE_DIR", "LOG_DIR", "SQL_SERVER", "BATCH_SIZE",
    "TARGET_STREAM_COUNT", "TARGET_FILE_SIZE_MB", "DELETE_AFTER_LOAD",
    "CONN_STR", "LOCAL_TEMP_DIR", "GCS_BUCKET", "USE_BCP",
    "BQ_CREDENTIALS", "AUTH_METHOD"
])

LimitsConfig = namedtuple("LimitsConfig", [
    "MAX_CONCURRENT_EXTRACT", "MAX_CONCURRENT_LOAD",
    "DEFAULT_EXTRACT_WORKERS", "DEFAULT_LOAD_WORKERS"
])

TableConfig = namedtuple("TableConfig", [
    "dataset_id", "source_table", "target_table", "priority",
    "extract_workers", "load_workers", "temp_table_ids", "format_file"
])


class ConfigManager:
    def __init__(self, config_path):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            self.config_data = json.load(f)

        self._create_dirs()
        self.global_config = self._load_global_config()
        self.limits_config = self._load_limits_config()
        self.table_configs = self._load_table_configs()

    def _create_dirs(self):
        """Create necessary directories"""
        settings = self.config_data.get("global_settings", {})

        os.makedirs(settings.get("OUTPUT_DIR", "pipeline_exports"), exist_ok=True)
        os.makedirs(settings.get("ARCHIVE_DIR", "pipeline_archive"), exist_ok=True)
        os.makedirs(settings.get("LOG_DIR", "pipeline_logs"), exist_ok=True)
        os.makedirs(settings.get("LOCAL_TEMP_DIR", "/tmp/pipeline_temp"), exist_ok=True)

    def _filter_comments(self, config_dict):
        """Remove comment fields (starting with _comment or _) from config"""
        return {
            key: value
            for key, value in config_dict.items()
            if not key.startswith('_comment') and not key.startswith('_')
        }

    def _load_global_config(self):
        """Load global configuration with credentials"""
        settings = self.config_data["global_settings"].copy()

        settings = self._filter_comments(settings)

        auth_method = settings.get("AUTH_METHOD", "default")

        # ========================================================================
        # AUTHENTICATION SETUP
        # ========================================================================

        if auth_method == "default":
            # METHOD 1: Use Application Default Credentials (ADC)
            print("INFO: Using Application Default Credentials (ADC)")
            print("      Make sure you've run: gcloud auth application-default login")

            try:
                credentials, project = default()
                settings["BQ_CREDENTIALS"] = credentials
                settings["AUTH_METHOD"] = "default"

                if not settings.get("PROJECT_ID"):
                    settings["PROJECT_ID"] = project
                    print(f"INFO: Using project from credentials: {project}")

                print(f"\u2713 Successfully loaded default credentials for project: {settings['PROJECT_ID']}")

            except Exception as e:
                raise ValueError(
                    f"Failed to load default credentials: {e}\n"
                    "Please run: gcloud auth application-default login"
                )

        # elif auth_method == "service_account":
        #     # METHOD 2: Use Service Account Key File
        #     # Uncomment this block to use service account authentication
        #     print("INFO: Using Service Account authentication")
        #
        #     key_path = settings.get("KEY_PATH")
        #
        #     if not key_path or not os.path.exists(key_path):
        #         raise FileNotFoundError(
        #             f"Service account key file not found: {key_path}\n"
        #             "Please provide a valid KEY_PATH in the config file"
        #         )
        #
        #     try:
        #         from google.oauth2 import service_account
        #
        #         credentials = service_account.Credentials.from_service_account_file(
        #             key_path,
        #             scopes=['https://www.googleapis.com/auth/cloud-platform']
        #         )
        #         settings["BQ_CREDENTIALS"] = credentials
        #         settings["AUTH_METHOD"] = "service_account"
        #
        #         print(f"\u2713 Successfully loaded service account credentials from: {key_path}")
        #
        #     except Exception as e:
        #         raise ValueError(f"Failed to load service account credentials: {e}")

        else:
            raise ValueError(
                f"Invalid AUTH_METHOD: {auth_method}\n"
                "Must be either 'default' or 'service_account'"
            )

        # ========================================================================
        # SQL SERVER CONNECTION STRING
        # ========================================================================

        uid = os.getenv("MSSQL_UID")
        pwd = os.getenv("MSSQL_PWD")

        if not uid or not pwd:
            raise ValueError(
                "MSSQL_UID and MSSQL_PWD environment variables are required.\n"
                "Set them with:\n"
                "  export MSSQL_UID=your_username\n"
                "  export MSSQL_PWD=your_password"
            )

        settings["CONN_STR"] = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={settings['SQL_SERVER']};"
            f"DATABASE={settings['DATABASE_NAME']};"
            f"UID={uid};"
            f"PWD={pwd};"
            f"TrustServerCertificate=yes;"
        )

        # ========================================================================
        # SET DEFAULTS FOR OPTIONAL FIELDS
        # ========================================================================

        settings.setdefault("USE_BCP", True)
        settings.setdefault("DELETE_AFTER_LOAD", True)
        settings.setdefault("BATCH_SIZE", 100000)
        settings.setdefault("TARGET_STREAM_COUNT", 100)
        settings.setdefault("TARGET_FILE_SIZE_MB", 300)
        settings.setdefault("KEY_PATH", None)

        return GlobalConfig(**settings)

    def _load_limits_config(self):
        """Load concurrency limits"""
        limits = self.config_data.get("global_limits", {}).copy()

        limits = self._filter_comments(limits)

        limits.setdefault("MAX_CONCURRENT_EXTRACT", 80)
        limits.setdefault("MAX_CONCURRENT_LOAD", 64)
        limits.setdefault("DEFAULT_EXTRACT_WORKERS", 10)
        limits.setdefault("DEFAULT_LOAD_WORKERS", 6)

        return LimitsConfig(**limits)

    def _load_table_configs(self):
        """Load table configurations"""
        tables = []
        defaults = self.config_data.get("global_limits", {})

        for table_data in self.config_data.get("tables", []):
            table_data = self._filter_comments(table_data)

            config = TableConfig(
                dataset_id=table_data.get("dataset_id", self.global_config.DATASET_ID),
                source_table=table_data["source_table"],
                target_table=table_data["target_table"],
                priority=table_data.get("priority", 10),
                extract_workers=table_data.get("extract_workers", defaults.get("DEFAULT_EXTRACT_WORKERS", 10)),
                load_workers=table_data.get("load_workers", defaults.get("DEFAULT_LOAD_WORKERS", 6)),
                temp_table_ids=table_data.get("temp_table_ids", []),
                format_file=table_data.get("FORMAT_FILE", table_data.get("format_file", ""))
            )
            tables.append(config)

        # Sort by priority (lower number = higher priority)
        tables.sort(key=lambda x: x.priority)

        return tables
