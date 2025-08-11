import duckdb
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Tuple
from datetime import datetime
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    postgres_host: str = os.getenv('POSTGRES_HOST', 'localhost')
    postgres_port: int = int(os.getenv('POSTGRES_PORT', '5432'))
    postgres_db: str = os.getenv('POSTGRES_DB', 'iceberg_dbt')
    postgres_user: str = os.getenv('POSTGRES_USER', 'dbt_user')
    postgres_password: str = os.getenv('POSTGRES_PASSWORD', 'dbt_password')
    s3_region: str = os.getenv('S3_REGION', 'eu-central-1')
    s3_access_key: str = os.getenv('S3_ACCESS_KEY', 'minio_user')
    s3_secret_key: str = os.getenv('S3_SECRET_KEY', 'minio_password')
    s3_endpoint: str = os.getenv('S3_ENDPOINT', '127.0.0.1:9000')
    s3_bucket: str = os.getenv('S3_BUCKET', 'iceberg-data')


class StateManagerInterface(ABC):
    @abstractmethod
    def get_last_processed_time(self) -> Tuple[str, dict]:
        """
        Returns a tuple of (last_end_timestamp_str, full_state_dict).
        - last_end_timestamp_str should represent the last processed window end boundary used as baseline for next run.
        - full_state_dict is the raw parsed state for the caller to compute window start/end and to persist updates.
        """
        pass

    @abstractmethod
    def save_state(self, state: dict) -> None:
        """Persist the provided state atomically to the state store."""
        pass


class S3StateManager(StateManagerInterface):
    def __init__(self, connection, bucket: str):
        self.connection = connection
        self.bucket = bucket
        self.state_file = f's3://{bucket}/incremental_state.json'

    def get_last_processed_time(self) -> Tuple[str, dict]:
        try:
            result = self.connection.execute(f"""
                SELECT * FROM read_text('{self.state_file}')
            """).fetchone()
            if result:
                state = json.loads(result[1])
                # Handle new window-based state structure with backward compatibility
                if 'readings_window' in state and 'end' in state['readings_window']:
                    last_end = state['readings_window']['end']
                else:
                    last_end = '1970-01-01 00:00:00'
                return last_end, state
        except Exception as e:
            logger.warning(f"Could not read state file: {e}")
        return '1970-01-01 00:00:00', {}

    def save_state(self, state: dict) -> None:
        try:
            state_json = json.dumps(state, indent=2)
            self.connection.execute(f"""
                COPY (SELECT '{state_json}' as json) TO '{self.state_file}' 
                (FORMAT CSV, HEADER false, QUOTE '', ESCAPE '')
            """)
            version = state.get('version', 1)
            window = state.get('readings_window')
            if window:
                logger.info(f"State saved successfully (v{version}): window [{window['start']}, {window['end']}]")
            else:
                logger.info(f"State saved successfully (v{version}): {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            raise


class DatabaseConnection:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None

    def __enter__(self):
        self.connection = duckdb.connect(":memory:")
        self._setup_connection()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def _setup_connection(self):
        extensions = [
            "INSTALL postgres_scanner; LOAD postgres_scanner;",
            "INSTALL httpfs; LOAD httpfs;"
        ]

        for ext in extensions:
            self.connection.execute(ext)

        postgres_conn_str = (
            f"host={self.config.postgres_host} "
            f"port={self.config.postgres_port} "
            f"dbname={self.config.postgres_db} "
            f"user={self.config.postgres_user} "
            f"password={self.config.postgres_password}"
        )

        self.connection.execute(f"ATTACH '{postgres_conn_str}' AS raw (TYPE postgres);")
        self.connection.execute("USE raw;")

        s3_config = f"""
            SET s3_region='{self.config.s3_region}';
            SET s3_access_key_id='{self.config.s3_access_key}';
            SET s3_secret_access_key='{self.config.s3_secret_key}';
            SET s3_endpoint='{self.config.s3_endpoint}';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
        """
        self.connection.execute(s3_config)


class DataProcessor:
    def __init__(self, connection, state_manager: StateManagerInterface, config: DatabaseConfig):
        self.connection = connection
        self.state_manager = state_manager
        self.config = config

    def get_max_creation_time(self, window_start: str) -> Optional[datetime]:
        result = self.connection.execute(f"""
            SELECT MAX(creation_time) FROM raw.meter_data.readings
            WHERE creation_time > '{window_start}'
        """).fetchone()
        return result[0] if result and result[0] else None

    def get_creation_dates(self, window_start: str, window_end: str) -> List[datetime]:
        results = self.connection.execute(f"""
            SELECT DISTINCT DATE(creation_time) as creation_date
            FROM raw.meter_data.readings
            WHERE creation_time > '{window_start}' AND creation_time <= '{window_end}'
            ORDER BY creation_date
        """).fetchall()
        return [row[0] for row in results]

    def export_date_partition(self, date: datetime, window_start: str, window_end: str) -> None:
        date_str = date.strftime('%Y-%m-%d')
        output_path = f's3://{self.config.s3_bucket}/readings/date={date_str}/readings.parquet'

        logger.info(f"Processing data for date: {date_str}")

        self.connection.execute(f"""
            COPY (
                SELECT * FROM raw.meter_data.readings
                WHERE creation_time > '{window_start}' AND creation_time <= '{window_end}'
                AND DATE(creation_time) = '{date}'
                ORDER BY creation_time
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        logger.info(f"Successfully exported data for {date_str}")

    def process_incremental_data(self) -> None:
        last_end, state = self.state_manager.get_last_processed_time()
        
        # Compute window boundaries
        window_start = last_end
        logger.info(f"Window start: {window_start}")
        
        max_creation_time = self.get_max_creation_time(window_start)
        window_end = str(max_creation_time)

        if max_creation_time is None:
            logger.info("No new data to load")
            window_end = window_start
            self._save_state(state, window_end, window_start)
            return
        
        logger.info(f"Window end: {window_end}")
        logger.info(f"Processing window: [{window_start}, {window_end}]")

        creation_dates = self.get_creation_dates(window_start, window_end)
        logger.info(f"Dates to export: {[d.strftime('%Y-%m-%d') for d in creation_dates]}")

        for creation_date in creation_dates:
            self.export_date_partition(creation_date, window_start, window_end)
        self._save_state(state, window_end, window_start)

    def _save_state(self, state, window_end, window_start):
        # Persist window-based state + maintain legacy compatibility
        state['readings_window'] = {
            'start': window_start,
            'end': window_end
        }
        state['version'] = 2
        self.state_manager.save_state(state)
        logger.info(f"Updated window: start={window_start}, end={window_end}")


class ParquetDataLoader:
    def __init__(self, config: DatabaseConfig = None):
        self.config = config or DatabaseConfig()

    def run(self) -> None:
        try:
            with DatabaseConnection(self.config) as conn:
                state_manager = S3StateManager(conn, self.config.s3_bucket)
                processor = DataProcessor(conn, state_manager, self.config)
                processor.process_incremental_data()
        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            raise


class IcebergTableManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config

    def _load_iceberg_catalog(self, bucket: str):
        try:
            bucket_name = bucket or self.config.s3_bucket

            uri = (
                f"postgresql://{self.config.postgres_user}:{self.config.postgres_password}"
                f"@{self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_db}"
            )

            endpoint = self.config.s3_endpoint
            if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
                endpoint = f"http://{endpoint}"

            # Use the bucket's warehouse path
            warehouse = f"s3a://{bucket_name}/warehouse"
            logger.info(f"Using warehouse: {warehouse}")

            return load_catalog(
                "iceberg",
                **{
                    "type": "sql",
                    "uri": uri,
                    "warehouse": warehouse,
                    "s3.endpoint": endpoint,
                    "s3.access-key-id": self.config.s3_access_key,
                    "s3.secret-access-key": self.config.s3_secret_key,
                    "s3.path-style-access": "true",
                    "s3.region": self.config.s3_region
                }
            )
        except Exception as e:
            logger.error(f"Failed to load iceberg table: {e}")
            raise

    @staticmethod
    def _create_iceberg_table(catalog, namespace: str, bucket_name: str, table_name: str, schema: Schema):
        table_location = f"s3a://{bucket_name}/warehouse/{namespace}.db/{table_name}"

        try:
            #for testing
            # catalog.drop_table(
            #     identifier=(namespace, table_name),
            # )
            table = catalog.create_table_if_not_exists(
                identifier=(namespace, table_name),
                schema=schema,
                location=table_location,
                properties={
                    "write.format.default": "parquet"
                },
            )
            return table
        except Exception as e:
            logger.error(f"Failed to create iceberg table: {e}")
            raise

    @staticmethod
    def _get_existing_data_files(table) -> set:
        """Get set of data file paths already registered in the Iceberg table."""
        existing_files = set()
        
        try:
            scan = table.scan()
            for task in scan.plan_files():
              file = task.file
              existing_files.add(file.file_path)
            
            logger.info(f"Found {len(existing_files)} existing data files in table")
            return existing_files
            

        except Exception as error:
          logger.warning(f"Failed to find files: {error}")
          # Return empty set to be safe - will re-add files (idempotent)
          return set()

    def run(self, files):
        try:
            # Make all fields optional to match Parquet's nullable columns
            schema = Schema(
                NestedField(1, "id", IntegerType()),
                NestedField(2, "meter_id", StringType()),
                NestedField(3, "reading_value", DecimalType(15, 3)),
                NestedField(4, "interval_start", TimestampType()),
                NestedField(5, "interval_end", TimestampType()),
                NestedField(6, "creation_time", TimestampType()),
                NestedField(7, "reading_type", StringType()),
                NestedField(8, "unit_of_measure", StringType()),
                NestedField(9, "quality_code", StringType()),
                NestedField(10, "status", StringType()),
            )

            # Use actual bucket for catalog and table location
            bucket_name = self.config.s3_bucket
            catalog = self._load_iceberg_catalog(bucket_name)
            catalog.create_namespace_if_not_exists("raw")

            table = self._create_iceberg_table(catalog, "raw", bucket_name, "readings", schema)
            logger.info(f"Loaded iceberg table: {table}")
            
            # Get existing files from table metadata
            existing_files = self._get_existing_data_files(table)
            
            # Filter to only new files
            new_files = [f for f in files if f not in existing_files]
            
            logger.info(f"Discovered {len(files)} parquet files, {len(existing_files)} already registered, {len(new_files)} new")
            
            if new_files:
                logger.info(f"Adding {len(new_files)} new files to table: {new_files}")
                table.add_files(new_files)
            else:
                logger.info("No new files to add to table")

        except Exception as e:
            logger.error(f"Failed to load iceberg table: {e}")
            raise


class IcebergDataLoader:
    def __init__(self, config: DatabaseConfig, table_manager: IcebergTableManager):
        self.config = config
        self.table_manager = table_manager

    def run(self) -> None:
        try:
            with DatabaseConnection(self.config) as conn:
                bucket = f's3://{self.config.s3_bucket}/readings/date*/*.parquet'
                logger.info(f"reading files in: {bucket}")

                result = conn.sql(f"""
                    SELECT distinct filename FROM read_parquet('{bucket}', filename = true);
                """).fetchall()
                files = [row[0] for row in result]
                self.table_manager.run(files)
                logger.info(f"Sync for iceberg files: {result}")

        except Exception as e:
            logger.error(f"Failed to load iceberg : {e}")
            raise


def main():
    parquet_loader = ParquetDataLoader()
    parquet_loader.run()

    iceberg_table_manager = IcebergTableManager(DatabaseConfig())

    iceberg_loader = IcebergDataLoader(DatabaseConfig(), iceberg_table_manager)
    iceberg_loader.run()

    # df = pq.read_table("./readings.parquet")
    # logger.info(df.schema)


if __name__ == "__main__":
    main()