import os
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from typing import List, Dict, Tuple, Optional
import hashlib
import logging
import sys


class BronzeLayerLoader:
    def __init__(
        self, db_path: str = ":memory:", base_path: str = r"C:\etl\data\output"
    ):
        self.conn = duckdb.connect(db_path)
        self.base_path = Path(base_path)
        self.features = ["sales", "supplies", "inventory"]

        # Initialize logging tables
        self._init_logging_tables()

    def _init_logging_tables(self):
        """Initialize logging tables if they don't exist"""
        init_scripts = [
            "CREATE SCHEMA IF NOT EXISTS etl_logs;",
            """
            CREATE TABLE IF NOT EXISTS etl_logs.load_operations (
                operation_id INTEGER PRIMARY KEY,
                feature_name VARCHAR,
                load_path VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                files_processed INTEGER,
                rows_loaded INTEGER,
                status VARCHAR,
                error_message VARCHAR
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS etl_logs.schema_evolution (
                evolution_id INTEGER PRIMARY KEY,
                feature_name VARCHAR,
                table_name VARCHAR,
                change_type VARCHAR,
                column_name VARCHAR,
                old_data_type VARCHAR,
                new_data_type VARCHAR,
                change_date TIMESTAMP,
                resolved BOOLEAN DEFAULT FALSE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS etl_logs.file_processing (
                file_id INTEGER PRIMARY KEY,
                operation_id INTEGER,
                file_path VARCHAR,
                file_size BIGINT,
                records_count INTEGER,
                processed_at TIMESTAMP,
                status VARCHAR
            );
            """,
            "CREATE SEQUENCE IF NOT EXISTS etl_logs.operation_id_seq;",
            "CREATE SEQUENCE IF NOT EXISTS etl_logs.evolution_id_seq;",
            "CREATE SEQUENCE IF NOT EXISTS etl_logs.file_id_seq;",
        ]

        for script in init_scripts:
            try:
                self.conn.execute(script)
            except Exception as e:
                print(f"Warning: Could not execute script: {e}")

    def discover_parquet_files(self, feature: str) -> List[Path]:
        """Discover all parquet files for a given feature"""
        feature_path = self.base_path / feature
        parquet_files = []

        if feature_path.exists():
            for parquet_file in feature_path.rglob("*.parquet"):
                parquet_files.append(parquet_file)

        return parquet_files

    def get_table_name_from_path(self, file_path: Path) -> str:
        """Convert file path to table name - group by feature and type, not by month"""
        relative_path = file_path.relative_to(self.base_path)
        path_parts = relative_path.parts

        # Extract feature and type (sales/cash_invoices/detailed) but exclude year/month
        if len(path_parts) >= 3:
            # Keep only: feature + invoice_type + detail_level
            table_parts = path_parts[:3]  # ['sales', 'cash_invoices', 'detailed']
        else:
            table_parts = path_parts

        table_name = "_".join(table_parts).lower()
        return f"bronze_{table_name}"

    def get_schema_from_parquet(self, file_path: Path) -> Dict[str, str]:
        """Extract schema from parquet file"""
        try:
            table = pq.read_table(file_path)
            schema = {}
            for field in table.schema:
                schema[field.name] = str(field.type)
            return schema
        except Exception as e:
            print(f"Error reading schema from {file_path}: {e}")
            return {}

    def get_current_schema(self, table_name: str) -> Dict[str, str]:
        """Get current schema from DuckDB table"""
        try:
            result = self.conn.execute(
                f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """
            ).fetchall()
            return {row[0]: row[1] for row in result}
        except:
            return {}  # Table doesn't exist

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            result = self.conn.execute(
                f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """
            ).fetchone()
            return result is not None
        except:
            return False

    def detect_schema_changes(
        self, current_schema: Dict, new_schema: Dict, table_name: str, feature: str
    ) -> List[Dict]:
        """Detect schema changes between current and new schema - IGNORE metadata columns"""

        # Define metadata columns that we add during loading
        metadata_columns = {"source_file_path", "load_timestamp", "batch_id"}

        # Filter out metadata columns from current schema
        data_current_schema = {
            k: v for k, v in current_schema.items() if k not in metadata_columns
        }

        changes = []

        # Check for new DATA columns (ignore metadata)
        for column, data_type in new_schema.items():
            if column not in data_current_schema:
                changes.append(
                    {
                        "change_type": "ADD_COLUMN",
                        "column_name": column,
                        "old_data_type": None,
                        "new_data_type": data_type,
                    }
                )

        # Check for removed DATA columns (ignore metadata)
        for column, data_type in data_current_schema.items():
            if column not in new_schema:
                changes.append(
                    {
                        "change_type": "REMOVE_COLUMN",
                        "column_name": column,
                        "old_data_type": data_type,
                        "new_data_type": None,
                    }
                )

        # Check for type changes in DATA columns (ignore metadata)
        for column, new_type in new_schema.items():
            if (
                column in data_current_schema
                and data_current_schema[column] != new_type
            ):
                # Normalize type names for comparison
                old_type_norm = self.normalize_type_name(data_current_schema[column])
                new_type_norm = self.normalize_type_name(new_type)

                if old_type_norm != new_type_norm:
                    changes.append(
                        {
                            "change_type": "TYPE_CHANGE",
                            "column_name": column,
                            "old_data_type": data_current_schema[column],
                            "new_data_type": new_type,
                        }
                    )

        # Log changes
        for change in changes:
            self.conn.execute(
                """
                INSERT INTO etl_logs.schema_evolution 
                (evolution_id, feature_name, table_name, change_type, column_name, 
                old_data_type, new_data_type, change_date)
                VALUES (
                    nextval('etl_logs.evolution_id_seq'),
                    ?, ?, ?, ?, ?, ?, ?
                )
            """,
                (
                    feature,
                    table_name,
                    change["change_type"],
                    change["column_name"],
                    change["old_data_type"],
                    change["new_data_type"],
                    datetime.now(),
                ),
            )

        return changes

    def normalize_type_name(self, type_name: str) -> str:
        """Normalize type names for proper comparison"""
        type_name = type_name.upper()

        # Map similar types
        type_mapping = {
            "STRING": "VARCHAR",
            "INT64": "BIGINT",
            "FLOAT64": "DOUBLE",
            "TIMESTAMP[US]": "TIMESTAMP",
            "TIMESTAMP[NS]": "TIMESTAMP",
        }

        return type_mapping.get(type_name, type_name)

    def handle_schema_evolution(self, table_name: str, changes: List[Dict]):
        """Handle schema evolution by altering table structure"""
        for change in changes:
            if change["change_type"] == "ADD_COLUMN":
                try:
                    # Use VARCHAR for maximum compatibility
                    self.conn.execute(
                        f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN {change['column_name']} VARCHAR
                    """
                    )
                    print(f"Added column {change['column_name']} to {table_name}")
                except Exception as e:
                    print(f"Error adding column {change['column_name']}: {e}")

    def _ensure_metadata_columns(self, table_name: str):
        """Ensure metadata columns exist in the table - only if table exists"""
        if not self.table_exists(table_name):
            return  # Table doesn't exist yet, will be created with metadata

        try:
            # Check and add each metadata column if needed
            metadata_columns = [
                ("source_file_path", "VARCHAR"),
                ("load_timestamp", "TIMESTAMP"),
                ("batch_id", "INTEGER"),
            ]

            for col_name, col_type in metadata_columns:
                try:
                    self.conn.execute(
                        f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                    """
                    )
                except Exception as e:
                    print(
                        f"Warning: Could not add column {col_name} to {table_name}: {e}"
                    )

        except Exception as e:
            print(f"Warning: Could not ensure metadata columns for {table_name}: {e}")

    def get_processed_files(self) -> set:
        """Get set of already processed files"""
        try:
            result = self.conn.execute(
                """
                SELECT file_path FROM etl_logs.file_processing WHERE status = 'SUCCESS'
            """
            ).fetchall()
            return {row[0] for row in result}
        except:
            return set()

    def load_file_incrementally(
        self, file_path: Path, operation_id: int
    ) -> Tuple[bool, int]:
        """Load a single parquet file incrementally with proper error handling"""
        file_id = self.conn.execute(
            "SELECT nextval('etl_logs.file_id_seq')"
        ).fetchone()[0]
        file_stats = os.stat(file_path)

        try:
            # Get table name (grouped by type, not by month)
            table_name = self.get_table_name_from_path(file_path)
            feature = file_path.relative_to(self.base_path).parts[0]

            print(f"Processing: {file_path}")
            print(f"Target table: {table_name}")

            # Get schema from parquet file
            new_schema = self.get_schema_from_parquet(file_path)
            if not new_schema:
                raise ValueError(f"Could not read schema from {file_path}")

            print(f"File schema: {list(new_schema.keys())}")

            # Get current schema (if table exists)
            current_schema = self.get_current_schema(table_name)
            table_exists = bool(current_schema)

            print(f"Table exists: {table_exists}")

            # Prepare column lists
            parquet_columns = list(new_schema.keys())
            parquet_columns_str = ", ".join(parquet_columns)

            # Handle table creation vs insertion
            if not table_exists:
                print(f"Creating new table: {table_name}")
                # Create table with metadata columns included from start
                self.conn.execute(
                    f"""
                    CREATE TABLE {table_name} AS 
                    SELECT 
                        {parquet_columns_str},
                        '{str(file_path)}' as source_file_path,
                        CURRENT_TIMESTAMP as load_timestamp,
                        {operation_id} as batch_id
                    FROM read_parquet('{file_path}')
                """
                )
                print(f"Successfully created table {table_name}")
            else:
                print(f"Table {table_name} exists, checking for schema changes...")
                # Check for schema changes
                changes = self.detect_schema_changes(
                    current_schema, new_schema, table_name, feature
                )
                if changes:
                    print(f"Detected {len(changes)} schema changes")
                    self.handle_schema_evolution(table_name, changes)

                # Ensure metadata columns exist
                self._ensure_metadata_columns(table_name)

                # Insert data with metadata
                self.conn.execute(
                    f"""
                    INSERT INTO {table_name} 
                    SELECT 
                        {parquet_columns_str},
                        '{str(file_path)}' as source_file_path,
                        CURRENT_TIMESTAMP as load_timestamp,
                        {operation_id} as batch_id
                    FROM read_parquet('{file_path}')
                """
                )
                print(f"Successfully inserted data into {table_name}")

            # Get row count for this file
            row_count = self.conn.execute(
                f"""
                SELECT COUNT(*) FROM read_parquet('{file_path}')
            """
            ).fetchone()[0]

            # Log successful processing
            self.conn.execute(
                """
                INSERT INTO etl_logs.file_processing 
                (file_id, operation_id, file_path, file_size, records_count, processed_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'SUCCESS')
            """,
                (
                    file_id,
                    operation_id,
                    str(file_path),
                    file_stats.st_size,
                    row_count,
                    datetime.now(),
                ),
            )

            print(f"Successfully processed {file_path} - {row_count} rows")
            return True, row_count

        except Exception as e:
            # Log failure
            self.conn.execute(
                """
                INSERT INTO etl_logs.file_processing 
                (file_id, operation_id, file_path, file_size, processed_at, status)
                VALUES (?, ?, ?, ?, ?, 'FAILED')
            """,
                (
                    file_id,
                    operation_id,
                    str(file_path),
                    file_stats.st_size,
                    datetime.now(),
                ),
            )

            print(f"Error processing {file_path}: {e}")
            return False, 0

    def load_feature_incrementally(self, feature: str) -> Dict:
        """Load all files for a specific feature incrementally"""
        operation_id = self.conn.execute(
            "SELECT nextval('etl_logs.operation_id_seq')"
        ).fetchone()[0]
        started_at = datetime.now()

        # Log operation start
        self.conn.execute(
            """
            INSERT INTO etl_logs.load_operations 
            (operation_id, feature_name, load_path, started_at, status)
            VALUES (?, ?, ?, ?, 'STARTED')
        """,
            (operation_id, feature, str(self.base_path / feature), started_at),
        )

        try:
            # Discover files
            all_files = self.discover_parquet_files(feature)
            processed_files = self.get_processed_files()
            new_files = [f for f in all_files if str(f) not in processed_files]

            print(f"Found {len(new_files)} new files for feature {feature}")
            print(f"Already processed: {len(processed_files)} files")

            files_processed = 0
            total_rows_loaded = 0

            # Process each new file
            for file_path in new_files:
                success, rows_loaded = self.load_file_incrementally(
                    file_path, operation_id
                )
                if success:
                    files_processed += 1
                    total_rows_loaded += rows_loaded

            # Update operation log
            self.conn.execute(
                """
                UPDATE etl_logs.load_operations 
                SET completed_at = ?, files_processed = ?, rows_loaded = ?, status = 'COMPLETED'
                WHERE operation_id = ?
            """,
                (datetime.now(), files_processed, total_rows_loaded, operation_id),
            )

            return {
                "feature": feature,
                "files_processed": files_processed,
                "total_rows_loaded": total_rows_loaded,
                "status": "SUCCESS",
            }

        except Exception as e:
            # Log operation failure
            self.conn.execute(
                """
                UPDATE etl_logs.load_operations 
                SET completed_at = ?, status = 'FAILED', error_message = ?
                WHERE operation_id = ?
            """,
                (datetime.now(), str(e), operation_id),
            )

            return {"feature": feature, "error": str(e), "status": "FAILED"}

    def load_all_features(self):
        """Load all features incrementally"""
        results = []
        for feature in self.features:
            print(f"\n{'='*60}")
            print(f"Loading feature: {feature}")
            print(f"{'='*60}")
            result = self.load_feature_incrementally(feature)
            results.append(result)
            print(
                f"Completed {feature}: {result.get('files_processed', 0)} files, {result.get('total_rows_loaded', 0)} rows"
            )

        return results

    def get_schema_changes(self):
        """Get recent schema changes"""
        try:
            return self.conn.execute(
                """
                SELECT feature_name, table_name, change_type, column_name, change_date
                FROM etl_logs.schema_evolution 
                ORDER BY change_date DESC
            """
            ).fetchall()
        except Exception as e:
            print(f"Error getting schema changes: {e}")
            return []

    def get_load_summary(self):
        """Get summary of load operations"""
        try:
            return self.conn.execute(
                """
                SELECT feature_name, status, files_processed, rows_loaded, 
                       started_at, completed_at,
                       EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_seconds
                FROM etl_logs.load_operations 
                ORDER BY started_at DESC
            """
            ).fetchall()
        except Exception as e:
            print(f"Error getting load summary: {e}")
            return []

    def verify_loaded_data(self):
        """Verify that data is actually loaded into DuckDB tables"""
        try:
            # List all bronze tables created
            tables = self.conn.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'bronze_%'
            """
            ).fetchall()

            print(f"\n{'='*60}")
            print("BRONZE TABLES IN DUCKDB")
            print(f"{'='*60}")

            for table in tables:
                table_name = table[0]
                try:
                    row_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]
                    columns = self.conn.execute(
                        f"PRAGMA table_info({table_name})"
                    ).fetchall()
                    column_names = [col[1] for col in columns]

                    print(f"\nTable: {table_name}")
                    print(f"  Rows: {row_count:,}")
                    print(f"  Columns: {len(columns)}")
                    print(
                        f"  Sample columns: {column_names[:5]}{'...' if len(column_names) > 5 else ''}"
                    )

                    # Show data distribution by batch
                    batch_dist = self.conn.execute(
                        f"""
                        SELECT batch_id, COUNT(*) as records, 
                               MIN(load_timestamp) as first_load,
                               MAX(load_timestamp) as last_load
                        FROM {table_name}
                        GROUP BY batch_id
                        ORDER BY batch_id
                    """
                    ).fetchall()

                    if batch_dist:
                        print("  Batch distribution:")
                        for batch in batch_dist:
                            print(f"    Batch {batch[0]}: {batch[1]:,} records")

                except Exception as e:
                    print(f"Error reading table {table_name}: {e}")

            return tables
        except Exception as e:
            print(f"Error verifying loaded data: {e}")
            return []


def run_with_logging():
    """Run with proper logging and error handling"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("C:/etl/logs/bronze_loader.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting bronze layer incremental load")

        loader = BronzeLayerLoader(
            db_path="C:/etl/duckdb/bronze_layer.db", base_path=r"C:\etl\data\output"
        )

        results = loader.load_all_features()

        # Log results
        for result in results:
            if result["status"] == "SUCCESS":
                logger.info(
                    f"Feature {result['feature']}: {result['files_processed']} files, {result['total_rows_loaded']} rows"
                )
            else:
                logger.error(
                    f"Feature {result['feature']} failed: {result.get('error', 'Unknown error')}"
                )

        # Verify loaded data
        loader.verify_loaded_data()

        # Log schema changes
        changes = loader.get_schema_changes()
        if changes:
            logger.warning(f"Detected {len(changes)} schema changes")
        else:
            logger.info("No schema changes detected")

        logger.info("Bronze layer load completed successfully")
        return True

    except Exception as e:
        logger.error(f"Bronze layer load failed: {e}")
        return False


if __name__ == "__main__":
    success = run_with_logging()
    sys.exit(0 if success else 1)
