#!/usr/bin/env python3
"""
Bulk upload Parquet files from nested folders to BigQuery with incremental loading.
Logs processed files and metadata to DuckDB.
"""

import os
import argparse
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from pathlib import Path
import json
import duckdb
import hashlib


class ParquetToBigQueryLoader:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_path: Optional[str] = None,
        duckdb_path: str = "processing_logs.duckdb",
    ):
        """
        Initialize the BigQuery loader with DuckDB logging.

        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            credentials_path: Path to service account credentials JSON file
            duckdb_path: Path to DuckDB database file for logging
        """
        # Initialize logger first
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = f"{project_id}.{dataset_id}"

        # Initialize DuckDB for logging
        self.duckdb_path = duckdb_path
        self._init_duckdb()

        # Create dataset if it doesn't exist
        self._create_dataset_if_not_exists()

    def _init_duckdb(self):
        """Initialize DuckDB database and tables for logging."""
        try:
            self.conn = duckdb.connect(self.duckdb_path)

            # Create processed_files table
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_files (
                    file_path VARCHAR PRIMARY KEY,
                    file_hash VARCHAR,
                    file_size INTEGER,
                    table_name VARCHAR,
                    processed_at TIMESTAMP,
                    status VARCHAR,
                    records_processed INTEGER,
                    error_message VARCHAR,
                    processing_time_seconds FLOAT
                )
            """
            )

            # Create processing_stats table for summary statistics
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processing_stats (
                    run_id VARCHAR,
                    run_timestamp TIMESTAMP,
                    total_files INTEGER,
                    successful_files INTEGER,
                    failed_files INTEGER,
                    skipped_files INTEGER,
                    total_processing_time_seconds FLOAT,
                    avg_processing_time_seconds FLOAT
                )
            """
            )

            self.logger.info(f"Initialized DuckDB logging database: {self.duckdb_path}")
        except Exception as e:
            self.logger.error(f"Error initializing DuckDB: {e}")
            raise

    def _create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist."""
        try:
            self.client.get_dataset(self.dataset_ref)
            self.logger.info(f"Dataset {self.dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"  # Change as needed
            self.client.create_dataset(dataset)
            self.logger.info(f"Created dataset {self.dataset_ref}")
        except Exception as e:
            self.logger.error(f"Error creating dataset: {e}")
            raise

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file for change detection."""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating file hash for {file_path}: {e}")
            return ""

    def _get_file_metadata(self, file_path: Path) -> Tuple[str, int]:
        """Get file hash and size."""
        try:
            file_hash = self._calculate_file_hash(file_path)
            file_size = file_path.stat().st_size
            return file_hash, file_size
        except Exception as e:
            self.logger.error(f"Error getting file metadata for {file_path}: {e}")
            return "", 0

    def is_file_processed(
        self, file_path: str, current_hash: str
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Check if file has been processed and if it has changed.

        Returns:
            Tuple of (is_processed, file_metadata)
        """
        try:
            result = self.conn.execute(
                """
                SELECT file_path, file_hash, table_name, status, processed_at 
                FROM processed_files 
                WHERE file_path = ? AND status = 'SUCCESS'
            """,
                [file_path],
            ).fetchone()

            if result:
                previous_hash = result[1]
                # If file has changed, it needs to be reprocessed
                if previous_hash == current_hash:
                    return True, {
                        "file_path": result[0],
                        "file_hash": result[1],
                        "table_name": result[2],
                        "status": result[3],
                        "processed_at": result[4],
                    }
                else:
                    self.logger.info(f"File {file_path} has changed, will reprocess")
                    return False, None
            return False, None
        except Exception as e:
            self.logger.error(f"Error checking if file is processed: {e}")
            return False, None

    def log_file_processing_start(
        self, file_path: str, file_hash: str, file_size: int, table_name: str
    ):
        """Log the start of file processing."""
        try:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO processed_files 
                (file_path, file_hash, file_size, table_name, processed_at, status, records_processed, error_message, processing_time_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    file_path,
                    file_hash,
                    file_size,
                    table_name,
                    datetime.now(),
                    "STARTED",
                    0,
                    None,
                    0,
                ],
            )
        except Exception as e:
            self.logger.error(f"Error logging processing start: {e}")

    def log_file_processing_result(
        self,
        file_path: str,
        status: str,
        records_processed: int = 0,
        error_message: str = None,
        processing_time: float = 0,
    ):
        """Log the result of file processing."""
        try:
            self.conn.execute(
                """
                UPDATE processed_files 
                SET status = ?, records_processed = ?, error_message = ?, processing_time_seconds = ?
                WHERE file_path = ?
            """,
                [status, records_processed, error_message, processing_time, file_path],
            )
        except Exception as e:
            self.logger.error(f"Error logging processing result: {e}")

    def log_processing_stats(self, run_id: str, stats: Dict):
        """Log overall processing statistics for the run."""
        try:
            self.conn.execute(
                """
                INSERT INTO processing_stats 
                (run_id, run_timestamp, total_files, successful_files, failed_files, skipped_files, 
                 total_processing_time_seconds, avg_processing_time_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    run_id,
                    datetime.now(),
                    stats["total_files"],
                    stats["successful_files"],
                    stats["failed_files"],
                    stats["skipped_files"],
                    stats["total_processing_time"],
                    stats["avg_processing_time"],
                ],
            )
        except Exception as e:
            self.logger.error(f"Error logging processing stats: {e}")

    def get_processing_summary(self) -> pd.DataFrame:
        """Get summary of processing history."""
        try:
            return self.conn.execute(
                """
                SELECT 
                    run_timestamp,
                    total_files,
                    successful_files,
                    failed_files,
                    skipped_files,
                    total_processing_time_seconds,
                    avg_processing_time_seconds
                FROM processing_stats 
                ORDER BY run_timestamp DESC
            """
            ).fetchdf()
        except Exception as e:
            self.logger.error(f"Error getting processing summary: {e}")
            return pd.DataFrame()

    def get_file_processing_history(self, file_path: str = None) -> pd.DataFrame:
        """Get processing history for files."""
        try:
            if file_path:
                return self.conn.execute(
                    """
                    SELECT * FROM processed_files 
                    WHERE file_path = ? 
                    ORDER BY processed_at DESC
                """,
                    [file_path],
                ).fetchdf()
            else:
                return self.conn.execute(
                    """
                    SELECT * FROM processed_files 
                    ORDER BY processed_at DESC
                """
                ).fetchdf()
        except Exception as e:
            self.logger.error(f"Error getting file history: {e}")
            return pd.DataFrame()

    def find_parquet_files(self, root_dir: str) -> List[Path]:
        """
        Recursively find all Parquet files in nested directories.

        Args:
            root_dir: Root directory to search for Parquet files

        Returns:
            List of Path objects for Parquet files
        """
        try:
            root_path = Path(root_dir)
            if not root_path.exists():
                self.logger.error(f"Source directory does not exist: {root_dir}")
                return []

            parquet_files = list(root_path.rglob("*.parquet"))

            self.logger.info(f"Found {len(parquet_files)} Parquet files in {root_dir}")
            return parquet_files
        except Exception as e:
            self.logger.error(f"Error finding Parquet files: {e}")
            return []

    def infer_table_name(
        self, file_path: Path, table_prefix: Optional[str] = None
    ) -> str:
        """
        Infer table name from file path.

        Args:
            file_path: Path to Parquet file
            table_prefix: Optional prefix for table names

        Returns:
            Table name for BigQuery
        """
        try:
            # Remove file extension and replace special characters
            table_name = file_path.stem.lower()
            table_name = "".join(
                c if c.isalnum() or c == "_" else "_" for c in table_name
            )

            # Use folder structure for table naming
            try:
                relative_path = file_path.relative_to(
                    file_path.parents[len(file_path.parents) - 1]
                )
                path_parts = [p.stem.lower() for p in relative_path.parents if p.stem]
                path_parts.reverse()

                if path_parts:
                    table_name = "_".join(path_parts + [table_name])
            except ValueError:
                # If relative path calculation fails, use just the filename
                pass

            if table_prefix:
                table_name = f"{table_prefix}_{table_name}"

            # Ensure table name is valid for BigQuery
            table_name = table_name[:1024]  # BigQuery table name length limit
            return table_name
        except Exception as e:
            self.logger.error(f"Error inferring table name: {e}")
            return "unknown_table"

    def get_file_row_count(self, file_path: Path) -> int:
        """Get approximate row count from Parquet file metadata."""
        try:
            # Efficiently get row count without loading entire file
            parquet_file = pd.read_parquet(file_path, engine="pyarrow")
            return len(parquet_file)
        except Exception as e:
            self.logger.warning(f"Could not get row count for {file_path}: {e}")
            return 0

    def create_table_if_not_exists(
        self, table_name: str, schema: List[bigquery.SchemaField]
    ):
        """
        Create BigQuery table if it doesn't exist.

        Args:
            table_name: Name of the table
            schema: Table schema
        """
        try:
            table_id = f"{self.dataset_ref}.{table_name}"

            try:
                self.client.get_table(table_id)
                self.logger.info(f"Table {table_id} already exists")
            except NotFound:
                table = bigquery.Table(table_id, schema=schema)
                table = self.client.create_table(table)
                self.logger.info(f"Created table {table_id}")
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")

    def load_file_to_bigquery(
        self, file_path: Path, table_name: str, write_disposition: str = "WRITE_APPEND"
    ) -> Tuple[bool, int, float, Optional[str]]:
        """
        Load a single Parquet file to BigQuery.

        Args:
            file_path: Path to Parquet file
            table_name: Target table name
            write_disposition: BigQuery write disposition

        Returns:
            Tuple of (success, records_processed, processing_time, error_message)
        """
        start_time = datetime.now()
        records_processed = 0
        error_message = None

        try:
            if not file_path.exists():
                error_message = f"File does not exist: {file_path}"
                self.logger.error(error_message)
                return False, 0, 0, error_message

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
                autodetect=True,
            )

            table_id = f"{self.dataset_ref}.{table_name}"

            with open(file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_id, job_config=job_config
                )

            job.result()  # Wait for job to complete

            if job.errors:
                error_message = str(job.errors)
                self.logger.error(f"Errors loading {file_path}: {job.errors}")
                return False, 0, 0, error_message

            # Get number of processed records
            records_processed = job.output_rows or 0

            processing_time = (datetime.now() - start_time).total_seconds()

            self.logger.info(
                f"Successfully loaded {file_path} to {table_id} "
                f"({records_processed} records, {processing_time:.2f}s)"
            )
            return True, records_processed, processing_time, None

        except Exception as e:
            error_message = str(e)
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Error loading {file_path} to BigQuery: {e}")
            return False, 0, processing_time, error_message

    def process_files(
        self,
        root_dir: str,
        table_prefix: Optional[str] = None,
        incremental: bool = True,
        write_disposition: str = "WRITE_APPEND",
        run_id: str = None,
    ):
        """
        Process all Parquet files in the directory tree.

        Args:
            root_dir: Root directory containing Parquet files
            table_prefix: Optional prefix for table names
            incremental: Whether to skip already processed files
            write_disposition: BigQuery write disposition
            run_id: Unique identifier for this processing run
        """
        if not run_id:
            run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        parquet_files = self.find_parquet_files(root_dir)

        if not parquet_files:
            self.logger.warning(f"No Parquet files found in {root_dir}")
            return

        stats = {
            "total_files": len(parquet_files),
            "successful_files": 0,
            "failed_files": 0,
            "skipped_files": 0,
            "total_processing_time": 0,
            "processing_times": [],
        }

        for file_path in parquet_files:
            file_path_str = str(file_path.absolute())
            file_hash, file_size = self._get_file_metadata(file_path)

            if not file_hash:  # Skip if we couldn't get file metadata
                stats["failed_files"] += 1
                continue

            # Check if file should be processed
            if incremental:
                is_processed, file_metadata = self.is_file_processed(
                    file_path_str, file_hash
                )
                if is_processed:
                    self.logger.info(f"Skipping already processed file: {file_path}")
                    stats["skipped_files"] += 1
                    continue

            table_name = self.infer_table_name(file_path, table_prefix)

            # Log processing start
            self.log_file_processing_start(
                file_path_str, file_hash, file_size, table_name
            )

            self.logger.info(f"Processing {file_path} -> table {table_name}")

            # Process the file
            success, records_processed, processing_time, error_message = (
                self.load_file_to_bigquery(file_path, table_name, write_disposition)
            )

            # Log result
            status = "SUCCESS" if success else "FAILED"
            self.log_file_processing_result(
                file_path_str, status, records_processed, error_message, processing_time
            )

            # Update statistics
            if success:
                stats["successful_files"] += 1
                stats["total_processing_time"] += processing_time
                stats["processing_times"].append(processing_time)
            else:
                stats["failed_files"] += 1

        # Calculate average processing time
        if stats["processing_times"]:
            stats["avg_processing_time"] = sum(stats["processing_times"]) / len(
                stats["processing_times"]
            )
        else:
            stats["avg_processing_time"] = 0

        # Log overall statistics
        self.log_processing_stats(run_id, stats)

        self.logger.info(f"Processing complete for run {run_id}:")
        self.logger.info(f"  Total files: {stats['total_files']}")
        self.logger.info(f"  Successful: {stats['successful_files']}")
        self.logger.info(f"  Failed: {stats['failed_files']}")
        self.logger.info(f"  Skipped: {stats['skipped_files']}")
        self.logger.info(
            f"  Total processing time: {stats['total_processing_time']:.2f}s"
        )
        self.logger.info(
            f"  Average processing time: {stats['avg_processing_time']:.2f}s"
        )

        # Save DuckDB changes
        self.conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Bulk upload Parquet files to BigQuery with DuckDB logging"
    )
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")
    parser.add_argument(
        "--source-dir", required=True, help="Source directory with Parquet files"
    )
    parser.add_argument(
        "--credentials", help="Path to service account credentials JSON file"
    )
    parser.add_argument("--table-prefix", help="Prefix for table names")
    parser.add_argument(
        "--duckdb-path",
        default="processing_logs.duckdb",
        help="Path to DuckDB log database",
    )
    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Force full reload (ignore processed files log)",
    )
    parser.add_argument(
        "--write-disposition",
        default="WRITE_APPEND",
        choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
        help="BigQuery write disposition",
    )
    parser.add_argument("--run-id", help="Unique identifier for this processing run")
    parser.add_argument(
        "--show-stats", action="store_true", help="Show processing statistics and exit"
    )
    parser.add_argument(
        "--show-file-history",
        metavar="FILE_PATH",
        help="Show processing history for a specific file",
    )

    args = parser.parse_args()

    # Initialize loader
    loader = ParquetToBigQueryLoader(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        credentials_path=args.credentials,
        duckdb_path=args.duckdb_path,
    )

    if args.show_stats:
        # Show processing statistics
        stats_df = loader.get_processing_summary()
        if not stats_df.empty:
            print("\nProcessing Statistics Summary:")
            print(stats_df.to_string(index=False))
        else:
            print("No processing statistics available.")
        return

    if args.show_file_history:
        # Show file processing history
        history_df = loader.get_file_processing_history(args.show_file_history)
        if not history_df.empty:
            print(f"\nProcessing History for {args.show_file_history}:")
            print(history_df.to_string(index=False))
        else:
            print(f"No processing history found for {args.show_file_history}")
        return

    # Process files
    loader.process_files(
        root_dir=args.source_dir,
        table_prefix=args.table_prefix,
        incremental=not args.full_reload,
        write_disposition=args.write_disposition,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()
