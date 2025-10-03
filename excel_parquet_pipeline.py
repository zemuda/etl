#!/usr/bin/env python3
"""
Excel to Parquet Pipeline Script
Incrementally processes nested Excel files and converts them to Parquet format.
Logs are saved in DuckDB database.
"""

import os
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass, asdict
import duckdb
import uuid


@dataclass
class ProcessingState:
    """Track processing state for incremental updates"""

    file_path: str
    file_hash: str
    last_modified: float
    processed_at: str
    sheets_processed: List[str]
    output_files: List[str]
    excel_row_count: int = 0  # Total rows in Excel file
    parquet_row_count: int = 0  # Total rows in all Parquet files


class ExcelParquetPipeline:
    """Pipeline for converting Excel files to Parquet format incrementally"""

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        state_file: str = "processing_state.json",
        log_level: str = "INFO",
        db_path: str = "pipeline_logs.db",
    ):
        """
        Initialize the pipeline

        Args:
            input_dir: Directory containing Excel files
            output_dir: Directory to save Parquet files
            state_file: File to track processing state
            log_level: Logging level
            db_path: Path to DuckDB database for logging
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.state_file = Path(state_file)
        self.db_path = db_path

        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging and DuckDB
        self._setup_logging(log_level)
        self._setup_duckdb()

        # Load processing state
        self.state = self._load_state()

    def _setup_logging(self, log_level: str):
        """Setup logging configuration"""
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("excel_parquet_pipeline.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def _setup_duckdb(self):
        """Setup DuckDB database for logging"""
        self.conn = duckdb.connect(self.db_path)

        # Create logs table if it doesn't exist
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_logs (
                log_id UUID PRIMARY KEY,
                timestamp TIMESTAMP,
                level VARCHAR(10),
                logger_name VARCHAR(100),
                message TEXT,
                file_path VARCHAR(500),
                sheet_name VARCHAR(200),
                operation_type VARCHAR(50),
                duration_ms INTEGER,
                rows_processed INTEGER,
                success BOOLEAN,
                error_message TEXT
            )
        """
        )

        # In _setup_duckdb() method, update the table schema:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processing_stats (
                run_id UUID PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_files INTEGER,
                total_sheets INTEGER,
                total_excel_rows INTEGER,
                total_parquet_rows INTEGER,
                total_duration_ms INTEGER,
                success_count INTEGER,
                failure_count INTEGER,
                files_with_row_mismatch INTEGER,
                total_row_difference INTEGER
            )
        """
        )

        self.current_run_id = str(uuid.uuid4())
        self.run_start_time = datetime.now()

    def _log_to_duckdb(self, level: str, message: str, **kwargs):
        """Log message to DuckDB"""
        try:
            self.conn.execute(
                """
                INSERT INTO pipeline_logs VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """,
                (
                    str(uuid.uuid4()),
                    datetime.now(),
                    level,
                    self.logger.name,
                    message,
                    kwargs.get("file_path"),
                    kwargs.get("sheet_name"),
                    kwargs.get("operation_type"),
                    kwargs.get("duration_ms"),
                    kwargs.get("rows_processed"),
                    kwargs.get("success"),
                    kwargs.get("error_message"),
                ),
            )
        except Exception as e:
            self.logger.error(f"Failed to log to DuckDB: {e}")

    def _count_excel_rows(self, file_path: Path) -> int:
        """
        Count total rows in Excel file across all sheets efficiently

        Args:
            file_path: Path to Excel file

        Returns:
            Total number of non-empty rows across all sheets
        """
        total_rows = 0
        try:
            excel_file = pd.ExcelFile(file_path)

            for sheet_name in excel_file.sheet_names:
                try:
                    # Read only first column to count rows (much faster)
                    df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=[0])
                    # Count non-empty rows
                    sheet_rows = len(df.dropna(how="all"))
                    total_rows += sheet_rows

                    self._log_to_duckdb(
                        "DEBUG",
                        f"Counted {sheet_rows} rows in sheet '{sheet_name}'",
                        file_path=str(file_path),
                        sheet_name=sheet_name,
                        operation_type="row_counting",
                        rows_processed=sheet_rows,
                    )

                except Exception as e:
                    self.logger.warning(
                        f"Error counting rows in sheet '{sheet_name}': {e}"
                    )
                    continue

        except Exception as e:
            self.logger.error(f"Error counting rows in {file_path}: {e}")

        return total_rows

    def _count_parquet_rows(self, parquet_path: Path) -> int:
        """
        Count rows in Parquet file efficiently

        Args:
            parquet_path: Path to Parquet file

        Returns:
            Number of rows in Parquet file
        """
        try:
            # Use PyArrow to get row count without loading full file
            parquet_file = pq.ParquetFile(parquet_path)
            return parquet_file.metadata.num_rows

        except Exception as e:
            self.logger.warning(f"Error counting rows in {parquet_path}: {e}")
            return 0

    def _save_run_stats(self, stats: Dict[str, Any]):
        """Save run statistics to DuckDB"""
        try:
            self.conn.execute(
                """
                INSERT INTO processing_stats VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """,
                (
                    self.current_run_id,
                    self.run_start_time,
                    datetime.now(),
                    stats.get("total_files", 0),
                    stats.get("total_sheets", 0),
                    stats.get("total_excel_rows", 0),
                    stats.get("total_parquet_rows", 0),
                    stats.get("total_duration_ms", 0),
                    stats.get("success_count", 0),
                    stats.get("failure_count", 0),
                    stats.get("files_with_row_mismatch", 0),
                    stats.get("total_row_difference", 0),
                ),
            )
        except Exception as e:
            self.logger.error(f"Failed to save run stats: {e}")

    def _load_state(self) -> Dict[str, ProcessingState]:
        """Load processing state from file"""
        if not self.state_file.exists():
            return {}

        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
                return {
                    path: ProcessingState(**state_data)
                    for path, state_data in data.items()
                }
        except Exception as e:
            self.logger.warning(f"Could not load state file: {e}")
            self._log_to_duckdb(
                "WARNING", "Could not load state file", error_message=str(e)
            )
            return {}

    def _save_state(self):
        """Save processing state to file"""
        try:
            state_data = {path: asdict(state) for path, state in self.state.items()}
            with open(self.state_file, "w") as f:
                json.dump(state_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save state file: {e}")
            self._log_to_duckdb(
                "ERROR", "Could not save state file", error_message=str(e)
            )

    def _get_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _needs_processing(self, file_path: Path) -> bool:
        """Check if file needs processing based on state"""
        file_str = str(file_path)

        if file_str not in self.state:
            return True

        state = self.state[file_str]
        current_hash = self._get_file_hash(file_path)
        current_modified = file_path.stat().st_mtime

        return (
            current_hash != state.file_hash or current_modified != state.last_modified
        )

    def _flatten_nested_data(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """
        Flatten nested data structures in DataFrame

        Args:
            df: DataFrame with potentially nested data
            sheet_name: Name of the sheet for context

        Returns:
            Flattened DataFrame
        """
        result_df = df.copy()

        # Handle nested dictionaries and lists in cells
        for col in result_df.columns:
            if result_df[col].dtype == "object":
                # Check if column contains nested structures
                sample_values = result_df[col].dropna().head()

                for idx, value in sample_values.items():
                    if isinstance(value, (dict, list)):
                        # Convert nested structures to JSON strings
                        result_df[col] = result_df[col].apply(
                            lambda x: (
                                json.dumps(x) if isinstance(x, (dict, list)) else x
                            )
                        )
                        self.logger.info(
                            f"Flattened nested data in column '{col}' of sheet '{sheet_name}'"
                        )
                        self._log_to_duckdb(
                            "INFO",
                            f"Flattened nested data in column '{col}'",
                            file_path=sheet_name,
                            sheet_name=sheet_name,
                            operation_type="data_flattening",
                        )
                        break

        # Add metadata columns
        result_df["_source_sheet"] = sheet_name
        result_df["_processed_at"] = datetime.now().isoformat()

        return result_df

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare DataFrame for Parquet conversion"""
        # Remove completely empty rows
        df = df.dropna(how="all")

        # Remove completely empty columns
        df = df.dropna(axis=1, how="all")

        # Clean column names
        df.columns = [
            str(col).strip().replace(" ", "_").replace(".", "_") for col in df.columns
        ]

        # Handle duplicate column names
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols == dup] = [f"{dup}_{i}" for i in range(sum(cols == dup))]
        df.columns = cols

        return df

    def _process_excel_file(self, file_path: Path) -> List[str]:
        """
        Process a single Excel file and convert to Parquet
        """
        output_files = []
        processed_sheets = []
        excel_row_count = 0
        parquet_row_count = 0

        try:
            # Count rows in Excel file first
            excel_row_count = self._count_excel_rows(file_path)

            self.logger.info(f"Excel file {file_path} has {excel_row_count} total rows")

            # Read all sheets from Excel file
            excel_file = pd.ExcelFile(file_path)

            # Get relative path from input directory to preserve structure
            relative_path = file_path.relative_to(self.input_dir)
            output_subdir = self.output_dir / relative_path.parent
            output_subdir.mkdir(parents=True, exist_ok=True)

            for sheet_name in excel_file.sheet_names:
                sheet_start_time = datetime.now()
                try:
                    # Read sheet
                    df = pd.read_excel(
                        file_path, sheet_name=sheet_name, header=0, index_col=None
                    )

                    if df.empty:
                        self.logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                        continue

                    # Clean and flatten data
                    df = self._clean_dataframe(df)
                    df = self._flatten_nested_data(df, sheet_name)

                    # Generate output filename
                    base_name = file_path.stem
                    safe_sheet_name = (
                        "".join(
                            c for c in sheet_name if c.isalnum() or c in (" ", "-", "_")
                        )
                        .strip()
                        .replace(" ", "_")
                    )

                    output_filename = f"{base_name}_{safe_sheet_name}.parquet"
                    output_path = output_subdir / output_filename

                    # Convert to Parquet
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, output_path, compression="snappy")

                    # Count rows in the generated Parquet file
                    sheet_parquet_rows = self._count_parquet_rows(output_path)
                    parquet_row_count += sheet_parquet_rows

                    output_files.append(str(output_path))
                    processed_sheets.append(sheet_name)

                    duration_ms = (
                        datetime.now() - sheet_start_time
                    ).total_seconds() * 1000

                    self.logger.info(
                        f"Converted sheet '{sheet_name}' to {output_path} ({sheet_parquet_rows} rows)"
                    )
                    self._log_to_duckdb(
                        "INFO",
                        f"Converted sheet to Parquet",
                        file_path=str(file_path),
                        sheet_name=sheet_name,
                        operation_type="sheet_conversion",
                        duration_ms=int(duration_ms),
                        rows_processed=sheet_parquet_rows,
                        success=True,
                    )

                except Exception as e:
                    duration_ms = (
                        datetime.now() - sheet_start_time
                    ).total_seconds() * 1000
                    self.logger.error(
                        f"Error processing sheet '{sheet_name}' in {file_path}: {e}"
                    )
                    self._log_to_duckdb(
                        "ERROR",
                        f"Error processing sheet",
                        file_path=str(file_path),
                        sheet_name=sheet_name,
                        operation_type="sheet_conversion",
                        duration_ms=int(duration_ms),
                        success=False,
                        error_message=str(e),
                    )
                    continue

            # Log row count comparison
            row_difference = excel_row_count - parquet_row_count
            if row_difference != 0:
                self.logger.warning(
                    f"Row count mismatch: Excel={excel_row_count}, Parquet={parquet_row_count}, Diff={row_difference}"
                )
                self._log_to_duckdb(
                    "WARNING",
                    f"Row count difference detected",
                    file_path=str(file_path),
                    operation_type="row_count_validation",
                    rows_processed=excel_row_count,
                    success=True,
                    error_message=f"Excel: {excel_row_count}, Parquet: {parquet_row_count}, Difference: {row_difference}",
                )

            # Update processing state with row counts
            file_hash = self._get_file_hash(file_path)
            file_modified = file_path.stat().st_mtime

            self.state[str(file_path)] = ProcessingState(
                file_path=str(file_path),
                file_hash=file_hash,
                last_modified=file_modified,
                processed_at=datetime.now().isoformat(),
                sheets_processed=processed_sheets,
                output_files=output_files,
                excel_row_count=excel_row_count,
                parquet_row_count=parquet_row_count,
            )

            self._log_to_duckdb(
                "INFO",
                f"Successfully processed Excel file",
                file_path=str(file_path),
                operation_type="file_processing_complete",
                rows_processed=excel_row_count,
                success=True,
            )

        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {e}")
            self._log_to_duckdb(
                "ERROR",
                f"Error processing Excel file",
                file_path=str(file_path),
                operation_type="file_processing",
                success=False,
                error_message=str(e),
            )

        return output_files

    def process_directory(self, file_pattern: str = "*.xls") -> Dict[str, List[str]]:
        """
        Process all Excel files and track row count statistics
        """
        results = {}
        success_count = 0
        failure_count = 0
        total_excel_rows = 0
        total_parquet_rows = 0
        total_files_with_mismatch = 0

        # Find Excel files
        excel_files = []
        excel_files.extend(list(self.input_dir.rglob("*.xls")))
        excel_files.extend(list(self.input_dir.rglob("*.xlsx")))
        excel_files.extend(list(self.input_dir.rglob("*.xlsm")))
        excel_files.extend(list(self.input_dir.rglob("*.xlsb")))

        if not excel_files:
            self.logger.warning(f"No Excel files found in {self.input_dir}")
            return results

        for file_path in excel_files:
            file_start_time = datetime.now()
            try:
                if self._needs_processing(file_path):
                    self.logger.info(f"Processing {file_path}")
                    output_files = self._process_excel_file(file_path)
                    results[str(file_path)] = output_files

                    # Update totals from state
                    if str(file_path) in self.state:
                        state = self.state[str(file_path)]
                        total_excel_rows += state.excel_row_count
                        total_parquet_rows += state.parquet_row_count

                        # Check for mismatches
                        if state.excel_row_count != state.parquet_row_count:
                            total_files_with_mismatch += 1

                    success_count += 1
                else:
                    # For already processed files, use existing state
                    results[str(file_path)] = self.state[str(file_path)].output_files
                    state = self.state[str(file_path)]
                    total_excel_rows += state.excel_row_count
                    total_parquet_rows += state.parquet_row_count

                    if state.excel_row_count != state.parquet_row_count:
                        total_files_with_mismatch += 1

                    success_count += 1

            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")
                failure_count += 1

        # Save state and statistics
        self._save_state()

        # Log overall row count comparison
        total_row_difference = total_excel_rows - total_parquet_rows
        if total_row_difference != 0:
            self.logger.warning(
                f"Overall row count mismatch: Excel={total_excel_rows}, Parquet={total_parquet_rows}, Diff={total_row_difference}"
            )
            self._log_to_duckdb(
                "WARNING",
                f"Overall row count difference",
                operation_type="summary_validation",
                rows_processed=total_excel_rows,
                success=True,
                error_message=f"Total Excel: {total_excel_rows}, Total Parquet: {total_parquet_rows}, Difference: {total_row_difference}, Files with mismatch: {total_files_with_mismatch}",
            )

        # Save run statistics
        run_stats = {
            "total_files": len(excel_files),
            "total_sheets": sum(
                len(state.sheets_processed) for state in self.state.values()
            ),
            "total_excel_rows": total_excel_rows,
            "total_parquet_rows": total_parquet_rows,
            "total_duration_ms": int(
                (datetime.now() - self.run_start_time).total_seconds() * 1000
            ),
            "success_count": success_count,
            "failure_count": failure_count,
            "files_with_row_mismatch": total_files_with_mismatch,
            "total_row_difference": total_row_difference,
        }
        self._save_run_stats(run_stats)

        return results

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing state"""
        total_files = len(self.state)
        total_sheets = sum(len(state.sheets_processed) for state in self.state.values())
        total_outputs = sum(len(state.output_files) for state in self.state.values())

        return {
            "total_excel_files": total_files,
            "total_sheets_processed": total_sheets,
            "total_parquet_files": total_outputs,
            "last_run": datetime.now().isoformat(),
        }

    def clean_orphaned_outputs(self):
        """Remove Parquet files for Excel files that no longer exist"""
        orphaned_files = []

        for file_path_str, state in list(self.state.items()):
            if not Path(file_path_str).exists():
                # Excel file no longer exists, remove corresponding Parquet files
                for output_file in state.output_files:
                    output_path = Path(output_file)
                    if output_path.exists():
                        output_path.unlink()
                        orphaned_files.append(str(output_path))
                        self._log_to_duckdb(
                            "INFO",
                            f"Removed orphaned output file",
                            file_path=output_file,
                            operation_type="cleanup",
                        )

                # Remove from state
                del self.state[file_path_str]

        if orphaned_files:
            self.logger.info(f"Cleaned up {len(orphaned_files)} orphaned output files")
            self._log_to_duckdb(
                "INFO",
                f"Cleaned up {len(orphaned_files)} orphaned output files",
                operation_type="cleanup",
                rows_processed=len(orphaned_files),
            )
            self._save_state()

        return orphaned_files

    def get_logs(self, limit: int = 100, level: str = None):
        """Retrieve logs from DuckDB"""
        query = "SELECT * FROM pipeline_logs"
        if level:
            query += f" WHERE level = '{level}'"
        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        return self.conn.execute(query).fetchdf()

    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    """Main function to run the pipeline"""
    import argparse

    parser = argparse.ArgumentParser(description="Excel to Parquet Pipeline")
    parser.add_argument(
        "--input-dir", required=True, help="Directory containing Excel files"
    )
    parser.add_argument(
        "--output-dir", required=True, help="Directory to save Parquet files"
    )
    parser.add_argument(
        "--state-file",
        default="processing_state.json",
        help="File to track processing state",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    parser.add_argument(
        "--clean-orphaned", action="store_true", help="Clean up orphaned output files"
    )
    parser.add_argument("--pattern", default="*.xlsx", help="File pattern to match")
    parser.add_argument(
        "--db-path", default="pipeline_logs.db", help="Path to DuckDB database"
    )
    parser.add_argument(
        "--show-logs", action="store_true", help="Show recent logs after processing"
    )
    parser.add_argument(
        "--log-limit", type=int, default=10, help="Number of log entries to show"
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = ExcelParquetPipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        state_file=args.state_file,
        log_level=args.log_level,
        db_path=args.db_path,
    )

    # Clean orphaned files if requested
    if args.clean_orphaned:
        pipeline.clean_orphaned_outputs()

    # Process files
    results = pipeline.process_directory(args.pattern)

    # Print summary
    summary = pipeline.get_processing_summary()
    print("\n=== Processing Summary ===")
    print(f"Total Excel files processed: {summary['total_excel_files']}")
    print(f"Total sheets processed: {summary['total_sheets_processed']}")
    print(f"Total Parquet files created: {summary['total_parquet_files']}")
    print(f"Last run: {summary['last_run']}")

    print(f"\nProcessed files:")
    for excel_file, parquet_files in results.items():
        print(f"  {excel_file} -> {len(parquet_files)} Parquet files")

    # Show logs if requested
    if args.show_logs:
        print(f"\n=== Recent Logs (last {args.log_limit}) ===")
        logs = pipeline.get_logs(limit=args.log_limit)
        if not logs.empty:
            for _, log in logs.iterrows():
                print(f"{log['timestamp']} [{log['level']}] {log['message']}")
        else:
            print("No logs found.")

    pipeline.close()


if __name__ == "__main__":
    main()
