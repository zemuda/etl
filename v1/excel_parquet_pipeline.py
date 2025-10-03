#!/usr/bin/env python3
"""
Excel to Parquet Pipeline Script
Incrementally processes nested Excel files and converts them to Parquet format.
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


@dataclass
class ProcessingState:
    """Track processing state for incremental updates"""

    file_path: str
    file_hash: str
    last_modified: float
    processed_at: str
    sheets_processed: List[str]
    output_files: List[str]


class ExcelParquetPipeline:
    """Pipeline for converting Excel files to Parquet format incrementally"""

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        state_file: str = "processing_state.json",
        log_level: str = "INFO",
    ):
        """
        Initialize the pipeline

        Args:
            input_dir: Directory containing Excel files
            output_dir: Directory to save Parquet files
            state_file: File to track processing state
            log_level: Logging level
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.state_file = Path(state_file)

        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self._setup_logging(log_level)

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
            return {}

    def _save_state(self):
        """Save processing state to file"""
        try:
            state_data = {path: asdict(state) for path, state in self.state.items()}
            with open(self.state_file, "w") as f:
                json.dump(state_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save state file: {e}")

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

        # Convert object columns with mixed types
        # for col in df.select_dtypes(include=['object']).columns:
        #     # Skip metadata columns
        #     if col.startswith('_'):
        #         continue

        #     # Try to convert to numeric if possible
        #     try:
        #         df[col] = pd.to_numeric(df[col], errors='ignore')
        #     except:
        #         pass

        return df

    def _process_excel_file(self, file_path: Path) -> List[str]:
        """
        Process a single Excel file and convert to Parquet

        Args:
            file_path: Path to Excel file

        Returns:
            List of output Parquet file paths
        """
        output_files = []
        processed_sheets = []

        try:
            # Read all sheets from Excel file
            excel_file = pd.ExcelFile(file_path)

            self.logger.info(
                f"Processing {file_path} with {len(excel_file.sheet_names)} sheets"
            )

            # Get relative path from input directory to preserve structure
            relative_path = file_path.relative_to(self.input_dir)
            output_subdir = self.output_dir / relative_path.parent

            # Create output subdirectory if it doesn't exist
            output_subdir.mkdir(parents=True, exist_ok=True)

            for sheet_name in excel_file.sheet_names:
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

                    # Generate output filename with preserved structure
                    base_name = file_path.stem
                    safe_sheet_name = "".join(
                        c for c in sheet_name if c.isalnum() or c in (" ", "-", "_")
                    ).strip()
                    safe_sheet_name = safe_sheet_name.replace(" ", "_")

                    output_filename = f"{base_name}_{safe_sheet_name}.parquet"
                    output_path = output_subdir / output_filename

                    # Convert to Parquet
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, output_path, compression="snappy")

                    output_files.append(str(output_path))
                    processed_sheets.append(sheet_name)

                    self.logger.info(f"Converted sheet '{sheet_name}' to {output_path}")

                except Exception as e:
                    self.logger.error(
                        f"Error processing sheet '{sheet_name}' in {file_path}: {e}"
                    )
                    continue

            # Update processing state
            file_hash = self._get_file_hash(file_path)
            file_modified = file_path.stat().st_mtime

            self.state[str(file_path)] = ProcessingState(
                file_path=str(file_path),
                file_hash=file_hash,
                last_modified=file_modified,
                processed_at=datetime.now().isoformat(),
                sheets_processed=processed_sheets,
                output_files=output_files,
            )

        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {e}")

        return output_files

    # def _process_excel_file(self, file_path: Path) -> List[str]:
    #     """
    #     Process a single Excel file and convert to Parquet

    #     Args:
    #         file_path: Path to Excel file

    #     Returns:
    #         List of output Parquet file paths
    #     """
    #     output_files = []
    #     processed_sheets = []

    #     try:
    #         # Read all sheets from Excel file
    #         excel_file = pd.ExcelFile(file_path)

    #         self.logger.info(f"Processing {file_path} with {len(excel_file.sheet_names)} sheets")

    #         for sheet_name in excel_file.sheet_names:
    #             try:
    #                 # Read sheet
    #                 df = pd.read_excel(file_path, sheet_name=sheet_name,
    #                                  header=0, index_col=None)

    #                 if df.empty:
    #                     self.logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
    #                     continue

    #                 # Clean and flatten data
    #                 df = self._clean_dataframe(df)
    #                 df = self._flatten_nested_data(df, sheet_name)

    #                 # Generate output filename
    #                 base_name = file_path.stem
    #                 safe_sheet_name = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '-', '_')).strip()
    #                 safe_sheet_name = safe_sheet_name.replace(' ', '_')

    #                 output_filename = f"{base_name}_{safe_sheet_name}.parquet"
    #                 output_path = self.output_dir / output_filename

    #                 # Convert to Parquet
    #                 table = pa.Table.from_pandas(df)
    #                 pq.write_table(table, output_path, compression='snappy')

    #                 output_files.append(str(output_path))
    #                 processed_sheets.append(sheet_name)

    #                 self.logger.info(f"Converted sheet '{sheet_name}' to {output_path}")

    #             except Exception as e:
    #                 self.logger.error(f"Error processing sheet '{sheet_name}' in {file_path}: {e}")
    #                 continue

    #         # Update processing state
    #         file_hash = self._get_file_hash(file_path)
    #         file_modified = file_path.stat().st_mtime

    #         self.state[str(file_path)] = ProcessingState(
    #             file_path=str(file_path),
    #             file_hash=file_hash,
    #             last_modified=file_modified,
    #             processed_at=datetime.now().isoformat(),
    #             sheets_processed=processed_sheets,
    #             output_files=output_files
    #         )

    #     except Exception as e:
    #         self.logger.error(f"Error processing Excel file {file_path}: {e}")

    #     return output_files

    def process_directory(self, file_pattern: str = "*.xls") -> Dict[str, List[str]]:
        """
        Process all Excel files in the input directory and subdirectories

        Args:
            file_pattern: Glob pattern for Excel files

        Returns:
            Dictionary mapping file paths to output Parquet files
        """
        results = {}

        # Recursively find all Excel files in input directory and subdirectories
        excel_files = []
        excel_files.extend(list(self.input_dir.rglob("*.xls")))  # Include .xls files
        excel_files.extend(list(self.input_dir.rglob("*.xlsx")))  # Include .xlsx files
        excel_files.extend(list(self.input_dir.rglob("*.xlsm")))  # Include .xlsm files
        excel_files.extend(list(self.input_dir.rglob("*.xlsb")))  # Include .xlsb files

        if not excel_files:
            self.logger.warning(
                f"No Excel files found in {self.input_dir} and its subdirectories"
            )
            return results

        self.logger.info(f"Found {len(excel_files)} Excel files to process")

        for file_path in excel_files:
            if self._needs_processing(file_path):
                self.logger.info(f"Processing {file_path}")
                output_files = self._process_excel_file(file_path)
                results[str(file_path)] = output_files
            else:
                self.logger.info(
                    f"Skipping {file_path} (already processed, no changes)"
                )
                results[str(file_path)] = self.state[str(file_path)].output_files

        # Save state after processing
        self._save_state()

        return results

    # def process_directory(self, file_pattern: str = "*.xlsx") -> Dict[str, List[str]]:
    #     """
    #     Process all Excel files in the input directory

    #     Args:
    #         file_pattern: Glob pattern for Excel files

    #     Returns:
    #         Dictionary mapping file paths to output Parquet files
    #     """
    #     results = {}
    #     excel_files = list(self.input_dir.glob(file_pattern))
    #     excel_files.extend(list(self.input_dir.glob("*.xls")))  # Include .xls files too

    #     if not excel_files:
    #         self.logger.warning(f"No Excel files found in {self.input_dir}")
    #         return results

    #     self.logger.info(f"Found {len(excel_files)} Excel files to process")

    #     for file_path in excel_files:
    #         if self._needs_processing(file_path):
    #             self.logger.info(f"Processing {file_path}")
    #             output_files = self._process_excel_file(file_path)
    #             results[str(file_path)] = output_files
    #         else:
    #             self.logger.info(f"Skipping {file_path} (already processed, no changes)")
    #             results[str(file_path)] = self.state[str(file_path)].output_files

    #     # Save state after processing
    #     self._save_state()

    #     return results

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

                # Remove from state
                del self.state[file_path_str]

        if orphaned_files:
            self.logger.info(f"Cleaned up {len(orphaned_files)} orphaned output files")
            self._save_state()

        return orphaned_files


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

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = ExcelParquetPipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        state_file=args.state_file,
        log_level=args.log_level,
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


if __name__ == "__main__":
    main()
