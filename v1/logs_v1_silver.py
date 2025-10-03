#!/usr/bin/env python3
"""
Silver Layer Processing Script
Processes bronze layer Parquet files and applies transformations for silver layer.
"""

import os
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from dataclasses import dataclass, asdict


@dataclass
class SilverProcessingState:
    """Track processing state for silver layer transformations"""

    input_file: str
    input_hash: str
    output_file: str
    processed_at: str
    transformations_applied: List[str]
    row_count: int
    column_count: int


class SilverLayerProcessor:
    """Processor for transforming bronze layer data to silver layer"""

    def __init__(
        self,
        bronze_dir: str,
        silver_dir: str,
        state_file: str = "silver_processing_state.json",
        log_level: str = "INFO",
    ):
        """
        Initialize the silver layer processor

        Args:
            bronze_dir: Directory containing bronze layer Parquet files
            silver_dir: Directory to save silver layer Parquet files
            state_file: File to track processing state
            log_level: Logging level
        """
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.state_file = Path(state_file)

        # Create directories if they don't exist
        self.silver_dir.mkdir(parents=True, exist_ok=True)

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
                logging.FileHandler("silver_layer_processor.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def _load_state(self) -> Dict[str, SilverProcessingState]:
        """Load processing state from file"""
        if not self.state_file.exists():
            return {}

        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
                return {
                    path: SilverProcessingState(**state_data)
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

        return current_hash != state.input_hash

    def _apply_standard_transformations(
        self, df: pd.DataFrame, file_path: Path
    ) -> pd.DataFrame:
        """
        Apply standard transformations for silver layer

        Args:
            df: Input DataFrame from bronze layer
            file_path: Path to the input file for context

        Returns:
            Transformed DataFrame
        """
        transformations = []
        result_df = df.copy()

        # 1. Standardize column names
        original_columns = list(result_df.columns)
        result_df.columns = [
            self._standardize_column_name(col) for col in result_df.columns
        ]
        if original_columns != list(result_df.columns):
            transformations.append("column_name_standardization")

        # 2. Handle missing values
        missing_before = result_df.isnull().sum().sum()

        # Fill numeric missing values with 0
        numeric_cols = result_df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            result_df[col] = result_df[col].fillna(0)

        # Fill string missing values with 'Unknown'
        string_cols = result_df.select_dtypes(include=["object"]).columns
        for col in string_cols:
            if col not in ["_source_sheet", "_processed_at"]:  # Skip metadata columns
                result_df[col] = result_df[col].fillna("Unknown")

        missing_after = result_df.isnull().sum().sum()
        if missing_before > missing_after:
            transformations.append("missing_value_handling")

        # 3. Data type standardization
        type_changes = self._standardize_data_types(result_df)
        if type_changes:
            transformations.append("data_type_standardization")

        # 4. Remove duplicate rows
        duplicates_before = len(result_df)
        result_df = result_df.drop_duplicates()
        duplicates_after = len(result_df)
        if duplicates_before != duplicates_after:
            transformations.append(
                f"duplicate_removal({duplicates_before - duplicates_after} rows)"
            )

        # 5. Add silver layer metadata
        result_df["_silver_processed_at"] = datetime.now().isoformat()
        result_df["_silver_input_file"] = str(file_path.name)

        return result_df, transformations

    def _standardize_column_name(self, column_name: str) -> str:
        """Standardize column names for consistency"""
        if not isinstance(column_name, str):
            column_name = str(column_name)

        # Convert to lowercase
        column_name = column_name.lower()

        # Replace special characters with underscores
        column_name = column_name.replace(" ", "_").replace("-", "_").replace(".", "_")

        # Remove multiple underscores
        while "__" in column_name:
            column_name = column_name.replace("__", "_")

        # Remove leading/trailing underscores
        column_name = column_name.strip("_")

        # Ensure column name is not empty
        if not column_name:
            column_name = "unknown_column"

        return column_name

    def _standardize_data_types(self, df: pd.DataFrame) -> bool:
        """Standardize data types across the dataset"""
        changes_made = False

        for col in df.columns:
            # Skip metadata columns
            if col.startswith("_"):
                continue

            # Try to convert string columns that look like dates
            if df[col].dtype == "object":
                try:
                    # Check if column contains date-like strings
                    sample = df[col].dropna().head(10)
                    if not sample.empty:
                        # Try to convert to datetime
                        pd.to_datetime(sample, errors="raise")
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                        changes_made = True
                        self.logger.info(f"Converted column '{col}' to datetime")
                except:
                    pass

            # Convert boolean-like strings to actual booleans
            if df[col].dtype == "object":
                unique_vals = df[col].dropna().unique()
                if set(unique_vals).issubset(
                    {"true", "false", "True", "False", "1", "0", "yes", "no"}
                ):
                    df[col] = df[col].replace(
                        {
                            "true": True,
                            "True": True,
                            "1": True,
                            "yes": True,
                            "false": False,
                            "False": False,
                            "0": False,
                            "no": False,
                        }
                    )
                    changes_made = True
                    self.logger.info(f"Converted column '{col}' to boolean")

        return changes_made

    def _process_parquet_file(self, file_path: Path) -> Optional[SilverProcessingState]:
        """
        Process a single Parquet file from bronze to silver layer

        Args:
            file_path: Path to bronze layer Parquet file

        Returns:
            Processing state for the file
        """
        try:
            # Read the Parquet file
            table = pq.read_table(file_path)
            df = table.to_pandas()

            self.logger.info(
                f"Processing {file_path} with {len(df)} rows and {len(df.columns)} columns"
            )

            # Apply transformations
            transformed_df, transformations = self._apply_standard_transformations(
                df, file_path
            )

            # Create output path preserving directory structure
            relative_path = file_path.relative_to(self.bronze_dir)
            output_path = self.silver_dir / relative_path

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to Parquet with better compression
            silver_table = pa.Table.from_pandas(transformed_df)
            pq.write_table(
                silver_table,
                output_path,
                compression="zstd",  # Better compression than snappy
                compression_level=3,
            )

            # Create processing state
            processing_state = SilverProcessingState(
                input_file=str(file_path),
                input_hash=self._get_file_hash(file_path),
                output_file=str(output_path),
                processed_at=datetime.now().isoformat(),
                transformations_applied=transformations,
                row_count=len(transformed_df),
                column_count=len(transformed_df.columns),
            )

            self.logger.info(f"Successfully processed {file_path} -> {output_path}")
            self.logger.info(f"Transformations applied: {transformations}")

            return processing_state

        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            return None

    def process_bronze_layer(self) -> Dict[str, SilverProcessingState]:
        """
        Process all bronze layer Parquet files

        Returns:
            Dictionary mapping input files to processing states
        """
        results = {}

        # Recursively find all Parquet files in bronze directory
        parquet_files = list(self.bronze_dir.rglob("*.parquet"))

        if not parquet_files:
            self.logger.warning(f"No Parquet files found in {self.bronze_dir}")
            return results

        self.logger.info(f"Found {len(parquet_files)} Parquet files to process")

        for file_path in parquet_files:
            if self._needs_processing(file_path):
                self.logger.info(f"Processing {file_path}")
                processing_state = self._process_parquet_file(file_path)

                if processing_state:
                    self.state[str(file_path)] = processing_state
                    results[str(file_path)] = processing_state
            else:
                self.logger.info(
                    f"Skipping {file_path} (already processed, no changes)"
                )
                results[str(file_path)] = self.state[str(file_path)]

        # Save state after processing
        self._save_state()

        return results

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of silver layer processing"""
        total_files = len(self.state)
        total_rows = sum(state.row_count for state in self.state.values())
        total_columns = sum(state.column_count for state in self.state.values())

        # Count transformations applied
        transformation_counts = {}
        for state in self.state.values():
            for transformation in state.transformations_applied:
                transformation_counts[transformation] = (
                    transformation_counts.get(transformation, 0) + 1
                )

        return {
            "total_files_processed": total_files,
            "total_rows_processed": total_rows,
            "total_columns_processed": total_columns,
            "transformations_applied": transformation_counts,
            "last_run": datetime.now().isoformat(),
        }

    def clean_orphaned_outputs(self):
        """Remove silver layer files for bronze files that no longer exist"""
        orphaned_files = []

        for file_path_str, state in list(self.state.items()):
            if not Path(file_path_str).exists():
                # Bronze file no longer exists, remove corresponding silver file
                output_path = Path(state.output_file)
                if output_path.exists():
                    output_path.unlink()
                    orphaned_files.append(str(output_path))

                # Remove from state
                del self.state[file_path_str]

        if orphaned_files:
            self.logger.info(f"Cleaned up {len(orphaned_files)} orphaned silver files")
            self._save_state()

        return orphaned_files


def main():
    """Main function to run the silver layer processor"""
    import argparse

    parser = argparse.ArgumentParser(description="Silver Layer Processor")
    parser.add_argument(
        "--bronze-dir",
        required=True,
        help="Directory containing bronze layer Parquet files",
    )
    parser.add_argument(
        "--silver-dir",
        required=True,
        help="Directory to save silver layer Parquet files",
    )
    parser.add_argument(
        "--state-file",
        default="silver_processing_state.json",
        help="File to track processing state",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    parser.add_argument(
        "--clean-orphaned", action="store_true", help="Clean up orphaned output files"
    )

    args = parser.parse_args()

    # Initialize processor
    processor = SilverLayerProcessor(
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        state_file=args.state_file,
        log_level=args.log_level,
    )

    # Clean orphaned files if requested
    if args.clean_orphaned:
        processor.clean_orphaned_outputs()

    # Process files
    results = processor.process_bronze_layer()

    # Print summary
    summary = processor.get_processing_summary()
    print("\n=== Silver Layer Processing Summary ===")
    print(f"Total files processed: {summary['total_files_processed']}")
    print(f"Total rows processed: {summary['total_rows_processed']}")
    print(f"Total columns processed: {summary['total_columns_processed']}")
    print(
        f"Transformations applied: {json.dumps(summary['transformations_applied'], indent=2)}"
    )
    print(f"Last run: {summary['last_run']}")

    print(f"\nProcessed files:")
    for bronze_file, state in results.items():
        print(f"  {bronze_file} -> {state.output_file}")
        print(f"    Rows: {state.row_count}, Columns: {state.column_count}")
        print(f"    Transformations: {state.transformations_applied}")


if __name__ == "__main__":
    main()
