python parquet_to_bigquery_bulk_upload.py ^    --project-id pharma-443114 ^    --dataset-id raw_layer_parquet ^    --source-dir C:\etl\data\output ^    --duckdb-path C:\etl\bigquery_processing_logs.duckdb ^    --credentials pharma-443114-7a8327afd98b.json

python parquet_to_bigquery_bulk_upload.py \
    --project-id pharma-443114 \
    --dataset-id pharma_lakehouse_dataset \
    --source-dir /data\output \
    --duckdb-path /processing_logs.duckdb

Here's the updated script that saves logs to DuckDB instead of a text file:

## Updated Script: `parquet_to_bigquery_bulk_upload.py`

```python
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
    def __init__(self, project_id: str, dataset_id: str, credentials_path: Optional[str] = None,
                 duckdb_path: str = "bigquery_processing_logs.duckdb"):
        """
        Initialize the BigQuery loader with DuckDB logging.
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            credentials_path: Path to service account credentials JSON file
            duckdb_path: Path to DuckDB database file for logging
        """
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = f"{project_id}.{dataset_id}"
        
        # Initialize DuckDB for logging
        self.duckdb_path = duckdb_path
        self._init_duckdb()
        
        # Create dataset if it doesn't exist
        self._create_dataset_if_not_exists()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _init_duckdb(self):
        """Initialize DuckDB database and tables for logging."""
        self.conn = duckdb.connect(self.duckdb_path)
        
        # Create processed_files table
        self.conn.execute("""
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
        """)
        
        # Create processing_stats table for summary statistics
        self.conn.execute("""
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
        """)
        
        self.logger.info(f"Initialized DuckDB logging database: {self.duckdb_path}")

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

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file for change detection."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_file_metadata(self, file_path: Path) -> Tuple[str, int]:
        """Get file hash and size."""
        file_hash = self._calculate_file_hash(file_path)
        file_size = file_path.stat().st_size
        return file_hash, file_size

    def is_file_processed(self, file_path: str, current_hash: str) -> Tuple[bool, Optional[Dict]]:
        """
        Check if file has been processed and if it has changed.
        
        Returns:
            Tuple of (is_processed, file_metadata)
        """
        result = self.conn.execute("""
            SELECT file_path, file_hash, table_name, status, processed_at 
            FROM processed_files 
            WHERE file_path = ? AND status = 'SUCCESS'
        """, [file_path]).fetchone()
        
        if result:
            previous_hash = result[1]
            # If file has changed, it needs to be reprocessed
            if previous_hash == current_hash:
                return True, {
                    'file_path': result[0],
                    'file_hash': result[1],
                    'table_name': result[2],
                    'status': result[3],
                    'processed_at': result[4]
                }
            else:
                self.logger.info(f"File {file_path} has changed, will reprocess")
                return False, None
        return False, None

    def log_file_processing_start(self, file_path: str, file_hash: str, file_size: int, table_name: str):
        """Log the start of file processing."""
        self.conn.execute("""
            INSERT OR REPLACE INTO processed_files 
            (file_path, file_hash, file_size, table_name, processed_at, status, records_processed, error_message, processing_time_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [file_path, file_hash, file_size, table_name, datetime.now(), 'STARTED', 0, None, 0])

    def log_file_processing_result(self, file_path: str, status: str, records_processed: int = 0, 
                                 error_message: str = None, processing_time: float = 0):
        """Log the result of file processing."""
        self.conn.execute("""
            UPDATE processed_files 
            SET status = ?, records_processed = ?, error_message = ?, processing_time_seconds = ?
            WHERE file_path = ?
        """, [status, records_processed, error_message, processing_time, file_path])

    def log_processing_stats(self, run_id: str, stats: Dict):
        """Log overall processing statistics for the run."""
        self.conn.execute("""
            INSERT INTO processing_stats 
            (run_id, run_timestamp, total_files, successful_files, failed_files, skipped_files, 
             total_processing_time_seconds, avg_processing_time_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id,
            datetime.now(),
            stats['total_files'],
            stats['successful_files'],
            stats['failed_files'],
            stats['skipped_files'],
            stats['total_processing_time'],
            stats['avg_processing_time']
        ])

    def get_processing_summary(self) -> pd.DataFrame:
        """Get summary of processing history."""
        return self.conn.execute("""
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
        """).fetchdf()

    def get_file_processing_history(self, file_path: str = None) -> pd.DataFrame:
        """Get processing history for files."""
        if file_path:
            return self.conn.execute("""
                SELECT * FROM processed_files 
                WHERE file_path = ? 
                ORDER BY processed_at DESC
            """, [file_path]).fetchdf()
        else:
            return self.conn.execute("""
                SELECT * FROM processed_files 
                ORDER BY processed_at DESC
            """).fetchdf()

    def find_parquet_files(self, root_dir: str) -> List[Path]:
        """
        Recursively find all Parquet files in nested directories.
        
        Args:
            root_dir: Root directory to search for Parquet files
            
        Returns:
            List of Path objects for Parquet files
        """
        root_path = Path(root_dir)
        parquet_files = list(root_path.rglob("*.parquet"))
        
        self.logger.info(f"Found {len(parquet_files)} Parquet files in {root_dir}")
        return parquet_files

    def infer_table_name(self, file_path: Path, table_prefix: Optional[str] = None) -> str:
        """
        Infer table name from file path.
        
        Args:
            file_path: Path to Parquet file
            table_prefix: Optional prefix for table names
            
        Returns:
            Table name for BigQuery
        """
        # Remove file extension and replace special characters
        table_name = file_path.stem.lower()
        table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)
        
        # Use folder structure for table naming
        relative_path = file_path.relative_to(file_path.parents[len(file_path.parents) - 1])
        path_parts = [p.stem.lower() for p in relative_path.parents if p.stem]
        path_parts.reverse()
        
        if path_parts:
            table_name = '_'.join(path_parts + [table_name])
        
        if table_prefix:
            table_name = f"{table_prefix}_{table_name}"
        
        # Ensure table name is valid for BigQuery
        table_name = table_name[:1024]  # BigQuery table name length limit
        return table_name

    def get_file_row_count(self, file_path: Path) -> int:
        """Get approximate row count from Parquet file metadata."""
        try:
            # Efficiently get row count without loading entire file
            parquet_file = pd.read_parquet(file_path, engine='pyarrow')
            return len(parquet_file)
        except Exception as e:
            self.logger.warning(f"Could not get row count for {file_path}: {e}")
            return 0

    def create_table_if_not_exists(self, table_name: str, schema: List[bigquery.SchemaField]):
        """
        Create BigQuery table if it doesn't exist.
        
        Args:
            table_name: Name of the table
            schema: Table schema
        """
        table_id = f"{self.dataset_ref}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            self.logger.info(f"Table {table_id} already exists")
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            self.logger.info(f"Created table {table_id}")

    def load_file_to_bigquery(self, file_path: Path, table_name: str, 
                            write_disposition: str = 'WRITE_APPEND') -> Tuple[bool, int, float, Optional[str]]:
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
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
                autodetect=True,
            )
            
            table_id = f"{self.dataset_ref}.{table_name}"
            
            with open(file_path, 'rb') as source_file:
                job = self.client.load_table_from_file(
                    source_file, table_id, job_config=job_config
                )
            
            job.result()  # Wait for job to complete
            
            if job.errors:
                error_message = str(job.errors)
                self.logger.error(f"Errors loading {file_path}: {job.errors}")
                return False, 0, 0, error_message
            
            # Get number of processed records
            table = self.client.get_table(table_id)
            records_processed = job.output_rows or 0
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"Successfully loaded {file_path} to {table_id} "
                           f"({records_processed} records, {processing_time:.2f}s)")
            return True, records_processed, processing_time, None
            
        except Exception as e:
            error_message = str(e)
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Error loading {file_path} to BigQuery: {e}")
            return False, 0, processing_time, error_message

    def process_files(self, root_dir: str, table_prefix: Optional[str] = None,
                     incremental: bool = True, write_disposition: str = 'WRITE_APPEND',
                     run_id: str = None):
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
        
        stats = {
            'total_files': len(parquet_files),
            'successful_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_processing_time': 0,
            'processing_times': []
        }
        
        for file_path in parquet_files:
            file_path_str = str(file_path.absolute())
            file_hash, file_size = self._get_file_metadata(file_path)
            
            # Check if file should be processed
            if incremental:
                is_processed, file_metadata = self.is_file_processed(file_path_str, file_hash)
                if is_processed:
                    self.logger.info(f"Skipping already processed file: {file_path}")
                    stats['skipped_files'] += 1
                    continue
            
            table_name = self.infer_table_name(file_path, table_prefix)
            
            # Log processing start
            self.log_file_processing_start(file_path_str, file_hash, file_size, table_name)
            
            self.logger.info(f"Processing {file_path} -> table {table_name}")
            
            # Process the file
            success, records_processed, processing_time, error_message = self.load_file_to_bigquery(
                file_path, table_name, write_disposition
            )
            
            # Log result
            status = 'SUCCESS' if success else 'FAILED'
            self.log_file_processing_result(
                file_path_str, status, records_processed, error_message, processing_time
            )
            
            # Update statistics
            if success:
                stats['successful_files'] += 1
                stats['total_processing_time'] += processing_time
                stats['processing_times'].append(processing_time)
            else:
                stats['failed_files'] += 1
        
        # Calculate average processing time
        if stats['processing_times']:
            stats['avg_processing_time'] = sum(stats['processing_times']) / len(stats['processing_times'])
        else:
            stats['avg_processing_time'] = 0
        
        # Log overall statistics
        self.log_processing_stats(run_id, stats)
        
        self.logger.info(f"Processing complete for run {run_id}:")
        self.logger.info(f"  Total files: {stats['total_files']}")
        self.logger.info(f"  Successful: {stats['successful_files']}")
        self.logger.info(f"  Failed: {stats['failed_files']}")
        self.logger.info(f"  Skipped: {stats['skipped_files']}")
        self.logger.info(f"  Total processing time: {stats['total_processing_time']:.2f}s")
        self.logger.info(f"  Average processing time: {stats['avg_processing_time']:.2f}s")
        
        # Save DuckDB changes
        self.conn.commit()

def main():
    parser = argparse.ArgumentParser(description='Bulk upload Parquet files to BigQuery with DuckDB logging')
    parser.add_argument('--project-id', required=True, help='Google Cloud project ID')
    parser.add_argument('--dataset-id', required=True, help='BigQuery dataset ID')
    parser.add_argument('--source-dir', required=True, help='Source directory with Parquet files')
    parser.add_argument('--credentials', help='Path to service account credentials JSON file')
    parser.add_argument('--table-prefix', help='Prefix for table names')
    parser.add_argument('--duckdb-path', default='bigquery_processing_logs.duckdb', help='Path to DuckDB log database')
    parser.add_argument('--full-reload', action='store_true', 
                       help='Force full reload (ignore processed files log)')
    parser.add_argument('--write-disposition', default='WRITE_APPEND',
                       choices=['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'],
                       help='BigQuery write disposition')
    parser.add_argument('--run-id', help='Unique identifier for this processing run')
    parser.add_argument('--show-stats', action='store_true', help='Show processing statistics and exit')
    parser.add_argument('--show-file-history', metavar='FILE_PATH', help='Show processing history for a specific file')
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = ParquetToBigQueryLoader(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        credentials_path=args.credentials,
        duckdb_path=args.duckdb_path
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
        run_id=args.run_id
    )

if __name__ == "__main__":
    main()
```

## Updated Requirements: `requirements.txt`

```txt
google-cloud-bigquery>=2.0.0
pandas>=1.3.0
pyarrow>=5.0.0
duckdb>=0.8.0
```

## New Utility Script: `query_logs.py`

```python
#!/usr/bin/env python3
"""
Utility script to query and analyze DuckDB processing logs.
"""

import duckdb
import pandas as pd
import argparse
from datetime import datetime, timedelta

def main():
    parser = argparse.ArgumentParser(description='Query DuckDB processing logs')
    parser.add_argument('--duckdb-path', default='bigquery_processing_logs.duckdb', 
                       help='Path to DuckDB database')
    parser.add_argument('--recent-runs', type=int, default=5,
                       help='Show statistics for recent N runs')
    parser.add_argument('--failed-files', action='store_true',
                       help='Show recently failed files')
    parser.add_argument('--file-stats', metavar='DAYS', type=int,
                       help='Show file processing statistics for last N days')
    parser.add_argument('--export-csv', metavar='PREFIX',
                       help='Export logs to CSV files with given prefix')
    
    args = parser.parse_args()
    
    conn = duckdb.connect(args.duckdb_path)
    
    if args.recent_runs:
        print(f"\n=== Last {args.recent_runs} Processing Runs ===")
        df = conn.execute(f"""
            SELECT * FROM processing_stats 
            ORDER BY run_timestamp DESC 
            LIMIT {args.recent_runs}
        """).fetchdf()
        print(df.to_string(index=False))
    
    if args.failed_files:
        print(f"\n=== Recently Failed Files ===")
        df = conn.execute("""
            SELECT file_path, table_name, error_message, processed_at 
            FROM processed_files 
            WHERE status = 'FAILED' 
            ORDER BY processed_at DESC 
            LIMIT 20
        """).fetchdf()
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No failed files found.")
    
    if args.file_stats:
        cutoff_date = datetime.now() - timedelta(days=args.file_stats)
        print(f"\n=== File Processing Stats (Last {args.file_stats} days) ===")
        df = conn.execute("""
            SELECT 
                table_name,
                COUNT(*) as total_files,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                AVG(processing_time_seconds) as avg_processing_time,
                SUM(records_processed) as total_records
            FROM processed_files 
            WHERE processed_at >= ?
            GROUP BY table_name
            ORDER BY total_files DESC
        """, [cutoff_date]).fetchdf()
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print(f"No processing data found for the last {args.file_stats} days.")
    
    if args.export_csv:
        # Export all data to CSV files
        tables = ['processed_files', 'processing_stats']
        for table in tables:
            filename = f"{args.export_csv}_{table}.csv"
            df = conn.execute(f"SELECT * FROM {table}").fetchdf()
            df.to_csv(filename, index=False)
            print(f"Exported {table} to {filename}")
    
    conn.close()

if __name__ == "__main__":
    main()
```

## Usage Examples

### 1. Basic incremental upload with DuckDB logging:
```bash
python parquet_to_bigquery_bulk_upload.py \
    --project-id my-project \
    --dataset-id my_dataset \
    --source-dir /path/to/parquet/files \
    --duckdb-path /path/to/logs.duckdb
```

### 2. Full reload with custom run ID:
```bash
python parquet_to_bigquery_bulk_upload.py \
    --project-id my-project \
    --dataset-id my_dataset \
    --source-dir /path/to/parquet/files \
    --full-reload \
    --run-id "full_reload_2024"
```

### 3. Query processing statistics:
```bash
# Show recent runs
python parquet_to_bigquery_bulk_upload.py --show-stats

# Show file history
python parquet_to_bigquery_bulk_upload.py --show-file-history "/path/to/file.parquet"

# Use utility script for advanced queries
python query_logs.py --recent-runs 10
python query_logs.py --failed-files
python query_logs.py --file-stats 30
```

## Key DuckDB Features Added

1. **File Change Detection**: Uses MD5 hashes to detect modified files
2. **Detailed Logging**: Tracks processing time, record counts, errors
3. **Run Statistics**: Aggregated statistics for each processing run
4. **Query Capabilities**: Easy querying of processing history
5. **CSV Export**: Export logs for external analysis
6. **Performance Metrics**: Track processing times and success rates

The DuckDB integration provides much richer logging and analytics capabilities compared to the simple text file approach, while maintaining high performance for log queries.