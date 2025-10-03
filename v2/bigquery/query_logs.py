#!/usr/bin/env python3
"""
Utility script to query and analyze DuckDB processing logs.
"""

import duckdb
import pandas as pd
import argparse
from datetime import datetime, timedelta


def main():
    parser = argparse.ArgumentParser(description="Query DuckDB processing logs")
    parser.add_argument(
        "--duckdb-path",
        default="bigquery_processing_logs.duckdb",
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--recent-runs", type=int, default=5, help="Show statistics for recent N runs"
    )
    parser.add_argument(
        "--failed-files", action="store_true", help="Show recently failed files"
    )
    parser.add_argument(
        "--file-stats",
        metavar="DAYS",
        type=int,
        help="Show file processing statistics for last N days",
    )
    parser.add_argument(
        "--export-csv",
        metavar="PREFIX",
        help="Export logs to CSV files with given prefix",
    )

    args = parser.parse_args()

    conn = duckdb.connect(args.duckdb_path)

    if args.recent_runs:
        print(f"\n=== Last {args.recent_runs} Processing Runs ===")
        df = conn.execute(
            f"""
            SELECT * FROM processing_stats 
            ORDER BY run_timestamp DESC 
            LIMIT {args.recent_runs}
        """
        ).fetchdf()
        print(df.to_string(index=False))

    if args.failed_files:
        print(f"\n=== Recently Failed Files ===")
        df = conn.execute(
            """
            SELECT file_path, table_name, error_message, processed_at 
            FROM processed_files 
            WHERE status = 'FAILED' 
            ORDER BY processed_at DESC 
            LIMIT 20
        """
        ).fetchdf()
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No failed files found.")

    if args.file_stats:
        cutoff_date = datetime.now() - timedelta(days=args.file_stats)
        print(f"\n=== File Processing Stats (Last {args.file_stats} days) ===")
        df = conn.execute(
            """
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
        """,
            [cutoff_date],
        ).fetchdf()
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print(f"No processing data found for the last {args.file_stats} days.")

    if args.export_csv:
        # Export all data to CSV files
        tables = ["processed_files", "processing_stats"]
        for table in tables:
            filename = f"{args.export_csv}_{table}.csv"
            df = conn.execute(f"SELECT * FROM {table}").fetchdf()
            df.to_csv(filename, index=False)
            print(f"Exported {table} to {filename}")

    conn.close()


if __name__ == "__main__":
    main()
