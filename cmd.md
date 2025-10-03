To run the scripts from the command line (CMD), here's how to do it:

## 1. First, Install Required Dependencies

```bash
pip install pandas pyarrow duckdb watchdog docker
```

## 2. Running the Excel to Parquet Pipeline

### Basic command:
```bash
python excel_parquet_pipeline.py --input-dir "C:\etl\data\input" --output-dir "C:\etl\data\output"
```
C:\etl\data\input
data\input
### Full command with all options:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\path\to\input" ^
  --output-dir "C:\path\to\output" ^
  --state-file "processing_state.json" ^
  --log-level INFO ^
  --clean-orphaned ^
  --pattern "*.xlsx" ^
  --db-path "pipeline_logs.db" ^
  --show-logs ^
  --log-limit 10
```

### Example with actual paths:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\Data\ExcelFiles" ^
  --output-dir "C:\Data\ParquetOutput" ^
  --state-file "C:\Data\state.json" ^
  --db-path "C:\Data\logs.db" ^
  --show-logs
```

## 3. Running the File Watcher

### Basic command:
```bash
python file_watcher.py
```

### With environment variables:
```bash
set WATCH_DIR=C:\Data\ExcelFiles
set PIPELINE_CONTAINER=excel-parquet-pipeline
set DB_PATH=C:\Data\watcher_logs.db

python file_watcher.py
```

### Or set variables inline:
```bash
set WATCH_DIR=C:\Data\ExcelFiles && set PIPELINE_CONTAINER=excel-pipeline && python file_watcher.py
```

## 4. Common Command Examples

### Process files once and show logs:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\ExcelFiles" ^
  --output-dir "C:\ParquetOutput" ^
  --show-logs
```

### Process files and clean up orphaned outputs:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\ExcelFiles" ^
  --output-dir "C:\ParquetOutput" ^
  --clean-orphaned
```

### Process only .xls files:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\ExcelFiles" ^
  --output-dir "C:\ParquetOutput" ^
  --pattern "*.xls"
```

### Run with debug logging:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\ExcelFiles" ^
  --output-dir "C:\ParquetOutput" ^
  --log-level DEBUG
```

## 5. Useful CMD Tips

### Navigate to script directory:
```bash
cd C:\path\to\scripts
```

### Check Python version:
```bash
python --version
```

### Create a batch file for easy execution:
Create a file named `run_pipeline.bat`:
```batch
@echo off
python excel_parquet_pipeline.py ^
  --input-dir "C:\Data\ExcelFiles" ^
  --output-dir "C:\Data\ParquetOutput" ^
  --state-file "processing_state.json" ^
  --show-logs
pause
```

Then just double-click the `.bat` file to run.

## 6. Docker-based Execution (if using containers)

If you're running in Docker, the commands would be similar but executed inside the container:

```bash
# Build the image
docker build -t excel-parquet-pipeline .

# Run the pipeline
docker run -v C:\Data:/data excel-parquet-pipeline ^
  python excel_parquet_pipeline.py ^
  --input-dir "/data/input" ^
  --output-dir "/data/output" ^
  --state-file "/data/state.json"
```

## 7. Troubleshooting Common Issues

### If you get "python is not recognized":
```bash
# Use python3 instead
python3 excel_parquet_pipeline.py --input-dir "C:\path\to\input" --output-dir "C:\path\to\output"

# Or specify full path
C:\Python39\python.exe excel_parquet_pipeline.py --input-dir "C:\path\to\input" --output-dir "C:\path\to\output"
```

### If you get module not found errors:
```bash
# Install missing packages
pip install pandas pyarrow duckdb
```

### If paths have spaces, use quotes:
```bash
python excel_parquet_pipeline.py ^
  --input-dir "C:\My Data\Excel Files" ^
  --output-dir "C:\My Data\Output Files"
```

The scripts will process Excel files and create corresponding Parquet files while logging all activities to both the console and the DuckDB database!