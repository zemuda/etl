# Excel to Parquet Pipeline

This project provides a pipeline for converting Excel files to Parquet format. It incrementally processes nested Excel files and manages the processing state to ensure efficient handling of data.

## Overview

The `ExcelParquetPipeline` class in `main.py` is responsible for reading Excel files, cleaning and flattening the data, and converting it to Parquet format. The pipeline tracks the processing state to avoid reprocessing unchanged files.

## Files

- **main.py**: Contains the implementation of the Excel to Parquet pipeline.
- **requirements.txt**: Lists the Python dependencies required for the project.
- **Dockerfile**: Instructions to build a Docker image for the project.
- **README.md**: Documentation for the project.

## Usage

1. **Install Dependencies**: Ensure you have Python 3 and pip installed. Install the required dependencies using:

   ```
   pip install -r requirements.txt
   ```

2. **Run the Pipeline**: You can run the pipeline using the command:

   ```
   python main.py --input-dir <input_directory> --output-dir <output_directory> --log-level <log_level>
   ```

   Replace `<input_directory>` with the path to the directory containing your Excel files, `<output_directory>` with the path where you want to save the Parquet files, and `<log_level>` with the desired logging level (DEBUG, INFO, WARNING, ERROR).

3. **Clean Orphaned Outputs**: To clean up orphaned Parquet files for Excel files that no longer exist, use the `--clean-orphaned` flag:

   ```
   python main.py --input-dir <input_directory> --output-dir <output_directory> --clean-orphaned
   ```

## Docker

To build and run the Docker container for this project, follow these steps:

1. **Build the Docker Image**:

   ```
   docker build -t excel-to-parquet-pipeline .
   ```

2. **Run the Docker Container**:

   ```
   docker run -v <input_directory>:/input -v <output_directory>:/output excel-to-parquet-pipeline --input-dir /input --output-dir /output
   ```

   Replace `<input_directory>` and `<output_directory>` with the appropriate paths.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.