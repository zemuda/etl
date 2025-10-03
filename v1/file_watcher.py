# file_watcher.py
#!/usr/bin/env python3
"""
File watcher service to trigger pipeline on new Excel files
"""

import os
import time
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import docker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExcelFileHandler(FileSystemEventHandler):
    """Handle Excel file events"""
    
    def __init__(self, pipeline_container):
        self.pipeline_container = pipeline_container
        self.docker_client = docker.from_env()
        self.processing_delay = 30  # Wait 30 seconds before processing
        
    def on_created(self, event):
        if not event.is_directory:
            self.handle_file_event(event.src_path, "created")
    
    def on_modified(self, event):
        if not event.is_directory:
            self.handle_file_event(event.src_path, "modified")
    
    def handle_file_event(self, file_path, event_type):
        """Handle file events for Excel files"""
        path = Path(file_path)
        
        # Check if it's an Excel file
        if path.suffix.lower() in ['.xlsx', '.xls']:
            logger.info(f"Excel file {event_type}: {file_path}")
            
            # Wait a bit to ensure file is completely written
            time.sleep(self.processing_delay)
            
            try:
                # Trigger pipeline processing
                container = self.docker_client.containers.get(self.pipeline_container)
                
                # Execute pipeline command in the container
                result = container.exec_run([
                    "python", "excel_parquet_pipeline.py",
                    "--input-dir", "/etl/data/input",
                    "--output-dir", "/etl/data/output",
                    "--state-file", "/etl/state/processing_state.json",
                    "--log-level", "INFO",
                    "--clean-orphaned"
                ])
                
                if result.exit_code == 0:
                    logger.info(f"Pipeline triggered successfully for {file_path}")
                else:
                    logger.error(f"Pipeline execution failed: {result.output.decode()}")
                    
            except Exception as e:
                logger.error(f"Error triggering pipeline: {e}")

def main():
    watch_dir = os.getenv('WATCH_DIR', '/etl/data/input')
    pipeline_container = os.getenv('PIPELINE_CONTAINER', 'excel-parquet-pipeline')
    
    logger.info(f"Starting file watcher for directory: {watch_dir}")
    logger.info(f"Pipeline container: {pipeline_container}")
    
    # Create watch directory if it doesn't exist
    Path(watch_dir).mkdir(parents=True, exist_ok=True)
    
    # Setup file watcher
    event_handler = ExcelFileHandler(pipeline_container)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=True)
    
    # Start watching
    observer.start()
    logger.info("File watcher started")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher...")
        observer.stop()
    
    observer.join()
    logger.info("File watcher stopped")

if __name__ == "__main__":
    main()

