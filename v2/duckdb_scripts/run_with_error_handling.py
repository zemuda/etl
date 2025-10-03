from bronze_loader import BronzeLayerLoader
import logging
import sys
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("C:/etl/logs/bronze_loader.log"),
        logging.StreamHandler(sys.stdout),
    ],
)


def run_with_logging():
    try:
        logger = logging.getLogger(__name__)
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
                logger.error(f"Feature {result['feature']} failed: {result['error']}")

        # Log schema changes
        changes = loader.get_schema_changes()
        if changes:
            logger.warning(f"Detected {len(changes)} schema changes")

        logger.info("Bronze layer load completed successfully")
        return True

    except Exception as e:
        logger.error(f"Bronze layer load failed: {e}")
        return False


if __name__ == "__main__":
    success = run_with_logging()
    sys.exit(0 if success else 1)
