# silver_sales_processor_enhanced.py
from silver_sales_transformer_transactional import SilverSalesTransformerTransactional
import logging
import sys
import os


def setup_logging():
    """Setup logging with proper encoding handling for Windows"""
    # Check if we're running in Windows console with limited encoding
    if (
        os.name == "nt"
        and hasattr(sys.stdout, "encoding")
        and sys.stdout.encoding == "cp1252"
    ):
        # Use simple characters for Windows console
        log_format = "%(asctime)s - %(levelname)s - %(message)s"
    else:
        # Use emojis for other environments
        log_format = "%(asctime)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(
                "C:/etl/logs/silver_sales_layer_enhanced.log", encoding="utf-8"
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        logger.info(
            "Starting Enhanced Silver Sales Layer processing with comprehensive data cleaning"
        )

        # Initialize the transactional transformer
        transformer = SilverSalesTransformerTransactional(
            db_path="C:/etl/duckdb/bronze_layer.db"
        )

        # First, ensure the schema is updated
        logger.info("Updating database schema...")
        transformer.update_schema()

        # Run the transactional cleaning pipeline
        success = transformer.run_transactional_cleaning()

        if success:
            logger.info("Enhanced Silver Sales Layer processing completed successfully")
        else:
            logger.error("Enhanced Silver Sales Layer processing failed")

        # Close the connection
        transformer.close()
        return success

    except Exception as e:
        logger.error(f"Enhanced Silver Sales Layer processing failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
