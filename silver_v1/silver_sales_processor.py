# silver_sales_processor.py
from silver_sales_transformer import SilverSalesTransformer
import logging
import sys


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("C:/etl/logs/silver_sales_layer.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Silver Sales Layer processing")

        transformer = SilverSalesTransformer(db_path="C:/etl/duckdb/bronze_layer.db")
        success = transformer.transform_all()

        if success:
            transformer.get_silver_tables_info()
            logger.info("Silver Sales Layer processing completed successfully")
        else:
            logger.error("Silver Sales Layer processing failed")

        return success

    except Exception as e:
        logger.error(f"Silver Sales Layer processing failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
