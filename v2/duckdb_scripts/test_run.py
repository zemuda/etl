from bronze_loader import BronzeLayerLoader
import tempfile
import os


def test_with_sample_data():
    """Test the loader with a temporary database"""

    # Use temporary database for testing
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
        test_db_path = tmp_db.name

    try:
        print("Testing bronze loader with sample data...")

        loader = BronzeLayerLoader(
            db_path=test_db_path,
            base_path=r"C:\etl\data\output",  # Your actual data path
        )

        # Run the load
        results = loader.load_all_features()

        # Print detailed results
        print("\n=== TEST RESULTS ===")
        for result in results:
            print(f"Feature: {result['feature']}")
            print(f"Status: {result['status']}")
            print(f"Files processed: {result.get('files_processed', 0)}")
            print(f"Rows loaded: {result.get('total_rows_loaded', 0)}")
            print("-" * 40)

        # Verify data was loaded
        tables = loader.verify_loaded_data()
        print(f"\nTotal tables created: {len(tables)}")

        return True

    except Exception as e:
        print(f"Test failed: {e}")
        return False

    finally:
        # Clean up
        if os.path.exists(test_db_path):
            os.unlink(test_db_path)


if __name__ == "__main__":
    test_with_sample_data()
