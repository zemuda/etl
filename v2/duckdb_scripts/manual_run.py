from bronze_loader import BronzeLayerLoader


def main():
    # Initialize loader with your paths
    loader = BronzeLayerLoader(
        db_path="C:/etl/duckdb/bronze_layer.db", base_path=r"C:\etl\data\output"
    )

    # Load all features
    print("Starting incremental load...")
    results = loader.load_all_features()

    # Verify results
    print("\nVerifying loaded data...")
    loader.verify_loaded_data()

    # Show summary
    summary = loader.get_load_summary()
    print(f"\nLoad operations completed: {len(summary)}")

    return results


if __name__ == "__main__":
    main()
