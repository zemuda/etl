from bronze_loader import run_daily_incremental_load

# Custom configuration
if __name__ == "__main__":
    run_daily_incremental_load(
        db_path="C:/etl/duckdb/bronze_layer.db",  # Custom database path
        base_path=r"C:\etl\data\output",  # Your data directory
    )
