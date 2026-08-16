from pathlib import Path

import duckdb

WAREHOUSE_PATH = Path("/opt/airflow/warehouse/warehouse.duckdb")
RAW_DIR = Path("/opt/airflow/raw")

TABLES = [
    "title_basics",
    "title_ratings",
    "title_crew",
    "title_principals",
    "name_basics",
    "title_akas",
    "title_episode"
]

def register_raw_views() -> None:
    connection = duckdb.connect(str(WAREHOUSE_PATH))

    try:
        for table_name in TABLES:
            parquet_path = RAW_DIR/f"{table_name}.parquet"

            if not parquet_path.exists():
                raise FileNotFoundError(
                    f"Parquet file not found: {parquet_path}"
                )

            connection.execute(
                f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * 
                FROM read_parquet('{parquet_path}')
                """
            )

            row_count = connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            print(f"Table {table_name} has {row_count} rows")
    finally:
        connection.close()

    print("All IMDb raw views registered successfully")

if __name__ == "__main__":
    register_raw_views()