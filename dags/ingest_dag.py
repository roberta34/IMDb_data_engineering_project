from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from airflow.utils.task_group import TaskGroup

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator


RAW_DIR = Path("/opt/airflow/raw")
DOWNLOAD_DIR = RAW_DIR / "downloads"
DUCKDB_TEMP_DIR = RAW_DIR / "duckdb_tmp"

IMDB_FILES = {
    "title.basics.tsv.gz": "title_basics",
    "title.ratings.tsv.gz": "title_ratings",
    "title.crew.tsv.gz": "title_crew",
    "title.principals.tsv.gz": "title_principals",
    "name.basics.tsv.gz": "name_basics",
    "title.akas.tsv.gz": "title_akas",
    "title.episode.tsv.gz": "title_episode",
}

WAREHOUSE_PATH = Path("/opt/airflow/warehouse/warehouse.duckdb")


def prepare_directories() -> None:
    """Create the directories required by the ingestion pipeline."""

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Raw directory ready: {RAW_DIR}")
    print(f"Downloads directory ready: {DOWNLOAD_DIR}")
    print(f"DuckDB temp directory ready: {DUCKDB_TEMP_DIR}")
    print(f"Warehouse directory ready: {WAREHOUSE_PATH.parent}")


def validate_imdb_sources() -> None:
    """Validate all locally downloaded IMDb source files."""

    print(f"Validating IMDb source files from: {DOWNLOAD_DIR}")

    for source_file in IMDB_FILES:
        source_path = DOWNLOAD_DIR / source_file

        if not source_path.exists():
            raise FileNotFoundError(
                f"IMDb source file not found: {source_path}"
            )

        file_size = source_path.stat().st_size

        if file_size == 0:
            raise ValueError(
                f"IMDb source file is empty: {source_path}"
            )

        print(
            f"Source validated: {source_file} "
            f"({file_size:,} bytes)"
        )

    print(
        f"IMDb source validation succeeded for "
        f"{len(IMDB_FILES)} files."
    )


def convert_imdb_sources_to_parquet() -> None:
    """Convert all compressed IMDb TSV files to Parquet format."""

    for source_file, output_name in IMDB_FILES.items():
        source_path = DOWNLOAD_DIR / source_file
        parquet_path = RAW_DIR / f"{output_name}.parquet"
        temp_parquet_path = RAW_DIR / f"{output_name}.tmp.parquet"

        print("=" * 60)
        print(f"Processing source: {source_file}")

        if not source_path.exists():
            raise FileNotFoundError(
                f"Source file not found: {source_path}"
            )

        if temp_parquet_path.exists():
            temp_parquet_path.unlink()

            print(
                f"Removed previous temporary file: "
                f"{temp_parquet_path}"
            )

        connection = duckdb.connect()

        try:
            connection.execute("SET memory_limit = '1GB'")
            connection.execute("SET threads = 1")

            connection.execute(
                f"""
                SET temp_directory = '{DUCKDB_TEMP_DIR}'
                """
            )

            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_csv(
                        '{source_path}',
                        delim = '\t',
                        header = true,
                        nullstr = '\\N',
                        quote = '',
                        sample_size = 100000
                    )
                )
                TO '{temp_parquet_path}'
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD
                )
                """
            )

            if not temp_parquet_path.exists():
                raise FileNotFoundError(
                    f"Temporary Parquet file was not created: "
                    f"{temp_parquet_path}"
                )

            temporary_file_size = temp_parquet_path.stat().st_size

            if temporary_file_size == 0:
                raise ValueError(
                    f"Temporary Parquet file is empty: "
                    f"{temp_parquet_path}"
                )

            row_count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{temp_parquet_path}')
                """
            ).fetchone()[0]

            if row_count == 0:
                raise ValueError(
                    f"Temporary Parquet file contains no rows: "
                    f"{temp_parquet_path}"
                )

        finally:
            connection.close()

        temp_parquet_path.replace(parquet_path)

        print(f"Conversion succeeded: {source_file}")
        print(f"Rows processed: {row_count:,}")
        print(f"Output path: {parquet_path}")
        print(
            f"Output size: "
            f"{parquet_path.stat().st_size:,} bytes"
        )

    print(
        f"Successfully converted {len(IMDB_FILES)} "
        f"IMDb source files to Parquet."
    )


def validate_imdb_parquet_files() -> None:
    """Validate all generated IMDb Parquet files."""

    for _, output_name in IMDB_FILES.items():
        parquet_path = RAW_DIR / f"{output_name}.parquet"

        print("=" * 60)
        print(f"Validating Parquet file: {parquet_path}")

        if not parquet_path.exists():
            raise FileNotFoundError(
                f"Parquet file not found: {parquet_path}"
            )

        file_size = parquet_path.stat().st_size

        if file_size == 0:
            raise ValueError(
                f"Parquet file is empty: {parquet_path}"
            )

        connection = duckdb.connect()

        try:
            connection.execute("SET memory_limit = '1GB'")
            connection.execute("SET threads = 1")

            row_count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM read_parquet('{parquet_path}')
                """
            ).fetchone()[0]

            if row_count == 0:
                raise ValueError(
                    f"Parquet file contains no rows: {parquet_path}"
                )

            columns = connection.execute(
                f"""
                DESCRIBE
                SELECT *
                FROM read_parquet('{parquet_path}')
                """
            ).fetchall()

            if len(columns) == 0:
                raise ValueError(
                    f"Parquet file contains no columns: "
                    f"{parquet_path}"
                )

            column_names = [column[0] for column in columns]

        finally:
            connection.close()

        print(f"Validation succeeded: {output_name}")
        print(f"Rows: {row_count:,}")
        print(f"Columns: {len(column_names)}")
        print(f"Column names: {column_names}")
        print(f"File size: {file_size:,} bytes")

    print(
        f"Successfully validated {len(IMDB_FILES)} "
        f"Parquet files."
    )


def register_imdb_sources_in_duckdb() -> None:
    """Expose all IMDb Parquet files as DuckDB views."""

    connection = duckdb.connect(str(WAREHOUSE_PATH))

    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS raw")

        for _, output_name in IMDB_FILES.items():
            parquet_path = RAW_DIR / f"{output_name}.parquet"

            if not parquet_path.exists():
                raise FileNotFoundError(
                    f"Parquet file not found: {parquet_path}"
                )

            print("=" * 60)
            print(f"Registering DuckDB view: raw.{output_name}")

            connection.execute(
                f"""
                CREATE OR REPLACE VIEW raw.{output_name} AS
                SELECT *
                FROM read_parquet('{parquet_path}')
                """
            )

            row_count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM raw.{output_name}
                """
            ).fetchone()[0]

            print(
                f"DuckDB view raw.{output_name} "
                f"created successfully"
            )
            print(
                f"Rows available in DuckDB: "
                f"{row_count:,}"
            )

    finally:
        connection.close()

    print(
        f"Successfully registered {len(IMDB_FILES)} "
        f"IMDb sources in DuckDB."
    )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="imdb_ingest_pipeline",
    description=(
        "IMDb local ingestion pipeline "
        "for all configured sources"
    ),
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["imdb", "ingestion"],
) as dag:

    prepare_directories_task = PythonOperator(
        task_id="prepare_directories",
        python_callable=prepare_directories,
    )

    with TaskGroup(group_id="extract") as extract_group:
        validate_source_task = PythonOperator(
            task_id="validate_imdb_sources",
            python_callable=validate_imdb_sources,
        )

    with TaskGroup(group_id="convert") as convert_group:
        convert_to_parquet_task = PythonOperator(
            task_id="convert_imdb_sources_to_parquet",
            python_callable=convert_imdb_sources_to_parquet,
        )

    with TaskGroup(group_id="validate") as validate_group:
        validate_parquet_task = PythonOperator(
            task_id="validate_imdb_parquet_files",
            python_callable=validate_imdb_parquet_files,
        )
    with TaskGroup(group_id="load") as load_group:
        register_duckdb_sources_task = PythonOperator(
            task_id="register_imdb_sources_in_duckdb",
            python_callable=register_imdb_sources_in_duckdb,
        )

    (
        prepare_directories_task
        >> extract_group
        >> convert_group
        >> validate_group
        >> load_group
    )