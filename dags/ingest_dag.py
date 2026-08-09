from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from dbt.adapters.record.cursor import description

RAW_DIR = Path("/opt/airflow/raw")
DOWNLOAD_DIR = RAW_DIR / "downloads"

TITLE_RATINGS_SOURCE = DOWNLOAD_DIR / "title.ratings.tsv.gz"

TITLE_RATINGS_PARQUET = RAW_DIR / "title.ratings.parquet"
TITLE_RATINGS_TEMP_PARQUET = RAW_DIR / "title.ratings.tmp.parquet"

def prepare_directories() -> None:
    """Create the directories required by the ingestion pipeline."""

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Raw directory ready: {RAW_DIR}")
    print(f"Downloads directory ready: {DOWNLOAD_DIR}")


def validate_title_ratings_source() -> None:
    """Validate the locally downloaded IMDb source file."""

    if not TITLE_RATINGS_SOURCE.exists():
        raise FileNotFoundError(
            f"IMDb source file not found: {TITLE_RATINGS_SOURCE}"
        )
    file_size = TITLE_RATINGS_SOURCE.stat().st_size

    if file_size == 0:
        raise FileNotFoundError(
            f"IMDb source file not found: {TITLE_RATINGS_SOURCE}"
        )

    print("IMDb title ratings souce validation succeeded")
    print(f"Source path: {TITLE_RATINGS_SOURCE}")
    print(f"Source size: {file_size:,} bytes")


def convert_title_ratings_to_parquet() -> None:
    """Convert the compressed IMDb TSV file to Parquet format."""

    if not TITLE_RATINGS_SOURCE.exists():
        raise FileNotFoundError(
            f"Source file not found: {TITLE_RATINGS_SOURCE}"
        )

    print(f"Reading source file: {TITLE_RATINGS_SOURCE}")

    dataframe = pd.read_csv(
        TITLE_RATINGS_SOURCE,
        sep="\t",
        compression="gzip",
        na_values="\\N",
        dtype={
            "tconst": "string",
            "averageRating": "float64",
            "numVotes": "Int64",
        },
    )

    if dataframe.empty:
        raise ValueError("The IMDb title ratings source contains no rows")

    expected_columns = {
        "tconst",
        "averageRating",
        "numVotes",
    }

    missing_columns = expected_columns - set(dataframe.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    print(f"Rows read: {len(dataframe):,}")
    print(f"Columns found: {list(dataframe.columns)}")
    print("Null counts:")
    print(dataframe.isna().sum().to_string())

    dataframe.to_parquet(
        TITLE_RATINGS_TEMP_PARQUET,
        index=False,
        engine="pyarrow",
    )

    if not TITLE_RATINGS_TEMP_PARQUET.exists():
        raise FileNotFoundError(
            f"Temporary Parquet file was not created: "
            f"{TITLE_RATINGS_TEMP_PARQUET}"
        )

    temporary_file_size = TITLE_RATINGS_TEMP_PARQUET.stat().st_size

    if temporary_file_size == 0:
        raise ValueError("Temporary Parquet file is empty")

    TITLE_RATINGS_TEMP_PARQUET.replace(
        TITLE_RATINGS_PARQUET
    )

    print("TSV to Parquet conversion succeeded")
    print(f"Output path: {TITLE_RATINGS_PARQUET}")
    print(
        f"Output size: "
        f"{TITLE_RATINGS_PARQUET.stat().st_size:,} bytes"
    )

def validate_title_ratings_parquet() -> None:
    """Validate the generated title ratings Parquet file."""

    if not TITLE_RATINGS_PARQUET.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {TITLE_RATINGS_PARQUET}"
        )

    dataframe = pd.read_parquet(
        TITLE_RATINGS_PARQUET,
        engine="pyarrow",
    )

    if dataframe.empty:
        raise ValueError("Generated Parquet file contains no rows")

    expected_columns = [
        "tconst",
        "averageRating",
        "numVotes",
    ]

    if list(dataframe.columns) != expected_columns:
        raise ValueError(
            "Unexpected Parquet columns. "
            f"Expected {expected_columns}, "
            f"Received {list(dataframe.columns)}"
        )

    null_title_ids = int(dataframe["tconst"].isna().sum())
    duplicate_title_ids = int(
        dataframe["tconst"].duplicated().sum()
    )

    invalid_ratings = int((
        dataframe["averageRating"].notna()
        & ~dataframe["averageRating"].between(0,10)
    ).sum()
    )

    invalid_vote_counts = int(
        (
            dataframe["numVotes"].notna()
            & (dataframe["numVotes"] < 0)
        ).sum()
    )

    if null_title_ids > 0:
        raise ValueError(
            f"Found {null_title_ids} null title IDs"
        )

    if duplicate_title_ids > 0:
        raise ValueError(
            f"Found {duplicate_title_ids} duplicate title IDs"
        )

    if invalid_ratings > 0:
        raise ValueError(
            f"Found {invalid_ratings} ratings outside range 0-10"
        )

    if invalid_vote_counts > 0:
        raise ValueError(
            f"Found {invalid_vote_counts} negative vote counts"
        )

    print("Parquet validation succeeded")
    print(f"Validated rows: {len(dataframe):,}")
    print(f"Null title IDs: {null_title_ids}")
    print(f"Duplicate title IDs: {duplicate_title_ids}")
    print(f"Invalid ratings: {invalid_ratings}")
    print(f"Invalid vote counts: {invalid_vote_counts}")

default_args = {
    "owner" : "airflow",
    "retries" : 1,
    "retry_delay" : timedelta(minutes=2),
}

with DAG(
    dag_id = "imdb_title_ratings_vertical_slice",
    description = "IMDb title.ratings local ingestion vertical slice",
    start_date = datetime(2026, 8, 1),
    schedule = None,
    catchup = False,
    default_args=default_args,
    tags = ["imdb", "ingestion", "vertical-slice"],
) as dag:

    prepare_directories_task = PythonOperator(
        task_id = "prepare_directories",
        python_callable = prepare_directories,
    )

    validate_source_task = PythonOperator(
        task_id = "validate_title_ratings_source",
        python_callable = validate_title_ratings_source,
    )

    convert_to_parquet_task = PythonOperator(
        task_id = "convert_title_ratings_to_parquet",
        python_callable = convert_title_ratings_to_parquet,
    )

    validate_parquet_task = PythonOperator(
        task_id = "validate_title_ratings_parquet",
        python_callable = validate_title_ratings_parquet,
    )

    (
        prepare_directories_task
        >> validate_source_task
        >> convert_to_parquet_task
        >> validate_parquet_task
    )