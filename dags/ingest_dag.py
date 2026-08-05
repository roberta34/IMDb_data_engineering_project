from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def verify_environment() -> None:
    print("IMDb Airflow environment is running correctly.")


with DAG(
    dag_id="imdb_environment_test",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["imdb", "setup"],
) as dag:
    verify_environment_task = PythonOperator(
        task_id="verify_environment",
        python_callable=verify_environment,
        retries=1,
    )