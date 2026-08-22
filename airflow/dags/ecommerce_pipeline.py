from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from src.ingestion.snowflake_loader import main


with DAG(
    dag_id = 'ecommerce_pipeline',
    start_date = datetime(2026, 8, 21),
    schedule = None,
    catchup = False,
    tags = ["ecommerce", "snowflake", "dbt"],
) as dag:

    ingest_to_snowflake = PythonOperator(
        task_id = 'ingest_to_snowflake',
        python_callable=main,
    )

    dbt_run = BashOperator(
        task_id = 'dbt_run',
        bash_command = (
            "cd /opt/airflow/ecommerce_dbt && "
            "dbt run"
        ),
    )

    dbt_test = BashOperator(
        task_id = "dbt_test",
        bash_command = (
            "cd /opt/airflow/ecommerce_dbt && "
            "dbt test"
        ),
    )

    ingest_to_snowflake >> dbt_run >> dbt_test