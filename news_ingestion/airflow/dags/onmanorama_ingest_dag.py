from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "amjad",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "onmanorama_ingest_dag",
    default_args=default_args,
    # schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
) as dag:

    crawl_task = BashOperator(
        task_id="crawl_onmanorama",
        bash_command="cd /app/scrapy_project && scrapy crawl onmanorama",
    )

    crawl_task
