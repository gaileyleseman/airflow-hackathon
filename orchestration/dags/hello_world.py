import random
import time
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def hello_scraper_fast():
    print("Hello from the fast scraper task!")
    print("Simulating quick data fetch from source...")
    time.sleep(random.uniform(0.1, 0.5))


def hello_scraper_slow():
    print("Hello from the slow scraper task!")
    print("Simulating slow data fetch from source...")
    time.sleep(5)


def hello_ingestion():
    print("Hello from the ingestion task!")
    print("Simulating data ingestion into raw storage...")
    time.sleep(0.2)
    if random.random() < 0.5:
        raise ValueError("Ingestion failed: simulated 50% flaky upstream write error")
    print("Ingestion succeeded!")


def hello_transform():
    print("Hello from the transform task!")
    print("Simulating data transformation and enrichment...")
    time.sleep(random.uniform(0.5, 1))


with DAG(
    dag_id="hello_world",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["debug"],
) as dag:
    task_scraper_fast = PythonOperator(
        task_id="hello_scraper_fast",
        python_callable=hello_scraper_fast,
    )

    task_scraper_slow = PythonOperator(
        task_id="hello_scraper_slow",
        python_callable=hello_scraper_slow,
    )

    task_ingest = PythonOperator(
        task_id="hello_ingestion",
        python_callable=hello_ingestion,
        retries=2,
        retry_delay=0.2,
    )

    task_transform = PythonOperator(
        task_id="hello_transform",
        python_callable=hello_transform,
    )

    [task_scraper_fast, task_scraper_slow] >> task_ingest >> task_transform
