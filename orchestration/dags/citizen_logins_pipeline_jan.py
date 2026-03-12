from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import os

_minio_env = {
    "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "MINIO_BUCKET_RAW": os.environ.get("MINIO_BUCKET_RAW", "raw"),
}

_postgres_env = {
    "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "hackathon"),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "hackathon"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "hackathon"),
}

with DAG(
    dag_id="citizen_pipeline_jan",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["logins", "citizens"],
) as dag:
    scrape_citizens = DockerOperator(
        task_id="scrape_citizens",
        image="airflow-hackathon-scraper-citizens",
        environment={
            **_minio_env,
            "PIPELINE_DATE": "{{ (data_interval_start or dag_run.run_after) | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    scrape_logins = DockerOperator(
        task_id="scrape_logins",
        image="airflow-hackathon-scraper-logins",
        environment={
            **_minio_env,
            "PIPELINE_DATE": "{{ (data_interval_start or dag_run.run_after) | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    ingest = DockerOperator(
        task_id="ingest",
        image="airflow-hackathon-ingestion",
        environment={
            **_minio_env,
            **_postgres_env,
            "PIPELINE_DATE": "{{ (data_interval_start or dag_run.run_after) | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    dbt_transform = DockerOperator(
        task_id="dbt_transform",
        image="airflow-hackathon-transform",
        environment=_postgres_env,
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    scrape_citizens >> scrape_logins >> ingest >> dbt_transform
