from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

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


def _dbt_build(task_id: str, select: str) -> DockerOperator:
    """Run dbt build (run + test) for the given model selection."""
    return DockerOperator(
        task_id=task_id,
        image="airflow-hackathon-transform",
        command=f"bash -c 'cd /app/dbt && dbt deps && dbt build --full-refresh --select {select} --profiles-dir /app/dbt'",
        environment=_postgres_env,
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )


with DAG(
    dag_id="citizen_pipeline_parallel",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["logins", "citizens", "parallel"],
) as dag:
    scrape_citizens = DockerOperator(
        task_id="scrape_citizens",
        image="airflow-hackathon-scraper-citizens",
        environment={
            **_minio_env,
            "PIPELINE_DATE": "{{ dag_run.logical_date | ds }}",
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
            "PIPELINE_DATE": "{{ dag_run.logical_date | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    ingest_citizens = DockerOperator(
        task_id="ingest_citizens",
        image="airflow-hackathon-ingestion",
        environment={
            **_minio_env,
            **_postgres_env,
            "INGEST_SOURCE": "citizens",
            "PIPELINE_DATE": "{{ dag_run.logical_date | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    ingest_logins = DockerOperator(
        task_id="ingest_logins",
        image="airflow-hackathon-ingestion",
        environment={
            **_minio_env,
            **_postgres_env,
            "INGEST_SOURCE": "logins",
            "PIPELINE_DATE": "{{ dag_run.logical_date | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    citizens_transform = _dbt_build("citizens_transform", "stg_citizens int_citizens_enriched")
    logins_transform   = _dbt_build("logins_transform",   "stg_logins int_login_activity")

    # mart_citizens depends only on the citizens branch.
    mart_citizens      = _dbt_build("mart_citizens",      "mart_citizens")

    # mart_logins joins logins with citizen info — waits for both branches.
    mart_logins        = _dbt_build("mart_logins",        "mart_logins")

    export_logins = DockerOperator(
        task_id="export_logins",
        image="airflow-hackathon-transform",
        command="python /app/export.py",
        environment={
            **_minio_env,
            **_postgres_env,
            "EXPORT_TABLE": "logins",
            "PIPELINE_DATE": "{{ dag_run.logical_date | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    scrape_citizens >> ingest_citizens >> citizens_transform >> mart_citizens
    scrape_logins   >> ingest_logins   >> logins_transform

    [citizens_transform, logins_transform] >> mart_logins >> export_logins
