from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset

# ---------------------------------------------------------------------------
# Assets — used for data lineage between tasks
# ---------------------------------------------------------------------------
citizens_raw = Asset("s3://raw/citizens")
logins_raw   = Asset("s3://raw/logins")
ingested     = Asset("postgres://postgres/hackathon/raw/citizens")
transformed  = Asset("postgres://postgres/hackathon/public_marts/mart_logins")

_minio_env = {
    "MINIO_ENDPOINT":   os.environ.get("MINIO_ENDPOINT",   "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "MINIO_BUCKET_RAW": os.environ.get("MINIO_BUCKET_RAW", "raw"),
}

_postgres_env = {
    "POSTGRES_HOST":     os.environ.get("POSTGRES_HOST",     "postgres"),
    "POSTGRES_PORT":     os.environ.get("POSTGRES_PORT",     "5432"),
    "POSTGRES_DB":       os.environ.get("POSTGRES_DB",       "hackathon"),
    "POSTGRES_USER":     os.environ.get("POSTGRES_USER",     "hackathon"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "hackathon"),
}

_pipeline_date = "{{ dag_run.logical_date | ds }}"

_docker_common = dict(
    network_mode="hackathon-network",
    auto_remove="force",
    mount_tmp_dir=False,
    docker_url="unix://var/run/docker.sock",
)

with DAG(
    dag_id="citizen_pipeline_assets",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["assets", "logins", "citizens"],
) as dag:

    scrape_citizens = DockerOperator(
        task_id="scrape_citizens",
        image="airflow-hackathon-scraper-citizens",
        environment={**_minio_env, "PIPELINE_DATE": _pipeline_date},
        outlets=[citizens_raw],
        **_docker_common,
    )

    scrape_logins = DockerOperator(
        task_id="scrape_logins",
        image="airflow-hackathon-scraper-logins",
        environment={**_minio_env, "PIPELINE_DATE": _pipeline_date},
        outlets=[logins_raw],
        **_docker_common,
    )

    ingest = DockerOperator(
        task_id="ingest",
        image="airflow-hackathon-ingestion",
        environment={
            **_minio_env,
            **_postgres_env,
            "PIPELINE_DATE": _pipeline_date,
        },
        inlets=[citizens_raw, logins_raw],
        outlets=[ingested],
        **_docker_common,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="airflow-hackathon-transform",
        command=(
            "bash -c 'cd /app/dbt"
            " && dbt deps"
            " && dbt build --full-refresh --profiles-dir /app/dbt'"
        ),
        environment=_postgres_env,
        inlets=[ingested],
        outlets=[transformed],
        **_docker_common,
    )

    [scrape_citizens, scrape_logins] >> ingest >> dbt_build
