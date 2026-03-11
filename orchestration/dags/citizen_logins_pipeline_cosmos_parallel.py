from pathlib import Path
from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

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

_profile_config = ProfileConfig(
    profile_name="pipeline",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

_project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)

with DAG(
    dag_id="citizen_pipeline_cosmos_parallel",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["logins", "citizens", "cosmos", "parallel"],
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

    ingest_citizens = DockerOperator(
        task_id="ingest_citizens",
        image="airflow-hackathon-ingestion",
        environment={
            **_minio_env,
            **_postgres_env,
            "INGEST_SOURCE": "citizens",
            "PIPELINE_DATE": "{{ (data_interval_start or dag_run.run_after) | ds }}",
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
            "PIPELINE_DATE": "{{ (data_interval_start or dag_run.run_after) | ds }}",
        },
        network_mode="hackathon-network",
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
    )

    # Citizens branch: stg_citizens → int_citizens_enriched
    # Starts as soon as citizens are ingested.
    citizens_transform = DbtTaskGroup(
        group_id="citizens_transform",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=True,
            select=["stg_citizens", "int_citizens_enriched"],
        ),
    )

    # Logins branch: stg_logins → int_login_activity
    # Starts as soon as logins are ingested.
    logins_transform = DbtTaskGroup(
        group_id="logins_transform",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=True,
            select=["stg_logins", "int_login_activity"],
        ),
    )

    # mart_logins depends only on the logins branch — completes independently.
    logins_marts = DbtTaskGroup(
        group_id="logins_marts",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=True,
            select=["mart_logins"],
        ),
    )

    # mart_citizens joins int_citizens_enriched + int_login_activity — waits for both branches.
    citizens_marts = DbtTaskGroup(
        group_id="citizens_marts",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=True,
            select=["mart_citizens"],
        ),
    )

    scrape_citizens >> ingest_citizens >> citizens_transform
    scrape_logins   >> ingest_logins   >> logins_transform >> logins_marts

    [citizens_transform, logins_transform] >> citizens_marts
