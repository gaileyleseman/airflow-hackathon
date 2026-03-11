"""
Ingestion script for loading raw JSON data from MinIO into PostgreSQL.

Idempotency is achieved by tracking the source file key in each row.
If a file key is already present in the table, that file is skipped.
This means data accumulates across days and re-running for the same day
is safe — no duplicates will be inserted.

The payload column stores the full JSON object exactly as received from MinIO.
No transformation occurs here — unpacking into typed columns happens in dbt
staging models.
"""

import json
import logging
import os
from datetime import date

import boto3
import psycopg2
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_minio_client():
    """Create and return a boto3 S3 client configured for MinIO."""
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not endpoint:
        raise ValueError("MINIO_ENDPOINT environment variable is required")
    if not access_key:
        raise ValueError("MINIO_ACCESS_KEY environment variable is required")
    if not secret_key:
        raise ValueError("MINIO_SECRET_KEY environment variable is required")

    logger.info(f"Connecting to Minio at {endpoint}")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1"
    )


def get_pg_connection():
    """Create and return a psycopg2 connection to PostgreSQL."""
    host = os.environ.get("POSTGRES_HOST")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    db = os.environ.get("POSTGRES_DB")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")

    if not host:
        raise ValueError("POSTGRES_HOST environment variable is required")
    if not db:
        raise ValueError("POSTGRES_DB environment variable is required")
    if not user:
        raise ValueError("POSTGRES_USER environment variable is required")
    if not password:
        raise ValueError("POSTGRES_PASSWORD environment variable is required")

    logger.info(f"Connecting to Postgres at {host}:{port}/{db}")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password
    )


def ensure_tables_exist(conn):
    """Create raw tables if they don't exist, and add source_file if missing."""
    logger.info("Ensuring tables exist")

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.citizens (
                id          SERIAL PRIMARY KEY,
                ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
                source_file VARCHAR NOT NULL,
                payload     JSONB NOT NULL
            )
        """)
        cur.execute("""
            ALTER TABLE raw.citizens
            ADD COLUMN IF NOT EXISTS source_file VARCHAR NOT NULL DEFAULT ''
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.logins (
                id          SERIAL PRIMARY KEY,
                ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
                source_file VARCHAR NOT NULL,
                payload     JSONB NOT NULL
            )
        """)
        cur.execute("""
            ALTER TABLE raw.logins
            ADD COLUMN IF NOT EXISTS source_file VARCHAR NOT NULL DEFAULT ''
        """)

    conn.commit()


def is_already_loaded(conn, table: str, source_file: str) -> bool:
    """Return True if this source file has already been loaded into the table."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {table} WHERE source_file = %s LIMIT 1",
            (source_file,)
        )
        return cur.fetchone() is not None


def read_from_minio(client, bucket: str, key: str) -> list[dict] | None:
    """
    Read a JSON file from MinIO and return parsed records.

    Returns None if the file does not exist.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.warning(f"File not found in Minio: {key} — skipping this source")
            return None
        raise


def insert_records(conn, table: str, source_file: str, records: list[dict]):
    """Insert an entire file's records as a single JSONB row."""
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} (source_file, payload) VALUES (%s, %s)",
            (source_file, json.dumps(records))
        )
    conn.commit()
    logger.info(f"Inserted {source_file} as 1 row into {table} ({len(records)} records)")


def main():
    """
    Main ingestion function.

    Loads all sources (citizens, logins) from MinIO into PostgreSQL.
    Skips any source file that has already been loaded (idempotent).

    NOTE: This script loads all sources in a single run because in the legacy
    setup ingestion is one CronJob that loads all sources. This is one of the
    things that will be refactored in the Airflow hackathon — split into
    per-source tasks for better observability and failure isolation.
    """
    bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    today = os.environ.get("PIPELINE_DATE", date.today().isoformat())

    minio_client = get_minio_client()
    conn = None

    try:
        conn = get_pg_connection()
        ensure_tables_exist(conn)

        citizens_count = 0
        logins_count = 0

        # Ingest citizens
        citizens_key = f"citizens/{today}.json"
        if is_already_loaded(conn, "raw.citizens", citizens_key):
            logger.info(f"Already loaded {citizens_key} — skipping")
        else:
            logger.info(f"Reading {bucket}/{citizens_key} from Minio")
            citizens = read_from_minio(minio_client, bucket, citizens_key)
            if citizens is not None:
                logger.info(f"Read {len(citizens)} citizen records")
                insert_records(conn, "raw.citizens", citizens_key, citizens)
                citizens_count = len(citizens)

        # Ingest logins
        logins_key = f"logins/{today}.json"
        if is_already_loaded(conn, "raw.logins", logins_key):
            logger.info(f"Already loaded {logins_key} — skipping")
        else:
            logger.info(f"Reading {bucket}/{logins_key} from Minio")
            logins = read_from_minio(minio_client, bucket, logins_key)
            if logins is not None:
                logger.info(f"Read {len(logins)} login records")
                insert_records(conn, "raw.logins", logins_key, logins)
                logins_count = len(logins)

        logger.info(f"Ingestion complete. Citizens: {citizens_count}, Logins: {logins_count}")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
