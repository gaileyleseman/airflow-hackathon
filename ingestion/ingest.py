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
                source_file VARCHAR NOT NULL UNIQUE,
                payload     JSONB NOT NULL
            )
        """)
        cur.execute("""
            ALTER TABLE raw.citizens
            ADD COLUMN IF NOT EXISTS source_file VARCHAR NOT NULL DEFAULT ''
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS citizens_source_file_idx
            ON raw.citizens (source_file)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.logins (
                id          SERIAL PRIMARY KEY,
                ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
                source_file VARCHAR NOT NULL UNIQUE,
                payload     JSONB NOT NULL
            )
        """)
        cur.execute("""
            ALTER TABLE raw.logins
            ADD COLUMN IF NOT EXISTS source_file VARCHAR NOT NULL DEFAULT ''
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS logins_source_file_idx
            ON raw.logins (source_file)
        """)

    conn.commit()



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


def insert_records(conn, table: str, source_file: str, records: list[dict]) -> bool:
    """Insert an entire file's records as a single JSONB row.

    Returns True if the row was inserted, False if it was already present.
    Uses ON CONFLICT DO NOTHING to enforce idempotency at the database level.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} (source_file, payload) VALUES (%s, %s) ON CONFLICT (source_file) DO NOTHING",
            (source_file, json.dumps(records))
        )
        inserted = cur.rowcount == 1
    conn.commit()
    if inserted:
        logger.info(f"Inserted {source_file} as 1 row into {table} ({len(records)} records)")
    else:
        logger.info(f"Skipped {source_file} — already present in {table}")
    return inserted


def ingest_citizens(conn, minio_client, bucket: str, today: str) -> int:
    """Ingest citizens source file. Returns number of records inserted (0 if skipped)."""
    key = f"citizens/{today}.json"
    logger.info(f"Reading {bucket}/{key} from Minio")
    records = read_from_minio(minio_client, bucket, key)
    if records is None:
        return 0
    logger.info(f"Read {len(records)} citizen records")
    return len(records) if insert_records(conn, "raw.citizens", key, records) else 0


def ingest_logins(conn, minio_client, bucket: str, today: str) -> int:
    """Ingest logins source file. Returns number of records inserted (0 if skipped)."""
    key = f"logins/{today}.json"
    logger.info(f"Reading {bucket}/{key} from Minio")
    records = read_from_minio(minio_client, bucket, key)
    if records is None:
        return 0
    logger.info(f"Read {len(records)} login records")
    return len(records) if insert_records(conn, "raw.logins", key, records) else 0


def main():
    """
    Main ingestion function.

    Loads sources from MinIO into PostgreSQL. The INGEST_SOURCE environment
    variable controls which source is loaded:
      - "citizens" — ingest only citizens
      - "logins"   — ingest only logins
      - unset      — ingest both (default, backwards-compatible)

    Idempotency is enforced at the database level via a UNIQUE constraint on
    source_file and ON CONFLICT DO NOTHING.
    """
    bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    today = os.environ.get("PIPELINE_DATE", date.today().isoformat())
    source = os.environ.get("INGEST_SOURCE", "").strip().lower()

    if source not in ("", "citizens", "logins"):
        raise ValueError(f"INGEST_SOURCE must be 'citizens', 'logins', or unset — got: {source!r}")

    minio_client = get_minio_client()
    conn = None

    try:
        conn = get_pg_connection()
        ensure_tables_exist(conn)

        if source in ("", "citizens"):
            n = ingest_citizens(conn, minio_client, bucket, today)
            logger.info(f"Citizens ingestion complete: {n} records")

        if source in ("", "logins"):
            n = ingest_logins(conn, minio_client, bucket, today)
            logger.info(f"Logins ingestion complete: {n} records")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
