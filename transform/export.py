"""
Export script for writing mart tables to Minio as parquet files.

Reads mart_citizens and mart_logins from PostgreSQL and writes each
as a parquet file to the exports bucket in Minio.
"""

import io
import logging
import os
from datetime import date

import boto3
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password
    )


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

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1"
    )


def ensure_bucket_exists(client, bucket: str):
    """Create bucket if it doesn't exist."""
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)
        logger.info(f"Created bucket: {bucket}")


def export_table(conn, client, table: str, bucket: str, key: str) -> int:
    """
    Export a mart table to Minio as parquet.

    Returns the number of rows exported.
    """
    logger.info(f"Exporting {table} to Minio")

    # Read table into DataFrame
    # Note: dbt creates schemas with profile schema prefix, so marts -> public_marts
    df = pd.read_sql(f"SELECT * FROM public_marts.{table}", conn)

    # Write to parquet buffer
    buffer = io.BytesIO()
    pa_table = pa.Table.from_pandas(df)
    pq.write_table(pa_table, buffer)
    buffer.seek(0)

    # Upload to Minio
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info(f"Exported {len(df)} rows to {bucket}/{key}")
    return len(df)


def main():
    """Main export function.

    EXPORT_TABLE controls which mart is exported:
      - "logins"   — export only mart_logins
      - "citizens" — export only mart_citizens
      - unset      — export both (default)
    """
    bucket = os.environ.get("MINIO_BUCKET_EXPORTS", "exports")
    today = os.environ.get("PIPELINE_DATE", date.today().isoformat())
    export_table_env = os.environ.get("EXPORT_TABLE", "").strip().lower()

    if export_table_env not in ("", "logins", "citizens"):
        raise ValueError(f"EXPORT_TABLE must be 'logins', 'citizens', or unset — got: {export_table_env!r}")

    conn = None
    try:
        conn = get_pg_connection()
        client = get_minio_client()

        ensure_bucket_exists(client, bucket)

        if export_table_env in ("", "citizens"):
            citizens_key = f"marts/{today}/mart_citizens.parquet"
            citizens_count = export_table(conn, client, "mart_citizens", bucket, citizens_key)
            logger.info(f"Citizens export complete: {citizens_count} rows")

        if export_table_env in ("", "logins"):
            logins_key = f"marts/{today}/mart_logins.parquet"
            logins_count = export_table(conn, client, "mart_logins", bucket, logins_key)
            logger.info(f"Logins export complete: {logins_count} rows")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
