import hashlib
import json
import logging
import os
import random
from datetime import date
from pathlib import Path

import boto3
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MUNICIPALITIES = [
    "Amsterdam", "Rotterdam", "Utrecht", "Den Haag", "Eindhoven",
    "Groningen", "Tilburg", "Almere", "Breda", "Nijmegen"
]

STATUSES = ["active", "inactive", "suspended"]
SERVICE_TIERS = ["basic", "standard", "premium"]


def load_citizen_ids(path: str = "citizen_ids.json") -> list[str]:
    """Load citizen IDs from the seed file."""
    script_dir = Path(__file__).parent
    file_path = script_dir / path
    with open(file_path) as f:
        return json.load(f)


def generate_citizens(ids: list[str], seed: int = 42) -> list[dict]:
    """Generate synthetic citizen records for each ID."""
    fake = Faker("nl_NL")
    Faker.seed(seed)
    random.seed(seed)

    citizens = []
    for citizen_id in ids:
        citizen = {
            "citizen_id": citizen_id,
            "full_name": fake.name(),
            "date_of_birth": fake.date_of_birth(
                minimum_age=20, maximum_age=85
            ).isoformat(),
            "municipality": random.choice(MUNICIPALITIES),
            "registration_date": fake.date_between(
                start_date=date(2000, 1, 1),
                end_date=date(2023, 12, 31)
            ).isoformat(),
            "status": random.choice(STATUSES),
            "service_tier": random.choice(SERVICE_TIERS),
            "email_verified": random.choice([True, False]),
            "national_id_hash": hashlib.sha256(citizen_id.encode()).hexdigest()
        }
        citizens.append(citizen)

    return citizens


def upload_to_minio(client, bucket: str, key: str, data: list[dict]) -> None:
    """Upload JSON data to MinIO."""
    body = json.dumps(data, indent=2, default=str).encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json"
    )


def main():
    if os.environ.get("FAIL_SCRAPE"):
        raise RuntimeError("Scrape intentionally failed (FAIL_SCRAPE is set)")

    # Load environment variables
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    minio_bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    citizens_count = int(os.environ.get("CITIZENS_COUNT", "50"))

    if not minio_endpoint:
        raise ValueError("MINIO_ENDPOINT environment variable is required")
    if not minio_access_key:
        raise ValueError("MINIO_ACCESS_KEY environment variable is required")
    if not minio_secret_key:
        raise ValueError("MINIO_SECRET_KEY environment variable is required")

    # Load citizen IDs
    logger.info("Loading citizen IDs from citizen_ids.json")
    ids = load_citizen_ids()

    if citizens_count != len(ids):
        logger.warning(
            f"CITIZENS_COUNT={citizens_count} does not match citizen_ids.json "
            f"length={len(ids)}. Using all {len(ids)} IDs from file."
        )

    # Generate citizens
    logger.info(f"Generating {len(ids)} citizens")
    citizens = generate_citizens(ids, seed=42)

    # Create MinIO client
    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name="us-east-1"
    )

    # Upload to MinIO
    pipeline_date = os.environ.get("PIPELINE_DATE", date.today().isoformat())
    key = f"citizens/{pipeline_date}.json"
    logger.info(f"Uploading to {minio_bucket}/{key}")
    upload_to_minio(client, minio_bucket, key, citizens)

    logger.info(f"Done. {len(citizens)} citizens uploaded.")


if __name__ == "__main__":
    main()
