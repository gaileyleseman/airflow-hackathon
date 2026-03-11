import json
import logging
import os
import random
import uuid
from datetime import date, datetime, timedelta
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

DEVICE_TYPES = ["desktop", "mobile", "tablet", "unknown"]
FAILURE_REASONS = ["wrong_password", "account_locked", "session_expired"]


def load_citizen_ids(path: str = "citizen_ids.json") -> list[str]:
    """Load citizen IDs from the seed file."""
    script_dir = Path(__file__).parent
    file_path = script_dir / path
    with open(file_path) as f:
        return json.load(f)


def generate_logins(ids: list[str], pipeline_date: date) -> list[dict]:
    """Generate synthetic login records for the 24-hour window before pipeline_date."""
    seed = int(pipeline_date.strftime("%Y%m%d"))
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    n = random.randint(20, 200)

    window_end = datetime.combine(pipeline_date, datetime.min.time())
    window_start = window_end - timedelta(hours=24)

    logins = []
    for _ in range(n):
        # Sample citizen_id with replacement
        citizen_id = random.choice(ids)

        logged_in_at = fake.date_time_between(
            start_date=window_start,
            end_date=window_end
        )

        # 20% chance of null logged_out_at (tab close)
        has_logout = random.random() > 0.20

        if has_logout:
            session_seconds = random.randint(60, 3600)
            logged_out_at = logged_in_at + timedelta(seconds=session_seconds)
            session_duration_seconds = session_seconds
        else:
            logged_out_at = None
            session_duration_seconds = None

        # 85% success rate
        success = random.random() < 0.85

        if success:
            failure_reason = None
        else:
            failure_reason = random.choice(FAILURE_REASONS)

        login = {
            "login_id": str(uuid.uuid4()),
            "citizen_id": citizen_id,
            "logged_in_at": logged_in_at.isoformat(),
            "logged_out_at": logged_out_at.isoformat() if logged_out_at else None,
            "session_duration_seconds": session_duration_seconds,
            "device_type": random.choice(DEVICE_TYPES),
            "success": success,
            "failure_reason": failure_reason,
            "ip_region": random.choice(MUNICIPALITIES)
        }
        logins.append(login)

    return logins


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
    pipeline_date = date.fromisoformat(os.environ.get("PIPELINE_DATE", date.today().isoformat()))

    if not minio_endpoint:
        raise ValueError("MINIO_ENDPOINT environment variable is required")
    if not minio_access_key:
        raise ValueError("MINIO_ACCESS_KEY environment variable is required")
    if not minio_secret_key:
        raise ValueError("MINIO_SECRET_KEY environment variable is required")

    # Load citizen IDs
    logger.info("Loading citizen IDs from citizen_ids.json")
    ids = load_citizen_ids()

    # Generate logins for the 24h window before pipeline_date
    logins = generate_logins(ids, pipeline_date)
    logger.info(f"Generated {len(logins)} logins for {pipeline_date}")

    # Create MinIO client
    client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name="us-east-1"
    )

    # Upload to MinIO
    key = f"logins/{pipeline_date.isoformat()}.json"
    logger.info(f"Uploading to {minio_bucket}/{key}")
    upload_to_minio(client, minio_bucket, key, logins)

    logger.info(f"Done. {len(logins)} logins uploaded.")


if __name__ == "__main__":
    main()
