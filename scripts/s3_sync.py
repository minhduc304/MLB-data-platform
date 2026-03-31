"""S3 helpers for downloading and uploading the SQLite database."""

import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


def download_db(bucket: str, key: str, local_path: str) -> bool:
    """
    Download the database from S3.

    Returns True if downloaded, False if the object does not exist yet
    (first-ever run). Raises on any other error.
    """
    s3 = boto3.client("s3")
    try:
        s3.download_file(bucket, key, local_path)
        size_mb = os.path.getsize(local_path) / 1024 / 1024
        logger.info(f"Downloaded s3://{bucket}/{key} ({size_mb:.1f} MB)")
        return True
    except NoCredentialsError:
        logger.error("No AWS credentials found. In ECS, ensure the task IAM role is configured.")
        raise
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info(f"No database found at s3://{bucket}/{key} — starting fresh")
            return False
        raise


def upload_db(bucket: str, key: str, local_path: str) -> None:
    """Upload the database to S3."""
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    size_mb = os.path.getsize(local_path) / 1024 / 1024
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{key} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    """
    Entrypoint for use from shell script:
        python scripts/s3_sync.py download <bucket> <key> <local_path>
        python scripts/s3_sync.py upload   <bucket> <key> <local_path>
    Exits 0 on success, 2 if download found no object (first run).
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    action, bucket, key, local_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

    if action == "download":
        found = download_db(bucket, key, local_path)
        sys.exit(0 if found else 2)
    elif action == "upload":
        upload_db(bucket, key, local_path)
    else:
        print(f"Unknown action: {action}", file=sys.stderr)
        sys.exit(1)
