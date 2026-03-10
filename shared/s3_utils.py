"""
S3 helpers for reading and writing agent artifacts.

Design principle: every agent reads its input from S3 and writes its output
to S3. This gives us:
  - A complete audit trail for every workflow run
  - The ability to replay any step in isolation (feed it the S3 artifact directly)
  - Natural decoupling — agents never call each other; Step Functions wires them

All artifact keys follow the pattern:
  {prefix}/{run_id}/{filename}.json

run_id ties all artifacts in a single workflow execution together and maps
directly to the Step Functions execution ID.
"""
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from shared.aws_client import s3 as get_s3
from shared.logger import get_logger
from config import SENTINEL_BUCKET

log = get_logger("s3_utils")


def generate_run_id() -> str:
    """
    Produce a sortable, unique run identifier.

    Format: YYYYMMDD-HHMMSS-<8-char uuid>
    Sortable by time so S3 listings are naturally chronological.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{ts}-{uuid.uuid4().hex[:8]}"


def write_artifact(
    prefix: str,
    run_id: str,
    filename: str,
    data: dict[str, Any],
    bucket: str = SENTINEL_BUCKET,
) -> str:
    """
    Serialise a dict to JSON and upload to S3.

    Returns the full S3 key so callers can pass it downstream.

    Engineering note: we embed metadata (run_id, timestamp, agent name)
    directly in the JSON envelope. This means the artifact is self-describing
    — you can understand it without querying Step Functions or CloudWatch.
    """
    key = f"{prefix}/{run_id}/{filename}.json"
    payload = json.dumps(data, indent=2, default=str)

    get_s3().put_object(
        Bucket=bucket,
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    log.info("artifact_written", bucket=bucket, key=key, size_bytes=len(payload))
    return key


def read_artifact(
    key: str,
    bucket: str = SENTINEL_BUCKET,
) -> dict[str, Any]:
    """Download and deserialise a JSON artifact from S3."""
    response = get_s3().get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    log.info("artifact_read", bucket=bucket, key=key)
    return data


def artifact_exists(key: str, bucket: str = SENTINEL_BUCKET) -> bool:
    """Check if an artifact key exists without downloading it."""
    try:
        get_s3().head_object(Bucket=bucket, Key=key)
        return True
    except get_s3().exceptions.ClientError:
        return False


def list_artifacts(prefix: str, bucket: str = SENTINEL_BUCKET) -> list[str]:
    """List all artifact keys under a given prefix."""
    paginator = get_s3().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
