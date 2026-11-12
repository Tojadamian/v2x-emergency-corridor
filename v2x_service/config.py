import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class Settings:
    aws_region: str = "us-east-1"
    sqs_queue_url: str = ""
    dynamodb_table: str = "v2x-events-store"
    s3_static_base_url: str = ""
    storage_backend: str = "memory"
    enable_sqs_worker: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        backend = os.getenv("STORAGE_BACKEND", "memory").lower()
        if backend not in ("memory", "dynamodb"):
            raise ValueError("STORAGE_BACKEND must be memory or dynamodb")
        worker = os.getenv("ENABLE_SQS_WORKER", "false").lower()
        if worker not in ("true", "false"):
            raise ValueError("ENABLE_SQS_WORKER must be true or false")
        url = os.getenv("S3_STATIC_BASE_URL", "").rstrip("/")
        if url:
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.query or parsed.fragment:
                raise ValueError("S3_STATIC_BASE_URL must be an HTTP(S) base URL")
        queue_url = os.getenv("SQS_QUEUE_URL", "")
        if worker == "true" and not queue_url:
            raise ValueError("SQS_QUEUE_URL is required when the SQS worker is enabled")
        return cls(
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            sqs_queue_url=queue_url,
            dynamodb_table=os.getenv("DYNAMODB_TABLE", "v2x-events-store"),
            s3_static_base_url=url,
            storage_backend=backend,
            enable_sqs_worker=worker == "true",
        )
