import logging
import threading
from typing import Any

import boto3
from pydantic import ValidationError

from v2x_service.storage import Storage
from common.schemas import V2XEvent

logger = logging.getLogger(__name__)


class SQSWorker:
    def __init__(self, storage: Storage, queue_url: str, region: str, client: Any = None) -> None:
        self.storage = storage
        self.queue_url = queue_url
        self.client = client if client is not None else boto3.client("sqs", region_name=region)
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, name="sqs-worker", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=20)
        if self.thread.is_alive():
            logger.warning("SQS worker did not stop within 20 seconds")

    def process_message(self, message: Any) -> None:
        try:
            event = V2XEvent.model_validate_json(message["Body"])
        except ValidationError:
            logger.exception("Discarding invalid SQS message %s", message.get("MessageId"))
            self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"])
            return
        self.storage.process_event(event)
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"])

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                response = self.client.receive_message(
                    QueueUrl=self.queue_url, WaitTimeSeconds=5, MaxNumberOfMessages=10
                )
                for message in response.get("Messages", []):
                    try:
                        self.process_message(message)
                    except Exception:
                        logger.exception("Failed to process SQS message %s; it will be retried", message.get("MessageId"))
            except Exception:
                logger.exception("SQS polling failed; retrying in 5 seconds")
                self.stop_event.wait(5)
