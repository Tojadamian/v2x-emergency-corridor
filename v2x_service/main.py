from contextlib import asynccontextmanager
from html import escape
import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

import boto3
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from common.schemas import V2XEvent
from v2x_service.config import Settings
from v2x_service.storage import DynamoDBStorage, InMemoryStorage, Storage
from v2x_service.worker import SQSWorker
from common.schemas import V2XAlert
from botocore.exceptions import BotoCoreError, ClientError

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
logger = logging.getLogger(__name__)


def create_app(settings: Optional[Settings] = None, storage: Optional[Storage] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if storage is None:
        storage = (
            DynamoDBStorage(settings.dynamodb_table, settings.aws_region)
            if settings.storage_backend == "dynamodb" else InMemoryStorage()
        )
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN", "").strip()
    rsu_alert_urls = [
        value.strip().rstrip("/")
        for value in os.getenv("RSU_ALERT_URLS", "").split(",")
        if value.strip()
    ]
    sns_client = None

    def publish_alert(alert: V2XAlert) -> None:
        nonlocal sns_client
        message = alert.model_dump_json()
        if sns_topic_arn:
            try:
                if sns_client is None:
                    sns_client = boto3.client("sns", region_name=settings.aws_region)
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Message=message,
                    Subject=alert.alert_type,
                )
                storage.record_sns_alert()
            except (BotoCoreError, ClientError):
                logger.exception("Failed to publish alert %s to SNS", alert.alert_id)

        for configured_url in rsu_alert_urls:
            path = urlsplit(configured_url).path.rstrip("/")
            url = (
                configured_url
                if path.endswith(("/sns/alerts", "/api/alerts/relay"))
                else f"{configured_url}/sns/alerts"
            )
            try:
                response = httpx.post(url, content=message, headers={"Content-Type": "application/json"}, timeout=5.0)
                response.raise_for_status()
            except httpx.HTTPError:
                logger.exception("Failed to relay alert %s to RSU endpoint %s", alert.alert_id, url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker = None
        if settings.enable_sqs_worker:
            worker = SQSWorker(
                storage,
                settings.sqs_queue_url,
                settings.aws_region,
                on_alert=publish_alert,
            )
            worker.start()
        try:
            yield
        finally:
            if worker:
                worker.stop()

    app = FastAPI(title="V2X Emergency Corridor", lifespan=lifespan)
    app.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")

    @app.get("/", response_class=HTMLResponse)
    def dashboard():
        html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        base = escape(settings.s3_static_base_url or "/ui", quote=True)
        return html.replace('href="/ui/styles.css"', f'href="{base}/styles.css"').replace(
            'src="/ui/app.js"', f'src="{base}/app.js"'
        )

    @app.get("/api/summary")
    def summary():
        return storage.summary()

    @app.get("/api/state")
    def state():
        result = storage.summary()
        return {"vehicles": result["vehicles"], "active_emergency_corridors": result["active_emergency_corridors"]}

    @app.get("/api/events")
    def events():
        return storage.summary()["recent_events"]

    @app.get("/api/alerts")
    def alerts():
        return storage.summary()["recent_alerts"]

    @app.post("/api/events/ingest")
    def ingest(event: V2XEvent):
        processed, alert = storage.process_event(event)
        if processed and alert:
            publish_alert(alert)
        return {"processed": processed, "alert": alert}

    @app.post("/api/control/reset")
    def reset():
        storage.reset()
        return {"status": "reset"}

    return app


app = create_app()
