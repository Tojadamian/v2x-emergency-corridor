from contextlib import asynccontextmanager
from html import escape
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from common.schemas import V2XEvent
from v2x_service.config import Settings
from v2x_service.storage import DynamoDBStorage, InMemoryStorage, Storage
from v2x_service.worker import SQSWorker

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


def create_app(settings: Optional[Settings] = None, storage: Optional[Storage] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if storage is None:
        storage = (
            DynamoDBStorage(settings.dynamodb_table, settings.aws_region)
            if settings.storage_backend == "dynamodb" else InMemoryStorage()
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker = None
        if settings.enable_sqs_worker:
            worker = SQSWorker(storage, settings.sqs_queue_url, settings.aws_region)
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
        return {"processed": processed, "alert": alert}

    return app


app = create_app()
