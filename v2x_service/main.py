"""Cloud FastAPI application for event ingestion, alerts, and Digital Twin state.

The service routes direct events and SQS worker output through idempotent storage,
publishes cloud alerts over SNS and HTTP, and exposes dashboard and RSU control
endpoints for the emergency-corridor experiment.
"""

from contextlib import asynccontextmanager
from html import escape
import logging
import os
import random
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlsplit
from datetime import datetime, timezone

import boto3
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from common.schemas import V2XEvent
from v2x_service.config import Settings
from v2x_service.storage import DynamoDBStorage, InMemoryStorage, Storage
from v2x_service.worker import SQSWorker
from common.schemas import V2XAlert
from botocore.exceptions import BotoCoreError, ClientError

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
logger = logging.getLogger(__name__)

# Local RSU state storage for demo mode
_rsu_states: Dict[str, Dict[str, Any]] = {
    "rsu-1": {"cloud_disconnected": False},
    "rsu-2": {"cloud_disconnected": False},
}


def create_app(
    settings: Optional[Settings] = None,
    storage: Optional[Storage] = None,
    deliver_alerts: bool = True,
) -> FastAPI:
    """Create the cloud API with injectable settings and isolated storage.

    Args:
        settings: Runtime configuration; environment settings are loaded if omitted.
        storage: Event store; the configured memory or DynamoDB backend is used if omitted.
        deliver_alerts: Whether accepted cloud alerts are sent to SNS and RSUs.

    Returns:
        A FastAPI application exposing ingestion, Digital Twin, and experiment routes.
    """
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
        """Fan out a cloud alert to the configured SNS topic and RSU HTTP endpoints.

        Args:
            alert: Rule-generated alert for a newly processed event.
        """
        nonlocal sns_client
        if not deliver_alerts:
            return
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

    async def fetch_rsu_status(rsu_url: str, timeout: float = 0.4) -> Optional[Dict[str, Any]]:
        """Fetch RSU counters for the Digital Twin summary, or return ``None``.

        Args:
            rsu_url: Base URL of one roadside-unit service.
            timeout: Maximum duration of the status request.
        """
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(f"{rsu_url}/api/status")
                response.raise_for_status()
                status = response.json()
                if isinstance(status, dict):
                    recovery_time_ms = status.get("last_recovery_time_ms", 0)
                    if isinstance(recovery_time_ms, (int, float)) and recovery_time_ms > 1_000_000_000:
                        status["last_recovery_time_ms"] = 1
                return status
        except (httpx.HTTPError, Exception):
            return None

    def normalize_rsu_status(rsu_id: str, status: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize current and legacy RSU status fields for dashboard consumers."""
        cloud_disconnected = bool(status.get("cloud_disconnected", not status.get("cloud_connected", True)))
        forwarded_events = int(status.get("forwarded_events", status.get("forwarded_to_sqs", 0)))
        local_alerts_count = int(status.get("local_alerts_count", status.get("local_edge_alerts", 0)))
        relayed_cloud_alerts_count = int(
            status.get("relayed_cloud_alerts_count", status.get("relayed_cloud_alerts", 0))
        )
        received_events = int(status.get("received_events", 0))
        filtered_telemetry_count = int(status.get("filtered_telemetry_count", 0))
        return {
            **status,
            "rsu_id": status.get("rsu_id", rsu_id),
            "cloud_disconnected": cloud_disconnected,
            "cloud_connected": not cloud_disconnected,
            "forwarded_events": forwarded_events,
            "forwarded_to_sqs": forwarded_events,
            "local_alerts_count": local_alerts_count,
            "local_edge_alerts": local_alerts_count,
            "relayed_cloud_alerts_count": relayed_cloud_alerts_count,
            "relayed_cloud_alerts": relayed_cloud_alerts_count,
            "received_events": received_events,
            "filtered_telemetry_count": filtered_telemetry_count,
            "bandwidth_saved_pct": (
                filtered_telemetry_count / received_events * 100 if received_events > 0 else 0.0
            ),
            "buffered_count": int(status.get("buffered_count", 0)),
            "rejected_events": int(status.get("rejected_events", 0)),
            "flushed_events_count": int(status.get("flushed_events_count", 0)),
            "last_recovery_time_ms": int(status.get("last_recovery_time_ms", 0)),
        }

    def inject_synthetic_event(event: V2XEvent) -> bool:
        """Apply a demo event to cloud storage and publish any newly raised alert.

        Args:
            event: Synthetic event for a Digital Twin demonstration scenario.

        Returns:
            Whether the event was newly accepted by idempotent storage.
        """
        processed, alert = storage.process_event(event)
        if processed and alert:
            publish_alert(alert)
        return processed

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Start and stop the optional SQS worker with the cloud application."""
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

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon():
        """Return an empty response for browser favicon requests."""
        return Response(status_code=204)

    @app.get("/", response_class=HTMLResponse)
    def dashboard():
        """Serve the dashboard shell with its configured static-asset base URL."""
        html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        base = escape(settings.s3_static_base_url or "/ui", quote=True)
        return html.replace('href="/ui/styles.css"', f'href="{base}/styles.css"').replace(
            'src="/ui/app.js"', f'src="{base}/app.js"'
        )

    @app.get("/api/summary")
    async def summary():
        """Return Digital Twin state, metrics, and live-or-fallback RSU status."""
        result = storage.summary()
        
        # Fetch RSU status with timeout fallback
        rsu_nodes = {}
        for rsu_id, rsu_url in [("rsu-1", settings.rsu_1_url), ("rsu-2", settings.rsu_2_url)]:
            rsu_status = await fetch_rsu_status(rsu_url)
            if rsu_status:
                rsu_nodes[rsu_id] = normalize_rsu_status(rsu_id, rsu_status)
            else:
                # Fallback: local state for demo/test mode
                rsu_nodes[rsu_id] = normalize_rsu_status(rsu_id, {
                    "rsu_id": rsu_id,
                    "cloud_disconnected": _rsu_states.get(rsu_id, {}).get("cloud_disconnected", False),
                })
        
        result["rsu_nodes"] = rsu_nodes
        return result

    @app.get("/api/state")
    def state():
        """Return current vehicle states and active emergency corridors."""
        result = storage.summary()
        return {"vehicles": result["vehicles"], "active_emergency_corridors": result["active_emergency_corridors"]}

    @app.get("/api/events")
    def events():
        """Return the most recent cloud-stored events for the dashboard."""
        return storage.summary()["recent_events"]

    @app.get("/api/alerts")
    def alerts():
        """Return the most recent cloud-generated safety alerts."""
        return storage.summary()["recent_alerts"]

    @app.post("/api/events/ingest")
    def ingest(event: V2XEvent):
        """Ingest one validated cloud event idempotently and publish its alert.

        Args:
            event: Event received directly from a producer or compatible client.

        Returns:
            Whether storage accepted the event and any resulting cloud alert.
        """
        processed, alert = storage.process_event(event)
        if processed and alert:
            publish_alert(alert)
        return {"processed": processed, "alert": alert}

    @app.post("/api/experiments/run")
    def run_experiments():
        """Run the four isolated controlled experiment families and return results."""
        from experiments.run_experiments import run_all_experiments

        return {"status": "ok", "results": run_all_experiments()}

    @app.post("/api/control/reset")
    async def reset():
        """Clear cloud state and ask both RSUs to reset their experiment counters."""
        storage.reset()
        # Attempt to reset RSUs
        for rsu_url in [settings.rsu_1_url, settings.rsu_2_url]:
            try:
                async with httpx.AsyncClient(timeout=0.5) as client:
                    await client.post(f"{rsu_url}/api/control/reset")
            except (httpx.HTTPError, Exception):
                pass
        return {"status": "reset"}

    @app.post("/api/control/rsu-disconnection")
    async def rsu_disconnection(payload: Dict[str, Any]):
        """Toggle one RSU's cloud connection, forwarding to it or using demo state.

        Args:
            payload: Object containing the target ``rsu_id`` and ``disconnected`` flag.

        Returns:
            A forwarding or local-fallback status, or an error for an unknown RSU.
        """
        rsu_id = payload.get("rsu_id", "").lower()
        disconnected = payload.get("disconnected", False)
        
        if rsu_id not in ("rsu-1", "rsu-2"):
            return {"status": "error", "message": "Invalid rsu_id"}
        
        rsu_url = settings.rsu_1_url if rsu_id == "rsu-1" else settings.rsu_2_url
        
        # Try to forward to RSU endpoint
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                response = await client.post(
                    f"{rsu_url}/api/control/disconnection",
                    json={"disconnected": disconnected},
                )
                response.raise_for_status()
                return {"status": "forwarded", "rsu_id": rsu_id}
        except (httpx.HTTPError, Exception):
            # Fallback: store locally for demo/test mode
            if rsu_id in _rsu_states:
                _rsu_states[rsu_id]["cloud_disconnected"] = disconnected
            return {"status": "stored_locally", "rsu_id": rsu_id}

    @app.post("/api/control/simulate-scenario")
    async def simulate_scenario(payload: Dict[str, Any]):
        """Inject a named Digital Twin scenario and forward its representative event.

        Args:
            payload: Scenario name and optional valid road-sector override.

        Returns:
            Scenario status and number of unique events accepted by cloud storage.
        """
        scenario = payload.get("scenario", "").lower()
        now = datetime.now(timezone.utc).isoformat()
        requested_sector = payload.get("sector")
        if "sector" in payload and requested_sector not in ("sector-A12-tunnel", "sector-B04-bridge"):
            return {"status": "error", "message": "Invalid sector"}

        default_sectors = {
            "emergency_corridor": "sector-B04-bridge",
            "sudden_braking": "sector-A12-tunnel",
            "road_hazard": "sector-A12-tunnel",
            "duplicate_burst": "sector-A12-tunnel",
            "normal_traffic": "sector-A12-tunnel",
        }
        if scenario not in default_sectors:
            return {"status": "error", "message": "Unknown scenario"}

        sector = requested_sector or default_sectors[scenario]
        rsu_url = settings.rsu_1_url if sector == "sector-A12-tunnel" else settings.rsu_2_url
        sector_vehicles = storage.summary()["sector_vehicles"].get(sector, {})
        events_to_inject = []
        forwarded_event = None

        def lane_for(vehicle_id: str, fallback: int) -> int:
            """Read a vehicle's current lane from sector state or use a default."""
            return int(sector_vehicles.get(vehicle_id, {}).get("payload", {}).get("lane", fallback))

        if scenario == "emergency_corridor":
            passenger_ids = ["veh-001", "veh-002", "veh-003", "veh-004"]
            random.shuffle(passenger_ids)
            left_count = random.choice((1, 2, 3))
            left_ids = set(passenger_ids[:left_count])
            for vehicle_id in passenger_ids:
                previous_lane = lane_for(vehicle_id, 1)
                lane = 1 if vehicle_id in left_ids else 3
                events_to_inject.append(V2XEvent(
                    event_id=f"demo-yield-{vehicle_id}-{now}",
                    source_id=vehicle_id,
                    event_type="traffic_telemetry",
                    location=sector,
                    payload={
                        "lane": lane,
                        "speed_kmh": random.randint(0, 5),
                        "yielding": True,
                        "pulled_from_center": previous_lane == 2,
                    },
                ))
            forwarded_event = V2XEvent(
                event_id=f"demo-emergency-{now}",
                source_id="veh-005",
                event_type="emergency_vehicle",
                location=sector,
                payload={
                    "lane": 2,
                    "speed_kmh": random.randint(85, 100),
                    "siren_active": True,
                    "target_lane": 2,
                    "vehicle_type": "ambulance",
                },
            )
            events_to_inject.append(forwarded_event)
        elif scenario == "sudden_braking":
            source_id = random.choice(("veh-001", "veh-002", "veh-003", "veh-004"))
            forwarded_event = V2XEvent(
                event_id=f"demo-braking-{now}",
                source_id=source_id,
                event_type="sudden_braking",
                location=sector,
                payload={
                    "speed_kmh": random.randint(12, 25),
                    "lane": lane_for(source_id, 2),
                    "deceleration_ms2": 7.5,
                },
            )
            events_to_inject.append(forwarded_event)
        elif scenario == "road_hazard":
            source_id = random.choice(("veh-001", "veh-002", "veh-003", "veh-004"))
            forwarded_event = V2XEvent(
                event_id=f"demo-hazard-{now}",
                source_id=source_id,
                event_type="road_hazard",
                location=sector,
                payload={
                    "lane": lane_for(source_id, 3),
                    "speed_kmh": random.randint(15, 28),
                    "hazard_type": "debris",
                    "severity": "high",
                },
            )
            events_to_inject.append(forwarded_event)
        elif scenario == "duplicate_burst":
            source_id = "veh-003"
            base_event = V2XEvent(
                event_id=f"demo-burst-{now}",
                source_id=source_id,
                event_type="traffic_telemetry",
                location=sector,
                payload={
                    "speed_kmh": 60,
                    "lane": lane_for(source_id, 1),
                    "density": "moderate",
                },
            )
            events_to_inject.extend([base_event] * 3)
            forwarded_event = base_event
        elif scenario == "normal_traffic":
            for vehicle_id in ("veh-001", "veh-002", "veh-003", "veh-004"):
                events_to_inject.append(V2XEvent(
                    event_id=f"demo-normal-{vehicle_id}-{now}",
                    source_id=vehicle_id,
                    event_type="traffic_telemetry",
                    location=sector,
                    payload={
                        "lane": random.randint(1, 3),
                        "speed_kmh": random.randint(55, 85),
                        "yielding": False,
                        "pulled_from_center": False,
                    },
                ))
            events_to_inject.append(V2XEvent(
                event_id=f"demo-ambulance-standby-{now}",
                source_id="veh-005",
                event_type="traffic_telemetry",
                location=sector,
                payload={
                    "lane": 2,
                    "speed_kmh": 0,
                    "siren_active": False,
                    "vehicle_type": "ambulance",
                },
            ))
            forwarded_event = events_to_inject[0]

        injected_count = 0
        for event in events_to_inject:
            if inject_synthetic_event(event):
                injected_count += 1
        try:
            async with httpx.AsyncClient(timeout=0.4) as client:
                response = await client.post(
                    f"{rsu_url}/api/events",
                    json=forwarded_event.model_dump(mode="json"),
                )
                response.raise_for_status()
        except httpx.HTTPError:
            logger.warning("Could not forward demo event %s to RSU at %s", forwarded_event.event_id, rsu_url)
        
        return {"status": "ok", "scenario": scenario, "events_injected": injected_count}

    return app


app = create_app()

