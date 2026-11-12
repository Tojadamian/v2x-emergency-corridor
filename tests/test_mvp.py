from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from pydantic import ValidationError

from common.schemas import V2XEvent
from simulator.obu_simulator import VEHICLES, generate_event
from v2x_service.config import Settings
from v2x_service.main import create_app
from v2x_service.storage import DynamoDBStorage, InMemoryStorage
from v2x_service.worker import SQSWorker


def event(event_id="event-1", event_type="traffic_telemetry", source_id="veh-001", **payload):
    return V2XEvent(event_id=event_id, source_id=source_id, event_type=event_type,
                    location="sector-A12-tunnel", payload=payload)


@pytest.mark.parametrize("changes", [
    {"event_type": "unknown"}, {"source_id": ""}, {"event_time": "not-a-time"},
    {"event_time": "2024-01-01T00:00:00"}, {"payload": {"speed_kmh": -1}},
    {"payload": {"deceleration_ms2": "bad"}}, {"payload": {"siren_active": "false"}},
    {"event_type": "sudden_braking", "payload": {}}, {"event_id": "__v2x_duplicates__"},
])
def test_schema_rejects_invalid_events(changes):
    data = event().model_dump()
    data.update(changes)
    with pytest.raises(ValidationError):
        V2XEvent.model_validate(data)


def test_unique_defaults_and_simulator_events():
    assert event().event_id == "event-1"
    assert V2XEvent(source_id="a", event_type="traffic_telemetry", location="x", payload={}).event_id != \
        V2XEvent(source_id="a", event_type="traffic_telemetry", location="x", payload={}).event_id
    for vehicle in VEHICLES:
        generated = generate_event(vehicle)
        assert generated.source_id == vehicle[0]
        assert generated.payload["vehicle_type"] == vehicle[1]


def test_emergency_idempotency_and_latest_state():
    store = InMemoryStorage()
    emergency = event(event_type="emergency_vehicle", source_id="veh-005", siren_active=True, speed_kmh=80)
    processed, alert = store.process_event(emergency)
    assert processed and alert.alert_type == "EMERGENCY_CORRIDOR_ALERT"
    assert "LEFT" in alert.action_instruction and "RIGHT" in alert.action_instruction
    assert store.process_event(emergency) == (False, None)
    summary = store.summary()
    assert summary["processed_events"] == 1
    assert summary["duplicates_ignored"] == 1
    assert len(summary["recent_alerts"]) == 1
    assert summary["active_emergency_corridors"] == ["sector-A12-tunnel"]
    assert summary["vehicles"]["veh-005"]["event_id"] == emergency.event_id
    cleared = event("event-2", "emergency_vehicle", "veh-005", siren_active=False, speed_kmh=80)
    store.process_event(cleared)
    assert store.summary()["active_emergency_corridors"] == []


@pytest.mark.parametrize("deceleration,expected", [(5.9, False), (6.0, True), (7.5, True)])
def test_braking_threshold(deceleration, expected):
    store = InMemoryStorage()
    _, alert = store.process_event(event(event_type="sudden_braking", deceleration_ms2=deceleration, speed_kmh=80))
    assert (alert is not None) == expected
    if alert:
        assert alert.alert_type == "SEVERE_BRAKING_CONGESTION_ALERT"


def test_congestion_uses_latest_speed_and_strict_threshold():
    store = InMemoryStorage()
    assert store.process_event(event("a", speed_kmh=30))[1] is None
    assert store.process_event(event("b", source_id="veh-002", speed_kmh=20))[1] is None
    _, alert = store.process_event(event("c", source_id="veh-002", speed_kmh=18))
    assert alert.alert_type == "SEVERE_BRAKING_CONGESTION_ALERT"
    assert "24.0 km/h" in alert.message


def test_ambulance_entering_occupied_segment_triggers_corridor():
    store = InMemoryStorage()
    store.process_event(event("car", speed_kmh=60))
    ambulance = event("ambulance-old", source_id="veh-005", vehicle_type="ambulance", speed_kmh=70)
    ambulance.location = "sector-B04-bridge"
    store.process_event(ambulance)
    _, alert = store.process_event(event("ambulance-new", source_id="veh-005", vehicle_type="ambulance", speed_kmh=70))
    assert alert.alert_type == "EMERGENCY_CORRIDOR_ALERT"
    assert store.summary()["active_emergency_corridors"] == ["sector-A12-tunnel"]


def test_concurrent_duplicate_events_only_commit_once():
    store = InMemoryStorage()
    emergency = event(event_type="emergency_vehicle")
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(store.process_event, [emergency] * 30))
    assert sum(processed for processed, _ in results) == 1
    summary = store.summary()
    assert summary["duplicates_ignored"] == 29
    assert len(summary["recent_alerts"]) == 1


def test_api_ingest_and_dashboard():
    with TestClient(create_app(Settings(), InMemoryStorage())) as client:
        body = event(event_type="emergency_vehicle", siren_active=True).model_dump()
        assert client.post("/api/events/ingest", json=body).json()["processed"] is True
        assert client.post("/api/events/ingest", json=body).json()["processed"] is False
        assert client.post("/api/events/ingest", json={}).status_code == 422
        summary = client.get("/api/summary").json()
        assert summary["duplicates_ignored"] == 1
        assert len(client.get("/api/alerts").json()) == 1
        assert len(client.get("/api/events").json()) == 1
        assert client.get("/api/state").json()["vehicles"]["veh-001"]["event_id"] == body["event_id"]
        assert client.get("/").status_code == 200
        assert client.get("/ui/app.js").status_code == 200


def test_config_validation(monkeypatch):
    monkeypatch.setenv("ENABLE_SQS_WORKER", "true")
    monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
    with pytest.raises(ValueError, match="SQS_QUEUE_URL"):
        Settings.from_env()
    monkeypatch.setenv("ENABLE_SQS_WORKER", "false")
    monkeypatch.setenv("STORAGE_BACKEND", "bad")
    with pytest.raises(ValueError, match="STORAGE_BACKEND"):
        Settings.from_env()


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, Key, **kwargs):
        return {"Item": self.items[Key["event_id"]]} if Key["event_id"] in self.items else {}

    def scan(self, **kwargs):
        return {"Items": list(self.items.values())}

    def update_item(self, Key, **kwargs):
        item = self.items.setdefault(Key["event_id"], {"event_id": Key["event_id"]})
        item["duplicates_ignored"] = item.get("duplicates_ignored", 0) + 1

    def put_item(self, Item, **kwargs):
        if Item["event_id"] in self.items:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[Item["event_id"]] = Item


def test_dynamodb_stores_atomic_event_alert_and_counts_duplicates():
    table = FakeTable()
    store = DynamoDBStorage("test", table=table)
    emergency = event(event_type="emergency_vehicle", speed_kmh=40.5)
    assert store.process_event(emergency)[0] is True
    assert isinstance(table.items[emergency.event_id]["event"]["payload"]["speed_kmh"], Decimal)
    assert store.process_event(emergency)[0] is False
    summary = store.summary()
    assert summary["processed_events"] == 1
    assert summary["duplicates_ignored"] == 1
    assert len(summary["recent_alerts"]) == 1


def test_sqs_deletes_processed_and_invalid_but_not_failed_messages():
    class Client:
        def __init__(self):
            self.deleted = []

        def delete_message(self, **kwargs):
            self.deleted.append(kwargs["ReceiptHandle"])

    client = Client()
    worker = SQSWorker(InMemoryStorage(), "queue", "us-east-1", client=client)
    worker.process_message({"Body": event().model_dump_json(), "ReceiptHandle": "valid"})
    worker.process_message({"Body": "not json", "ReceiptHandle": "invalid"})
    assert client.deleted == ["valid", "invalid"]

    class BrokenStorage(InMemoryStorage):
        def process_event(self, event):
            raise RuntimeError("storage unavailable")

    worker.storage = BrokenStorage()
    with pytest.raises(RuntimeError):
        worker.process_message({"Body": event().model_dump_json(), "ReceiptHandle": "retry"})
    assert client.deleted == ["valid", "invalid"]
