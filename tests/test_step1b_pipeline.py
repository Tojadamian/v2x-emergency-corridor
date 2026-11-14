import json
import random
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from common.schemas import V2XAlert, V2XEvent
from rsu_service import main as rsu_main
from rsu_service.edge_processor import EdgeProcessor
from simulator.obu_simulator import AnomalyInjector, VEHICLES, generate_burst, generate_event
from v2x_service import main as cloud_main
from v2x_service.config import Settings
from v2x_service.storage import InMemoryStorage


def make_event(
    event_id="step1b-event",
    event_type="traffic_telemetry",
    source_id="veh-001",
    *,
    seq_num=None,
    rsu_id="rsu-1",
    event_time=None,
    location="sector-A12-tunnel",
    **payload,
):
    return V2XEvent(
        event_id=event_id,
        event_type=event_type,
        source_id=source_id,
        seq_num=seq_num,
        rsu_id=rsu_id,
        event_time=event_time or datetime.now(timezone.utc).isoformat(),
        location=location,
        payload=payload,
    )


def test_rsu_http_and_mqtt_events_share_ingestion_pipeline(monkeypatch):
    published = []
    monkeypatch.setattr(rsu_main, "processor", EdgeProcessor(rsu_id="rsu-1"))
    monkeypatch.setattr(rsu_main, "send_to_sqs", lambda event: True)
    monkeypatch.setattr(rsu_main, "publish_mqtt", lambda topic, payload: published.append((topic, payload)))
    event = make_event(
        event_type="emergency_vehicle",
        source_id="veh-005",
        siren_active=True,
    )

    with TestClient(rsu_main.app) as client:
        http_result = client.post("/api/events", json=event.model_dump(mode="json")).json()
        assert http_result["local_alert"]["alert_type"] == "LOCAL_EMERGENCY_CORRIDOR_ALERT"

        monkeypatch.setattr(rsu_main, "processor", EdgeProcessor(rsu_id="rsu-1"))
        mqtt_result = rsu_main.handle_mqtt_payload(event.model_dump_json().encode("utf-8"))

    assert mqtt_result["processed"] == http_result["processed"]
    assert {
        key: value
        for key, value in mqtt_result["local_alert"].items()
        if key not in {"alert_id", "created_at"}
    } == {
        key: value
        for key, value in http_result["local_alert"].items()
        if key not in {"alert_id", "created_at"}
    }
    expected_topics = ["v2x/rsu-1/alerts/local", "v2x/rsu-1/alerts"]
    assert [topic for topic, _ in published] == expected_topics * 2
    assert all(payload["triggered_by_event_id"] == event.event_id for _, payload in published)


def test_rsu_mqtt_subscribes_to_both_telemetry_topics():
    class MQTTClient:
        def __init__(self):
            self.topics = None

        def subscribe(self, topics):
            self.topics = topics

    client = MQTTClient()
    rsu_main._mqtt_on_connect(client, None, None, 0, None)
    assert client.topics == [
        ("v2x/rsu-1/telemetry", 0),
        ("v2x/telemetry/rsu-1", 0),
    ]


def test_cloud_road_hazard_alert_and_traffic_density_summary():
    storage = InMemoryStorage()
    with TestClient(cloud_main.create_app(Settings(), storage)) as client:
        hazard = make_event(
            event_id="hazard-1",
            event_type="road_hazard",
            source_id="veh-002",
            hazard_type="debris",
        )
        response = client.post("/api/events/ingest", json=hazard.model_dump(mode="json"))
        alert = V2XAlert.model_validate(response.json()["alert"])
        assert alert.alert_type == "ROAD_HAZARD_ALERT"
        assert alert.severity == "WARNING"
        assert alert.origin == "cloud"
        assert "reduce speed" in alert.message
        assert "caution" in alert.action_instruction.lower()

        density = make_event(
            event_id="density-1",
            event_type="traffic_density",
            source_id="rsu-1",
            seq_num=8,
            avg_speed_kmh=32.5,
            vehicle_count=4,
            sample_count=5,
            location="sector-A12-tunnel",
        )
        density.rsu_id = "rsu-1"
        assert client.post("/api/events/ingest", json=density.model_dump(mode="json")).json()["processed"]
        summary = client.get("/api/summary").json()

    assert summary["traffic_density_reports"] == 1
    assert summary["sector_density"]["sector-A12-tunnel"] == {
        "avg_speed_kmh": 32.5,
        "vehicle_count": 4,
        "sample_count": 5,
        "rsu_id": "rsu-1",
    }


def test_cloud_sns_publish_and_direct_rsu_alert_relay(monkeypatch):
    sns_calls = []

    class FakeSNS:
        def publish(self, **kwargs):
            sns_calls.append(kwargs)
            return {"MessageId": "sns-1"}

    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:v2x-alerts")
    monkeypatch.setenv("RSU_ALERT_URLS", "http://rsu-1")
    monkeypatch.setattr(cloud_main.boto3, "client", lambda service, region_name: FakeSNS())

    relayed_mqtt = []
    monkeypatch.setattr(rsu_main, "processor", EdgeProcessor(rsu_id="rsu-1"))
    monkeypatch.setattr(
        rsu_main,
        "publish_mqtt",
        lambda topic, payload: relayed_mqtt.append((topic, payload)),
    )

    with TestClient(rsu_main.app) as rsu_client:
        def relay_http(url, *, content, headers, timeout):
            response = rsu_client.post("/sns/alerts", content=content, headers=headers)
            response.raise_for_status()
            return response

        monkeypatch.setattr(cloud_main.httpx, "post", relay_http)
        store = InMemoryStorage()
        with TestClient(cloud_main.create_app(Settings(), store)) as cloud_client:
            hazard = make_event(
                event_id="sns-hazard",
                event_type="road_hazard",
                source_id="veh-004",
                rsu_id="rsu-2",
                location="sector-B04-bridge",
                hazard_type="debris",
            )
            cloud_client.post("/api/events/ingest", json=hazard.model_dump(mode="json"))
            summary = cloud_client.get("/api/summary").json()
            rsu_status = rsu_client.get("/api/status").json()

    assert len(sns_calls) == 1
    assert sns_calls[0]["TopicArn"].endswith(":v2x-alerts")
    published_alert = json.loads(sns_calls[0]["Message"])
    assert sns_calls[0]["Subject"] == "ROAD_HAZARD_ALERT"
    assert published_alert["alert_type"] == "ROAD_HAZARD_ALERT"
    assert summary["sns_alerts_published"] == 1
    assert rsu_status["relayed_cloud_alerts_count"] == 1
    assert [topic for topic, _ in relayed_mqtt] == [
        "v2x/rsu-1/alerts/cloud",
        "v2x/rsu-1/alerts",
    ]


def test_cloud_latency_sequence_metrics_and_reset():
    storage = InMemoryStorage()
    event_time = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()
    with TestClient(cloud_main.create_app(Settings(), storage)) as client:
        for seq_num in (1, 3, 2):
            event = make_event(
                event_id=f"sequence-{seq_num}",
                seq_num=seq_num,
                event_time=event_time,
                speed_kmh=55,
            )
            client.post("/api/events/ingest", json=event.model_dump(mode="json"))
        summary = client.get("/api/summary").json()
        assert summary["p95_latency_ms"] >= summary["p50_latency_ms"] > 0
        assert summary["avg_latency_ms"] > 0
        assert summary["reordered_events"] == 1
        assert summary["estimated_lost_events"] == 1

        assert client.post("/api/control/reset").json() == {"status": "reset"}
        cleared = client.get("/api/summary").json()

    assert cleared["processed_events"] == 0
    assert cleared["p50_latency_ms"] == 0.0
    assert cleared["reordered_events"] == 0
    assert cleared["estimated_lost_events"] == 0


def test_simulator_assignments_sequences_and_bursts():
    expected = {
        "veh-001": ("rsu-1", "sector-A12-tunnel"),
        "veh-002": ("rsu-1", "sector-A12-tunnel"),
        "veh-003": ("rsu-1", "sector-A12-tunnel"),
        "veh-004": ("rsu-2", "sector-B04-bridge"),
        "veh-005": ("rsu-2", "sector-B04-bridge"),
    }
    assert {vehicle[0]: (vehicle[3], vehicle[2]) for vehicle in VEHICLES} == expected

    sequences = {}
    first = generate_event(VEHICLES[0], sequences, random.Random(1))
    second = generate_event(VEHICLES[0], sequences, random.Random(1))
    assert (first.rsu_id, first.location, first.seq_num) == ("rsu-1", "sector-A12-tunnel", 1)
    assert second.seq_num == 2
    burst, next_index = generate_burst(7, sequence_numbers={}, rng=random.Random(3))
    assert len(burst) == 7
    assert next_index == 2
    assert [event.seq_num for event in burst[:5]] == [1, 1, 1, 1, 1]


def test_simulator_loss_duplication_reordering_and_delay_controls():
    event = make_event(seq_num=1)
    lost = []
    assert AnomalyInjector(loss_rate=1.0, rng=random.Random(5)).transmit(event, lost.append) == 0
    assert lost == []

    duplicate_event = make_event(event_id="duplicate", seq_num=2)
    duplicated = []
    count = AnomalyInjector(duplicate_rate=1.0, rng=random.Random(5)).transmit(
        duplicate_event, duplicated.append
    )
    assert count == 2
    assert duplicated[0] is duplicated[1]
    assert duplicated[0].event_id == duplicated[1].event_id

    reordered = []
    reorderer = AnomalyInjector(reorder_rate=1.0, rng=random.Random(5))
    older = make_event(event_id="older", seq_num=1)
    newer = make_event(event_id="newer", seq_num=2)
    assert reorderer.transmit(older, reordered.append) == 0
    reorderer.reorder_rate = 0.0
    unrelated = make_event(event_id="other-vehicle", source_id="veh-002", seq_num=1)
    assert reorderer.transmit(unrelated, reordered.append) == 1
    assert [item.seq_num for item in reordered] == [1]
    reordered.clear()
    assert reorderer.transmit(newer, reordered.append) == 2
    assert [item.seq_num for item in reordered] == [2, 1]

    delays = []
    delayed = make_event(event_time=datetime.now(timezone.utc).isoformat())
    sender_calls = []
    AnomalyInjector(delay_ms=250, rng=random.Random(5), sleep=delays.append).transmit(
        delayed, sender_calls.append
    )
    assert delays == [0.25]
    assert sender_calls == [delayed]
    assert datetime.fromisoformat(delayed.event_time) < datetime.now(timezone.utc)
