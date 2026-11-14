from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
import pytest

from v2x_service.config import Settings
from v2x_service.main import create_app
from v2x_service.storage import InMemoryStorage

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("scenario", "expected_rsu_url", "expected_event_type", "expected_count"),
    [
        ("emergency_corridor", "http://rsu-2:8002", "emergency_vehicle", 1),
        ("sudden_braking", "http://rsu-1:8001", "sudden_braking", 1),
        ("road_hazard", "http://rsu-1:8001", "road_hazard", 1),
        ("duplicate_burst", "http://rsu-1:8001", "traffic_telemetry", 3),
    ],
)
def test_scenario_events_are_forwarded_to_the_assigned_rsu(
    scenario, expected_rsu_url, expected_event_type, expected_count
):
    settings = Settings(
        storage_backend="memory",
        rsu_1_url="http://rsu-1:8001",
        rsu_2_url="http://rsu-2:8002",
    )
    app = create_app(settings=settings, storage=InMemoryStorage())
    response_from_rsu = MagicMock()
    response_from_rsu.raise_for_status.return_value = None
    http_client = AsyncMock()
    http_client.__aenter__.return_value = http_client
    http_client.post.return_value = response_from_rsu

    with patch("v2x_service.main.httpx.AsyncClient", return_value=http_client):
        response = TestClient(app).post(
            "/api/control/simulate-scenario",
            json={"scenario": scenario},
        )

    assert response.status_code == 200
    assert http_client.post.await_count == expected_count
    for call in http_client.post.await_args_list:
        assert call.args[0] == f"{expected_rsu_url}/api/events"
        assert call.kwargs["json"]["event_type"] == expected_event_type


def test_summary_normalizes_live_rsu_status_aliases():
    settings = Settings(
        storage_backend="memory",
        rsu_1_url="http://rsu-1:8001",
        rsu_2_url="http://rsu-2:8002",
    )
    app = create_app(settings=settings, storage=InMemoryStorage())
    status = {
        "rsu_id": "rsu-live",
        "cloud_disconnected": True,
        "received_events": 10,
        "filtered_telemetry_count": 4,
        "forwarded_events": 6,
        "local_alerts_count": 2,
        "relayed_cloud_alerts_count": 3,
        "buffered_count": 1,
        "rejected_events": 2,
        "flushed_events_count": 5,
        "last_recovery_time_ms": 1234,
    }
    response_from_rsu = MagicMock()
    response_from_rsu.json.return_value = status
    response_from_rsu.raise_for_status.return_value = None
    http_client = AsyncMock()
    http_client.__aenter__.return_value = http_client
    http_client.get.return_value = response_from_rsu

    with patch("v2x_service.main.httpx.AsyncClient", return_value=http_client):
        response = TestClient(app).get("/api/summary")

    assert response.status_code == 200
    nodes = response.json()["rsu_nodes"]
    for node in nodes.values():
        assert node["cloud_disconnected"] is True
        assert node["cloud_connected"] is False
        assert node["forwarded_events"] == node["forwarded_to_sqs"] == 6
        assert node["local_alerts_count"] == node["local_edge_alerts"] == 2
        assert node["relayed_cloud_alerts_count"] == node["relayed_cloud_alerts"] == 3
        assert node["received_events"] == 10
        assert node["filtered_telemetry_count"] == 4
        assert node["bandwidth_saved_pct"] == 40
        assert node["buffered_count"] == 1
        assert node["rejected_events"] == 2
        assert node["flushed_events_count"] == 5
        assert node["last_recovery_time_ms"] == 1234


def test_dashboard_rsu_controls_and_emergency_corridor_rendering():
    app_js = (ROOT / "static" / "app.js").read_text(encoding="utf-8")

    assert ":contains(" not in app_js
    assert "event.target" not in app_js
    assert "latestRsuNodes = data.rsu_nodes || {}" in app_js
    assert "latestRsuNodes[rsuId]?.cloud_disconnected" in app_js
    assert 'await update();' in app_js
    assert "emergency-corridor-banner" in app_js
    assert "EMERGENCY CORRIDOR OPEN" in app_js
    assert 'badge.className += " pull-left"' in app_js
    assert 'badge.className += " pull-right"' in app_js


def test_shared_image_compose_and_ec2_deployment_configuration():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    mosquitto = (ROOT / "deploy" / "mosquitto.conf").read_text(encoding="utf-8")
    deploy_script = (ROOT / "deploy" / "deploy_ec2.sh").read_text(encoding="utf-8")

    for directory in ("common", "v2x_service", "rsu_service", "simulator", "static"):
        assert f"COPY {directory}/" in dockerfile
    assert 'CMD ["uvicorn", "v2x_service.main:app", "--host", "0.0.0.0", "--port", "8000"]' in dockerfile

    for service in ("v2x-mqtt", "rsu-1", "rsu-2", "v2x-service"):
        assert service in compose
        assert service in deploy_script
    assert "v2x-alerts" in deploy_script
    assert "listener 1883 0.0.0.0" in mosquitto
    assert "allow_anonymous true" in mosquitto
    assert "aws sqs create-queue" in deploy_script
    assert "aws sns create-topic" in deploy_script
    assert "v2x-net" in deploy_script
