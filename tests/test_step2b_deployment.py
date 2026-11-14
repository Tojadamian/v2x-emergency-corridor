from pathlib import Path
import os
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
import pytest

from rsu_service import main as rsu_main
from simulator import obu_simulator
from v2x_service.config import Settings
from v2x_service.main import create_app
from v2x_service.storage import DynamoDBStorage, InMemoryStorage, METRICS_KEY

ROOT = Path(__file__).resolve().parents[1]


def test_dynamodb_reset_deletes_all_pages_and_duplicate_counter():
    items = {
        "event-1": {"event_id": "event-1", "event": {}, "alert": {}},
        "event-2": {"event_id": "event-2", "event": {}},
        METRICS_KEY: {"event_id": METRICS_KEY, "duplicates_ignored": 2},
    }
    table = MagicMock()
    table.scan.side_effect = [
        {"Items": [items["event-1"]], "LastEvaluatedKey": {"event_id": "event-1"}},
        {"Items": [items["event-2"], items[METRICS_KEY]]},
    ]
    table.delete_item.side_effect = lambda Key: items.pop(Key["event_id"])
    storage = DynamoDBStorage("test", table=table)
    storage._metrics.record_sns_alert()

    storage.reset()

    assert table.scan.call_count == 2
    assert table.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == {"event_id": "event-1"}
    assert {call.kwargs["Key"]["event_id"] for call in table.delete_item.call_args_list} == {
        "event-1", "event-2", METRICS_KEY,
    }
    assert items == {}
    assert storage._metrics.summary()["sns_alerts_published"] == 0


def test_dynamodb_reset_surfaces_delete_failure():
    table = MagicMock()
    table.scan.return_value = {"Items": [{"event_id": "event-1"}]}
    table.delete_item.side_effect = RuntimeError("delete failed")
    storage = DynamoDBStorage("test", table=table)
    storage._metrics.record_sns_alert()
    with pytest.raises(RuntimeError, match="delete failed"):
        storage.reset()
    assert storage._metrics.summary()["sns_alerts_published"] == 1


def test_favicon_is_empty_and_not_in_openapi():
    app = create_app(settings=Settings(storage_backend="memory"), storage=InMemoryStorage())
    client = TestClient(app)
    response = client.get("/favicon.ico")
    assert response.status_code == 204
    assert response.content == b""
    assert "/favicon.ico" not in client.get("/openapi.json").json()["paths"]


@pytest.mark.parametrize("qos", [1, 0, 2])
def test_rsu_mqtt_qos_for_subscriptions_and_alerts(monkeypatch, qos):
    if qos == 1:
        assert rsu_main.MQTT_QOS == 1
    monkeypatch.setattr(rsu_main, "MQTT_QOS", qos)
    client = MagicMock()
    monkeypatch.setattr(rsu_main, "mqtt_client", client)
    client.publish.return_value.rc = 0

    rsu_main._mqtt_on_connect(client, None, None, 0, None)
    rsu_main.publish_mqtt("v2x/rsu-1/alerts", {"alert": True})

    client.subscribe.assert_called_once_with([
        (f"v2x/{rsu_main.rsu_id}/telemetry", qos),
        (f"v2x/telemetry/{rsu_main.rsu_id}", qos),
    ])
    assert client.publish.call_args.kwargs["qos"] == qos


@pytest.mark.parametrize(("qos_args", "expected_qos"), [([], 1), (["--mqtt-qos", "0"], 0), (["--mqtt-qos", "2"], 2)])
def test_simulator_mqtt_qos_for_subscriptions_and_events(monkeypatch, qos_args, expected_qos):
    client = MagicMock()
    client.publish.return_value.rc = 0
    client.connect.side_effect = lambda *args, **kwargs: client.on_connect(client, None, None, 0, None)
    monkeypatch.setattr(sys, "argv", ["obu_simulator", "--mode", "mqtt", "--iterations", "1", *qos_args])
    monkeypatch.setattr(obu_simulator.time, "sleep", lambda _: None)
    with patch("paho.mqtt.client.Client", return_value=client):
        obu_simulator.main()
    client.subscribe.assert_called_once_with("v2x/+/alerts/#", qos=expected_qos)
    assert client.publish.call_args.kwargs["qos"] == expected_qos


def test_ci_publishes_dedicated_rsu_image():
    workflow = (ROOT / ".github" / "workflows" / "mvp-ci-cd.yml").read_text(encoding="utf-8")
    assert "v2x-rsu" in workflow


@pytest.mark.parametrize("rsu_pull_fails", [False, True])
def test_ec2_deploy_selects_rsu_image_or_cloud_fallback(tmp_path, rsu_pull_fails):
    log = tmp_path / "docker.log"
    aws = tmp_path / "aws"
    aws.write_text(
        '#!/bin/bash\n'
        'case "$1 $2" in\n'
        '  "sqs create-queue") echo queue-url ;;\n'
        '  "sns create-topic") echo topic-arn ;;\n'
        '  "sts get-caller-identity") echo 123456789012 ;;\n'
        '  "ecr get-login-password") echo password ;;\n'
        'esac\n',
        encoding="utf-8",
    )
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/bash\n'
        'printf "%s\\n" "$*" >> "$DOCKER_LOG"\n'
        'if [[ "$1" == pull && "$2" == */v2x-rsu:* && "$RSU_PULL_FAIL" == 1 ]]; then exit 1; fi\n',
        encoding="utf-8",
    )
    aws.chmod(0o755)
    docker.chmod(0o755)
    script = tmp_path / "deploy_ec2.sh"
    script.write_text(
        (ROOT / "deploy" / "deploy_ec2.sh").read_text(encoding="utf-8").replace(
            "/tmp/mosquitto.conf", str(tmp_path / "mosquitto.conf")
        ),
        encoding="utf-8",
    )
    env = {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "DOCKER_LOG": str(log),
        "RSU_PULL_FAIL": str(int(rsu_pull_fails)),
        "EC2_PUBLIC_IP": "192.0.2.1",
        "HOME": str(tmp_path),
        "AWS_PROFILE": "test-profile",
    }
    result = subprocess.run(
        ["bash", str(script), "test-tag"],
        env=env, capture_output=True, text=True, check=True,
    )
    commands = log.read_text(encoding="utf-8").splitlines()
    registry = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    assert f"pull {registry}/v2x-rsu:test-tag" in commands
    expected_image = f"{registry}/{'v2x-service' if rsu_pull_fails else 'v2x-rsu'}:test-tag"
    for rsu in ("rsu-1", "rsu-2"):
        run = next(command for command in commands if f"--name {rsu} " in command)
        assert f" {expected_image} uvicorn " in run
    assert ("Warning: Could not pull" in result.stderr) is rsu_pull_fails


@pytest.mark.parametrize(
    ("scenario", "expected_rsu_url", "expected_event_type", "expected_count"),
    [
        ("emergency_corridor", "http://rsu-2:8002", "emergency_vehicle", 1),
        ("sudden_braking", "http://rsu-1:8001", "sudden_braking", 1),
        ("road_hazard", "http://rsu-1:8001", "road_hazard", 1),
        ("duplicate_burst", "http://rsu-1:8001", "traffic_telemetry", 1),
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


def test_summary_sanitizes_epoch_recovery_timestamps():
    settings = Settings(
        storage_backend="memory",
        rsu_1_url="http://rsu-1:8001",
        rsu_2_url="http://rsu-2:8002",
    )
    app = create_app(settings=settings, storage=InMemoryStorage())
    response_from_rsu = MagicMock()
    response_from_rsu.json.return_value = {"last_recovery_time_ms": 1_794_628_363_698}
    response_from_rsu.raise_for_status.return_value = None
    http_client = AsyncMock()
    http_client.__aenter__.return_value = http_client
    http_client.get.return_value = response_from_rsu

    with patch("v2x_service.main.httpx.AsyncClient", return_value=http_client):
        response = TestClient(app).get("/api/summary")

    assert response.status_code == 200
    for node in response.json()["rsu_nodes"].values():
        assert node["last_recovery_time_ms"] == 1


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
