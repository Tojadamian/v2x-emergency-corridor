import ast
import json
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from experiments import run_experiments as experiments
from v2x_service.config import Settings
from v2x_service.main import create_app
from v2x_service.storage import InMemoryStorage


ROOT = Path(__file__).resolve().parent.parent


def test_delivery_anomalies():
    rows = experiments.run_delivery_anomalies_experiment()
    by_condition = {(r["condition"], r["level"]): r for r in rows}
    assert len(rows) == 9
    expected_controls = {
        ("baseline", "baseline"): {},
        ("delay", "level_1"): {"delay_ms": 50},
        ("delay", "level_2"): {"delay_ms": 250},
        ("loss", "level_1"): {"loss_rate": 0.1},
        ("loss", "level_2"): {"loss_rate": 0.3},
        ("duplication", "level_1"): {"duplicate_rate": 0.15},
        ("duplication", "level_2"): {"duplicate_rate": 0.4},
        ("reordering", "level_1"): {"reorder_rate": 0.15},
        ("reordering", "level_2"): {"reorder_rate": 0.4},
    }
    assert set(by_condition) == set(expected_controls)
    for key, controls in expected_controls.items():
        row = by_condition[key]
        assert row["controls"] == controls
        assert row["generated_events"] == 100
        assert 0 < row["accepted_by_rsu"] <= 100
        assert row["accepted_by_rsu"] == row["forwarded_to_cloud"] == row["processed_by_cloud"]
        assert row["estimated_lost_events"] >= 0
        assert row["duplicates_ignored"] >= 0
        assert row["reordered_events"] >= 0
        assert 0 <= row["p50_latency_ms"] <= row["p95_latency_ms"]
        assert isinstance(row["final_state_consistent"], bool)
    assert by_condition["baseline", "baseline"]["final_state_consistent"]
    for level in ("level_1", "level_2"):
        assert by_condition["loss", level]["estimated_lost_events"] > 0
        assert by_condition["duplication", level]["duplicates_ignored"] > 0
        assert by_condition["reordering", level]["reordered_events"] > 0
        assert by_condition["reordering", level]["final_state_consistent"]
        assert by_condition["delay", level]["p50_latency_ms"] >= expected_controls["delay", level]["delay_ms"]


def test_disconnection_buffering_and_local_alerts():
    rows = experiments.run_edge_cloud_disconnection_experiment()
    assert [row["scenario"] for row in rows] == ["A", "B"]
    for row, capacity, buffered, rejected in zip(rows, (1000, 15), (40, 15), (0, 25)):
        assert row["buffer_max_size"] == capacity
        assert row["events_sent_during_outage"] == 40
        assert row["local_alerts_during_outage"] == 40
        assert set(row["local_alert_types"]) == {
            "LOCAL_EMERGENCY_CORRIDOR_ALERT", "LOCAL_COLLISION_RISK_ALERT"
        }
        assert row["buffered_events"] == row["flushed_events"] == row["final_cloud_processed"] == buffered
        assert row["rejected_events"] == row["data_loss_count"] == rejected
        assert row["order_preserved"] is True
        assert row["recovery_time_ms"] >= 0


def test_component_failure_retries_use_worker():
    rows = experiments.run_component_failure_experiment()
    for row, level, count, retries in zip(rows, ("level_1", "level_2"), (25, 75), (5, 15)):
        assert row["level"] == level
        assert row["queue_peak_depth"] == count + retries
        assert row["unprocessed_during_outage"] == count
        assert row["retry_deliveries_injected"] == row["duplicates_ignored"] == retries
        assert row["processed_unique_events"] == count
        assert row["recovery_time_ms"] >= 0
        assert row["final_state_consistent"] is True


def test_load_increase():
    rows = experiments.run_load_increase_experiment()
    for row, level, vehicles, burst, total in zip(
        rows, ("level_1", "level_2", "level_3"), (5, 15, 30), (1, 3, 6), (100, 300, 600)
    ):
        assert (row["level"], row["vehicles"], row["burst_size"], row["total_events"]) == (
            level, vehicles, burst, total
        )
        assert 0 < row["rsu_filtered_telemetry"] < total
        assert row["cloud_processed_events"] + row["rsu_filtered_telemetry"] == total
        assert row["bandwidth_saved_pct"] == pytest.approx(
            (total - row["cloud_processed_events"]) / total * 100, abs=0.0051
        )
        assert row["throughput_events_per_sec"] > 0
        assert 0 <= row["p50_latency_ms"] <= row["p95_latency_ms"]
        assert 0 < row["max_queue_depth"] <= burst
        assert row["error_rate_pct"] == 0.0
        assert row["memory_delta_kb"] >= 0


def test_endpoint_is_isolated_and_does_not_publish(monkeypatch):
    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:v2x-alerts")
    monkeypatch.setenv("RSU_ALERT_URLS", "http://example.invalid")

    def unexpected_network(*args, **kwargs):
        pytest.fail("Isolated experiments must not contact AWS or RSUs")

    monkeypatch.setattr("v2x_service.main.boto3.client", unexpected_network)
    monkeypatch.setattr("v2x_service.main.httpx.post", unexpected_network)
    storage = InMemoryStorage()
    with TestClient(create_app(Settings(), storage)) as client:
        response = client.post("/api/experiments/run")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert set(body["results"]) == {
        "delivery_anomalies", "edge_cloud_disconnection", "component_failure", "load_increase"
    }
    assert storage.summary()["processed_events"] == 0


def test_cli_generates_report_and_json():
    subprocess.run(
        [str(ROOT / ".venv/bin/python"), "-m", "experiments.run_experiments"],
        cwd=ROOT, check=True, capture_output=True, text=True, timeout=5,
    )
    results = json.loads((ROOT / "dev_notes/experiment_results.json").read_text())
    report = (ROOT / "EXPERIMENT_RESULTS.md").read_text()
    assert len(results) == 4
    for number, name in enumerate(
        ("Delivery Anomalies", "Edge-Cloud Disconnection", "Component Failure", "Load Increase"), 1
    ):
        assert f"## Experiment {number}: {name}" in report
    assert sum(line.startswith("| --- |") for line in report.splitlines()) == 4


def test_live_url_invokes_remote_runner(monkeypatch, tmp_path):
    expected = experiments.run_all_experiments()
    calls = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"status": "ok", "results": expected}

    def post(url, timeout):
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(experiments.httpx, "post", post)
    monkeypatch.setattr(experiments, "ROOT", tmp_path)
    monkeypatch.setattr("sys.argv", ["run_experiments", "--live-url", "http://deployment/"])
    experiments.main()
    assert calls == [("http://deployment/api/experiments/run", 120)]
    assert json.loads((tmp_path / "dev_notes/experiment_results.json").read_text()) == expected
    assert "remote deployment" in (tmp_path / "EXPERIMENT_RESULTS.md").read_text()


def test_deliverable_documentation_exists_and_covers_architecture_and_schemas():
    documentation_paths = {
        name: ROOT / name
        for name in (
            "DELIVERABLE_2_DOCUMENTATION.md",
            "EXPERIMENT_RESULTS.md",
            "MVP_DOCUMENTATION.md",
        )
    }
    assert all(path.is_file() for path in documentation_paths.values())

    deliverable_2 = documentation_paths["DELIVERABLE_2_DOCUMENTATION.md"].read_text(
        encoding="utf-8"
    )
    for section in (
        "## 1. End-to-End Distributed Architecture (Figure 1 Implementation)",
        "## 2. Event & Alert Schemas",
        "## 3. Messaging Configuration, QoS, Idempotency & Edge Buffering Justification",
        "## 4. Summary of Controlled Experiments",
        "## 5. Reproducible Deployment & Demonstration Runbook",
        "OBUs -> MQTT -> RSU-1 / RSU-2 -> SQS v2x-events -> V2X-SERVICE -> DynamoDB",
        "GitHub Actions -> pytest -> Docker build -> Amazon ECR + S3 bundle -> EC2 vdeploy",
        "sector-A12-tunnel",
        "sector-B04-bridge",
        "veh-001",
        "veh-005",
    ):
        assert section in deliverable_2

    for field in (
        "event_id",
        "source_id",
        "event_type",
        "event_time",
        "location",
        "rsu_id",
        "seq_num",
        "payload",
    ):
        assert f"`{field}`" in deliverable_2
    for field in (
        "alert_id",
        "alert_type",
        "severity",
        "origin",
        "rsu_id",
        "location",
        "triggered_by_event_id",
        "source_id",
        "created_at",
        "message",
        "action_instruction",
    ):
        assert f"`{field}`" in deliverable_2
    for event_type in (
        "traffic_telemetry",
        "sudden_braking",
        "emergency_vehicle",
        "road_hazard",
        "traffic_density",
    ):
        assert f"`{event_type}`" in deliverable_2

    experiment_results = documentation_paths["EXPERIMENT_RESULTS.md"].read_text(
        encoding="utf-8"
    )
    for section in (
        "## Experiment 1: Delivery Anomalies",
        "## Experiment 2: Edge-Cloud Disconnection",
        "## Experiment 3: Component Failure",
        "## Experiment 4: Load Increase",
    ):
        assert section in experiment_results

    mvp_documentation = documentation_paths["MVP_DOCUMENTATION.md"].read_text(
        encoding="utf-8"
    )
    assert "## 1. System Architecture & Data Flow" in mvp_documentation


def test_python_modules_have_docstrings_and_deliverable2_criteria():
    source_directories = (
        "common",
        "rsu_service",
        "v2x_service",
        "simulator",
        "experiments",
    )
    module_paths = sorted(
        path
        for directory in source_directories
        for path in (ROOT / directory).rglob("*.py")
        if path.name != "__init__.py"
    )
    for path in module_paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        assert ast.get_docstring(tree), f"{path.relative_to(ROOT)} has no module docstring"
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                assert ast.get_docstring(node), (
                    f"{path.relative_to(ROOT)} class {node.name} has no docstring"
                )
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if not node.name.startswith("_"):
                    assert ast.get_docstring(node), (
                        f"{path.relative_to(ROOT)} function {node.name} has no docstring"
                    )

    simulator_path = ROOT / "simulator/obu_simulator.py"
    simulator_tree = ast.parse(simulator_path.read_text(encoding="utf-8"))
    vehicles_node = next(
        node
        for node in simulator_tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "VEHICLES"
            for target in node.targets
        )
    )
    vehicles = ast.literal_eval(vehicles_node.value)
    vehicle_ids = {vehicle[0] for vehicle in vehicles}
    assigned_rsus = {vehicle[3] for vehicle in vehicles}
    assert len(vehicle_ids) >= 5
    assert assigned_rsus == {"rsu-1", "rsu-2"}

    experiment_specs = {
        "delivery_anomalies": (
            "## Experiment 1: Delivery Anomalies",
            "run_delivery_anomalies_experiment",
            {
                "condition", "level", "controls", "generated_events", "accepted_by_rsu",
                "forwarded_to_cloud", "processed_by_cloud", "estimated_lost_events",
                "duplicates_ignored", "reordered_events", "p50_latency_ms",
                "p95_latency_ms", "final_state_consistent",
            },
        ),
        "edge_cloud_disconnection": (
            "## Experiment 2: Edge-Cloud Disconnection",
            "run_edge_cloud_disconnection_experiment",
            {
                "scenario", "buffer_max_size", "events_sent_during_outage",
                "local_alerts_during_outage", "local_alert_types", "buffered_events",
                "rejected_events", "flushed_events", "data_loss_count", "order_preserved",
                "recovery_time_ms", "final_cloud_processed",
            },
        ),
        "component_failure": (
            "## Experiment 3: Component Failure",
            "run_component_failure_experiment",
            {
                "level", "queue_peak_depth", "unprocessed_during_outage",
                "retry_deliveries_injected", "duplicates_ignored",
                "processed_unique_events", "recovery_time_ms", "final_state_consistent",
            },
        ),
        "load_increase": (
            "## Experiment 4: Load Increase",
            "run_load_increase_experiment",
            {
                "level", "vehicles", "burst_size", "total_events",
                "rsu_filtered_telemetry", "bandwidth_saved_pct", "cloud_processed_events",
                "throughput_events_per_sec", "p50_latency_ms", "p95_latency_ms",
                "max_queue_depth", "error_rate_pct", "memory_delta_kb",
            },
        ),
    }
    report = (ROOT / "EXPERIMENT_RESULTS.md").read_text(encoding="utf-8")
    experiment_source = (ROOT / "experiments/run_experiments.py").read_text(encoding="utf-8")
    for family, (heading, runner, required_metrics) in experiment_specs.items():
        assert heading in report, f"Missing {family} report section"
        assert f"def {runner}(" in experiment_source, f"Missing {family} runner"

        report_lines = report.splitlines()
        heading_index = report_lines.index(heading)
        header_line = next(
            line for line in report_lines[heading_index + 1:] if line.startswith("| ")
        )
        report_metrics = {
            column.strip() for column in header_line.strip().strip("|").split("|")
        }
        assert required_metrics <= report_metrics, (
            f"{family} report is missing metrics: {required_metrics - report_metrics}"
        )
        assert all(f'"{metric}"' in experiment_source for metric in required_metrics), (
            f"{family} runner source is missing required metric keys"
        )
