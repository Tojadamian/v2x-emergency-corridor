"""Run isolated, controlled edge/cloud experiments without AWS resources."""

import argparse
import json
import random
import resource
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from common.schemas import V2XEvent
from rsu_service.edge_processor import EdgeProcessor
from simulator.obu_simulator import AnomalyInjector, VEHICLES, generate_event
from v2x_service.config import Settings
from v2x_service.storage import InMemoryStorage, _percentile
from v2x_service.worker import SQSWorker

ROOT = Path(__file__).resolve().parent.parent


def _edges(buffer_max_size: int = 1000) -> dict[str, EdgeProcessor]:
    return {
        "rsu-1": EdgeProcessor("rsu-1", "sector-A12-tunnel", buffer_max_size=buffer_max_size),
        "rsu-2": EdgeProcessor("rsu-2", "sector-B04-bridge", buffer_max_size=buffer_max_size),
    }


class Cloud:
    """Use the service's real ingestion endpoint with isolated memory storage."""

    def __init__(self) -> None:
        from v2x_service.main import create_app

        self.storage = InMemoryStorage()
        app = create_app(Settings(), self.storage, deliver_alerts=False)
        self.ingest = next(route.endpoint for route in app.routes if route.path == "/api/events/ingest")
        self.delivered: list[V2XEvent] = []

    def send(self, event: V2XEvent) -> bool:
        processed = self.ingest(event)["processed"]
        if processed:
            self.delivered.append(event)
        return processed

    def consistent_with(self, expected: list[V2XEvent]) -> bool:
        latest: dict[str, V2XEvent] = {}
        for event in expected:
            if event.source_id not in latest or event.event_time > latest[event.source_id].event_time:
                latest[event.source_id] = event
        actual = self.storage.summary()["vehicles"]
        return {source: event.event_id for source, event in latest.items()} == {
            source: event["event_id"] for source, event in actual.items()
        }


def _vehicle(index: int) -> tuple[str, str, str, str]:
    if index < len(VEHICLES):
        return VEHICLES[index]
    rsu_id, sector = (
        ("rsu-1", "sector-A12-tunnel") if index % 2 == 0
        else ("rsu-2", "sector-B04-bridge")
    )
    return (f"veh-{index + 1:03d}", "car", sector, rsu_id)


def _event(
    index: int, vehicle_index: int, sequences: dict[str, int], rng: random.Random,
    base_time: datetime, critical: bool = False,
) -> V2XEvent:
    event = generate_event(_vehicle(vehicle_index), sequences, rng)
    event.event_id = f"experiment-{index:04d}"
    event.event_time = (base_time + timedelta(microseconds=index)).isoformat()
    if critical:
        event.event_type = "road_hazard"
        event.payload["hazard_type"] = "debris"
    return event


def _latencies(samples: list[float]) -> dict[str, float]:
    return {
        "p50_latency_ms": round(_percentile(samples, 0.5), 3),
        "p95_latency_ms": round(_percentile(samples, 0.95), 3),
    }


def run_delivery_anomalies_experiment() -> list[dict[str, Any]]:
    conditions = [
        ("baseline", "baseline", {}),
        ("delay", "level_1", {"delay_ms": 50}),
        ("delay", "level_2", {"delay_ms": 250}),
        ("loss", "level_1", {"loss_rate": 0.10}),
        ("loss", "level_2", {"loss_rate": 0.30}),
        ("duplication", "level_1", {"duplicate_rate": 0.15}),
        ("duplication", "level_2", {"duplicate_rate": 0.40}),
        ("reordering", "level_1", {"reorder_rate": 0.15}),
        ("reordering", "level_2", {"reorder_rate": 0.40}),
    ]
    results = []
    for condition, level, controls in conditions:
        edges, cloud = _edges(), Cloud()
        rng = random.Random(2026)
        injector = AnomalyInjector(rng=random.Random(301), sleep=lambda _: None, **controls)
        sequences: dict[str, int] = {}
        generated: list[V2XEvent] = []
        starts: dict[str, float] = {}
        samples: list[float] = []
        base_time = datetime.now(timezone.utc) - timedelta(seconds=1)

        def receive(event: V2XEvent) -> None:
            forwarded, _ = edges[event.rsu_id].process_event(event)
            if forwarded is not None:
                if cloud.send(forwarded):
                    samples.append((time.perf_counter() - starts[event.event_id]) * 1000
                                   + controls.get("delay_ms", 0))

        for index in range(100):
            event = _event(index, index % 5, sequences, rng, base_time, critical=True)
            generated.append(event.model_copy(deep=True))
            starts[event.event_id] = time.perf_counter()
            injector.transmit(event, receive)
        injector.flush(receive)
        results.append({
            "condition": condition,
            "level": level,
            "controls": controls,
            "generated_events": len(generated),
            "accepted_by_rsu": sum(e.received_events - e.duplicates_ignored for e in edges.values()),
            "forwarded_to_cloud": sum(e.forwarded_events for e in edges.values()),
            "processed_by_cloud": len(cloud.delivered),
            "estimated_lost_events": sum(e.estimated_lost_events for e in edges.values()),
            "duplicates_ignored": sum(e.duplicates_ignored for e in edges.values()),
            "reordered_events": sum(e.reordered_events for e in edges.values()),
            **_latencies(samples),
            "final_state_consistent": cloud.consistent_with(generated),
        })
    return results


def run_edge_cloud_disconnection_experiment() -> list[dict[str, Any]]:
    results = []
    for scenario, capacity in (("A", 1000), ("B", 15)):
        edge, cloud = _edges(capacity)["rsu-1"], Cloud()
        edge.cloud_disconnected = True
        sequences: dict[str, int] = {}
        rng = random.Random(42)
        base_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        accepted: list[str] = []
        alert_types: list[str] = []
        for index in range(40):
            event = _event(index, index % 3, sequences, rng, base_time, critical=True)
            event.event_type = "emergency_vehicle" if index % 2 == 0 else "sudden_braking"
            event.payload.update(siren_active=True, deceleration_ms2=7.5)
            forwarded, alert = edge.process_event(event)
            if alert:
                alert_types.append(alert.alert_type)
            if forwarded is not None and edge.buffer_event(forwarded):
                accepted.append(forwarded.event_id)
        buffered = edge.buffered_count
        edge.cloud_disconnected = False
        start = time.perf_counter()
        flushed = edge.flush_buffer()
        for event in flushed:
            cloud.send(event)
        recovery = (time.perf_counter() - start) * 1000
        results.append({
            "scenario": scenario,
            "buffer_max_size": capacity,
            "events_sent_during_outage": 40,
            "local_alerts_during_outage": len(alert_types),
            "local_alert_types": sorted(set(alert_types)),
            "buffered_events": buffered,
            "rejected_events": edge.rejected_events,
            "flushed_events": len(flushed),
            "data_loss_count": 40 - len(cloud.delivered),
            "order_preserved": accepted == [e.event_id for e in cloud.delivered],
            "recovery_time_ms": round(recovery, 3),
            "final_cloud_processed": len(cloud.delivered),
        })
    return results


class _QueueClient:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def delete_message(self, QueueUrl: str, ReceiptHandle: str) -> None:
        self.deleted.append(ReceiptHandle)


def run_component_failure_experiment() -> list[dict[str, Any]]:
    results = []
    for level, count, retries in (("level_1", 25, 5), ("level_2", 75, 15)):
        cloud = Cloud()
        client = _QueueClient()
        worker = SQSWorker(cloud.storage, "in-process", "us-east-1", client=client)
        sequences: dict[str, int] = {}
        rng = random.Random(73)
        base_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        events = [_event(i, i % 5, sequences, rng, base_time, critical=True) for i in range(count)]
        queue = deque(events + events[:retries])
        peak = len(queue)
        unprocessed = cloud.storage.summary()["processed_events"]
        start = time.perf_counter()
        while queue:
            event = queue.popleft()
            worker.process_message({"Body": event.model_dump_json(), "ReceiptHandle": str(len(client.deleted))})
        recovery = (time.perf_counter() - start) * 1000
        summary = cloud.storage.summary()
        actual = {source: event["event_id"] for source, event in summary["vehicles"].items()}
        expected = {}
        for event in events:
            expected[event.source_id] = event.event_id
        results.append({
            "level": level,
            "queue_peak_depth": peak,
            "unprocessed_during_outage": count - unprocessed,
            "retry_deliveries_injected": retries,
            "duplicates_ignored": summary["duplicates_ignored"],
            "processed_unique_events": summary["processed_events"],
            "recovery_time_ms": round(recovery, 3),
            "final_state_consistent": actual == expected and summary["processed_events"] == count,
        })
    return results


def _peak_memory_kb() -> float:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value / 1024 if sys.platform == "darwin" else float(value)


def run_load_increase_experiment() -> list[dict[str, Any]]:
    results = []
    for level, vehicles, burst, total in (
        ("level_1", 5, 1, 100),
        ("level_2", 15, 3, 300),
        ("level_3", 30, 6, 600),
    ):
        memory_before = _peak_memory_kb()
        edges, cloud = _edges(), Cloud()
        sequences: dict[str, int] = {}
        rng = random.Random(2026)
        base_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        samples: list[float] = []
        failures = 0
        peak = 0
        start = time.perf_counter()
        for offset in range(0, total, burst):
            queue: deque[tuple[V2XEvent, float]] = deque()
            for index in range(offset, min(offset + burst, total)):
                created = time.perf_counter()
                event = _event(index, index % vehicles, sequences, rng, base_time)
                forwarded, _ = edges[event.rsu_id].process_event(event)
                if forwarded is not None:
                    queue.append((forwarded, created))
            peak = max(peak, len(queue))
            while queue:
                forwarded, created = queue.popleft()
                if not cloud.send(forwarded):
                    failures += 1
                samples.append((time.perf_counter() - created) * 1000)
        elapsed = time.perf_counter() - start
        filtered = sum(e.filtered_telemetry_count for e in edges.values())
        results.append({
            "level": level,
            "vehicles": vehicles,
            "burst_size": burst,
            "total_events": total,
            "rsu_filtered_telemetry": filtered,
            "bandwidth_saved_pct": round((total - len(cloud.delivered)) / total * 100, 2),
            "cloud_processed_events": len(cloud.delivered),
            "throughput_events_per_sec": round(total / elapsed, 2),
            **_latencies(samples),
            "max_queue_depth": peak,
            "error_rate_pct": round(failures / total * 100, 2),
            "memory_delta_kb": round(max(0, _peak_memory_kb() - memory_before), 2),
        })
    return results


def run_all_experiments() -> dict[str, Any]:
    return {
        "delivery_anomalies": run_delivery_anomalies_experiment(),
        "edge_cloud_disconnection": run_edge_cloud_disconnection_experiment(),
        "component_failure": run_component_failure_experiment(),
        "load_increase": run_load_increase_experiment(),
    }


def _table(rows: list[dict[str, Any]]) -> str:
    columns = list(rows[0])
    def cell(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True).replace("|", "\\|")
        return str(value).replace("|", "\\|")
    return "\n".join([
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
        *("| " + " | ".join(cell(row[column]) for column in columns) + " |" for row in rows),
    ])


def render_report(results: dict[str, Any], source: str) -> str:
    sections = [
        ("Experiment 1: Delivery Anomalies", "delivery_anomalies"),
        ("Experiment 2: Edge-Cloud Disconnection", "edge_cloud_disconnection"),
        ("Experiment 3: Component Failure", "component_failure"),
        ("Experiment 4: Load Increase", "load_increase"),
    ]
    report = [
        "# Controlled Experiment Results",
        "## Methodology",
        f"Execution target: {source}. Each run uses fresh, isolated in-memory state, "
        "two real edge processors, simulator-generated events, and the cloud rule engine. "
        "Random seeds and workloads are fixed; elapsed time, memory and generated alert IDs are not deterministic.",
        "Delivery trials use 100 critical hazard events so aggregation cannot hide loss. "
        "The real anomaly injector applies independently seeded controls; percentages are probabilities, "
        "not exact counts. Delay uses virtual sleep and is added to observed processing latency; "
        "it is not a network or wall-clock delay. Reordering holds events per vehicle.",
        "Disconnection buffers forwarded events and measures flush plus cloud processing. "
        "Component failure pauses a simulated FIFO queue, injects duplicate message deliveries, "
        "then uses the real SQS worker's message handler; it does not stop a deployed process "
        "or model SQS timing or ordering guarantees.",
        "Load sends generated events in bursts to both RSUs, queues forwarded events per burst, "
        "then drains into the cloud. Throughput is input events divided by elapsed wall time; "
        "latency is generation-to-cloud completion for forwarded events only, not filtered events. "
        "Bandwidth savings are the reduction in event count, not measured bytes. "
        "Memory is the increase in process peak RSS, so zero does not mean no allocation. "
        "Queue depth is the in-process burst queue, not an AWS queue. Timing and memory vary by host.",
        "Final-state consistency compares latest event IDs by source with the generated input for "
        "delivery trials, and with unique queued events for worker trials. "
        "Sequence gaps are estimates and can overcount reordered or late packets; tail losses "
        "cannot be detected from a subsequent sequence number.",
    ]
    for title, key in sections:
        report.extend([f"## {title}", _table(results[key])])
    a, b = results["edge_cloud_disconnection"]
    delivery = results["delivery_anomalies"]
    failure = results["component_failure"]
    load = results["load_increase"]
    report.extend([
        "## Engineering Conclusions",
        f"The higher loss trial accepted {delivery[4]['accepted_by_rsu']} of "
        f"{delivery[4]['generated_events']} events and reported "
        f"{delivery[4]['estimated_lost_events']} estimated gaps. The higher reordering trial "
        f"reported {delivery[8]['reordered_events']} reordered events and "
        f"{delivery[8]['estimated_lost_events']} estimated gaps despite no injected loss. "
        "Gap estimates therefore cannot be treated as exact loss counts.",
        f"With capacities {a['buffer_max_size']} and {b['buffer_max_size']}, the edge retained "
        f"{a['buffered_events']} and {b['buffered_events']} events respectively, rejecting "
        f"{a['rejected_events']} and {b['rejected_events']}. Local safety alerts remained "
        "available during the partition, but bounded queues trade availability for eventual data loss.",
        f"On restart, the worker ignored {failure[0]['duplicates_ignored']} and "
        f"{failure[1]['duplicates_ignored']} retry deliveries while processing "
        f"{failure[0]['processed_unique_events']} and {failure[1]['processed_unique_events']} "
        "unique events. Idempotency suppresses repeated deliveries, while timestamp-based state "
        "selection mitigates reordering; neither recovers lost data.",
        f"At {load[0]['total_events']} and {load[-1]['total_events']} input events, edge filtering "
        f"reduced forwarded event counts by {load[0]['bandwidth_saved_pct']}% and "
        f"{load[-1]['bandwidth_saved_pct']}%. Aggregation reduces cloud traffic at the cost "
        "of individual telemetry visibility; local measurements do not establish production capacity.",
    ])
    return "\n\n".join(report) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-url", help="Base URL of a deployment with the experiment API")
    args = parser.parse_args()
    if args.live_url:
        response = httpx.post(f"{args.live_url.rstrip('/')}/api/experiments/run", timeout=120)
        response.raise_for_status()
        body = response.json()
        if body.get("status") != "ok" or not isinstance(body.get("results"), dict):
            raise ValueError("Deployment returned an invalid experiment response")
        results = body["results"]
        source = "remote deployment, isolated in-process runner"
    else:
        results = run_all_experiments()
        source = "local isolated in-process runner"
    notes = ROOT / "dev_notes"
    notes.mkdir(exist_ok=True)
    (notes / "experiment_results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    (ROOT / "EXPERIMENT_RESULTS.md").write_text(render_report(results, source), encoding="utf-8")
    print("Experiment results written to dev_notes/experiment_results.json and EXPERIMENT_RESULTS.md")


if __name__ == "__main__":
    main()
