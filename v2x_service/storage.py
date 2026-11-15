"""Idempotent event storage, timestamp-ordered state, and experiment metrics."""

import json
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Protocol, Tuple

import boto3
from botocore.exceptions import ClientError

from common.schemas import V2XAlert, V2XEvent
from v2x_service.rules import evaluate_event_for_alert

METRICS_KEY = "__v2x_duplicates__"
SECTORS = ("sector-A12-tunnel", "sector-B04-bridge")
BASELINE_VEHICLES = {
    "veh-001": (1, 64),
    "veh-002": (2, 72),
    "veh-003": (3, 58),
    "veh-004": (1, 78),
}


def event_time(event: V2XEvent) -> datetime:
    """Parse an event's validated ISO 8601 timestamp for ordering comparisons."""
    return datetime.fromisoformat(event.event_time.replace("Z", "+00:00"))


def latest_states(events: List[V2XEvent]) -> Dict[str, V2XEvent]:
    """Select each vehicle's newest event by event timestamp, not arrival order.

    Args:
        events: Events already accepted by the storage backend.

    Returns:
        A mapping from source identifier to its latest event.
    """
    states: Dict[str, V2XEvent] = {}
    for event in events:
        previous = states.get(event.source_id)
        if previous is None or event_time(event) > event_time(previous):
            states[event.source_id] = event
    return states


def baseline_sector_vehicles() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Create dashboard baseline vehicle records for both configured sectors."""
    sector_vehicles: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for sector in SECTORS:
        sector_vehicles[sector] = {}
        for vehicle_id, (lane, speed_kmh) in BASELINE_VEHICLES.items():
            event = V2XEvent(
                event_id=f"baseline-{sector}-{vehicle_id}",
                source_id=vehicle_id,
                event_type="traffic_telemetry",
                location=sector,
                payload={"lane": lane, "speed_kmh": speed_kmh},
            )
            sector_vehicles[sector][vehicle_id] = event.model_dump(mode="json")
        ambulance = V2XEvent(
            event_id=f"baseline-{sector}-veh-005",
            source_id="veh-005",
            event_type="traffic_telemetry",
            location=sector,
            payload={
                "lane": 2,
                "speed_kmh": 0,
                "siren_active": False,
                "vehicle_type": "ambulance",
            },
        )
        sector_vehicles[sector]["veh-005"] = ambulance.model_dump(mode="json")
    return sector_vehicles


def _percentile(samples: List[float], percentile: float) -> float:
    """Interpolate a percentile from numeric samples, returning zero when empty."""
    if not samples:
        return 0.0
    ordered = sorted(samples)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


class ExperimentMetrics:
    """Collect thread-safe latency, sequence, aggregation, and SNS counters."""

    def __init__(self) -> None:
        """Create empty experiment metrics and initialize their synchronization lock."""
        self._lock = threading.Lock()
        self.reset()

    def record_event(self, event: V2XEvent) -> None:
        """Record event latency, sequence gaps, reordering, and density output."""
        now = datetime.now(timezone.utc)
        latency = max(0.0, (now - event_time(event).astimezone(timezone.utc)).total_seconds() * 1000)
        with self._lock:
            self.latencies.append(latency)
            if len(self.latencies) > 5000:
                del self.latencies[:-5000]

            if event.seq_num is not None:
                previous = self.sequence_by_source.get(event.source_id)
                if previous is not None:
                    last_seq = previous
                    if event.seq_num < last_seq:
                        self.reordered_events += 1
                    elif event.seq_num > last_seq + 1:
                        self.estimated_lost_events += event.seq_num - last_seq - 1
                    self.sequence_by_source[event.source_id] = max(last_seq, event.seq_num)
                else:
                    self.sequence_by_source[event.source_id] = event.seq_num

            if event.event_type == "traffic_density":
                self.traffic_density_reports += 1
                self.sector_density[event.location] = {
                    "avg_speed_kmh": event.payload.get("avg_speed_kmh"),
                    "vehicle_count": event.payload.get("vehicle_count"),
                    "sample_count": event.payload.get("sample_count"),
                    "rsu_id": event.rsu_id,
                }

    def record_sns_alert(self) -> None:
        """Increment the count of alerts successfully published to SNS."""
        with self._lock:
            self.sns_alerts_published += 1

    def summary(self) -> Dict[str, Any]:
        """Return a consistent snapshot of accumulated experiment metrics."""
        with self._lock:
            latencies = list(self.latencies)
            return {
                "p50_latency_ms": _percentile(latencies, 0.50),
                "p95_latency_ms": _percentile(latencies, 0.95),
                "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0.0,
                "reordered_events": self.reordered_events,
                "estimated_lost_events": self.estimated_lost_events,
                "traffic_density_reports": self.traffic_density_reports,
                "sns_alerts_published": self.sns_alerts_published,
                "sector_density": {key: value.copy() for key, value in self.sector_density.items()},
            }

    def reset(self) -> None:
        """Clear event and alert measurements for a fresh experiment run."""
        with self._lock:
            self.latencies: List[float] = []
            self.sequence_by_source: Dict[str, int] = {}
            self.reordered_events = 0
            self.estimated_lost_events = 0
            self.traffic_density_reports = 0
            self.sns_alerts_published = 0
            self.sector_density: Dict[str, Dict[str, Any]] = {}


def build_summary(
    records: List[Dict[str, Any]],
    duplicates: int,
    experiment_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the dashboard state from stored event/alert records and metrics.

    Args:
        records: Persisted event records and any associated cloud alerts.
        duplicates: Number of duplicate event IDs ignored by the backend.
        experiment_metrics: Optional live metric snapshot to include in the result.

    Returns:
        A JSON-compatible summary with latest vehicle states, alerts, and events.
    """
    events = [V2XEvent.model_validate(record["event"]) for record in records]
    states = latest_states(events)
    sector_states: Dict[str, Dict[str, V2XEvent]] = {}
    for event in events:
        sector_states.setdefault(event.location, {})
        previous = sector_states[event.location].get(event.source_id)
        if previous is None or event_time(event) > event_time(previous):
            sector_states[event.location][event.source_id] = event

    sector_vehicles = baseline_sector_vehicles()
    for sector, vehicles in sector_states.items():
        sector_vehicles.setdefault(sector, {})
        for vehicle_id, event in vehicles.items():
            sector_vehicles[sector][vehicle_id] = event.model_dump(mode="json")

    alerts = [record["alert"] for record in records if record.get("alert")]
    events.sort(key=event_time, reverse=True)
    alerts.sort(key=lambda alert: alert["created_at"], reverse=True)
    corridor_event_ids = {
        record["event"]["event_id"] for record in records
        if record.get("alert") and record["alert"]["alert_type"] == "EMERGENCY_CORRIDOR_ALERT"
    }
    corridors = sorted({
        sector
        for sector, vehicles in sector_vehicles.items()
        if any(state.get("event_id") in corridor_event_ids for state in vehicles.values())
    })
    result = {
        "processed_events": len(events),
        "duplicates_ignored": duplicates,
        "active_emergency_corridors": corridors,
        "vehicles": {key: value.model_dump(mode="json") for key, value in states.items()},
        "sector_vehicles": sector_vehicles,
        "recent_alerts": alerts[:50],
        "recent_events": [event.model_dump(mode="json") for event in events[:50]],
    }
    result.update(experiment_metrics or {
        "p50_latency_ms": 0.0,
        "p95_latency_ms": 0.0,
        "avg_latency_ms": 0.0,
        "reordered_events": 0,
        "estimated_lost_events": 0,
        "traffic_density_reports": sum(event.event_type == "traffic_density" for event in events),
        "sns_alerts_published": 0,
        "sector_density": {},
    })
    return result


def make_record(event: V2XEvent, events: List[V2XEvent]) -> Dict[str, Any]:
    """Create a stored event/alert record using prior timestamp-ordered state."""
    previous_states = latest_states(events)
    previous = previous_states.get(event.source_id)
    states = latest_states(events + [event])
    entered = previous if states.get(event.source_id) is event else None
    alert = evaluate_event_for_alert(event, list(states.values()), entered)
    return {
        "event": event.model_dump(mode="json"),
        "alert": alert.model_dump(mode="json") if alert else None,
    }


class Storage(Protocol):
    """Define the idempotent storage interface shared by memory and DynamoDB."""

    def process_event(self, event: V2XEvent) -> Tuple[bool, Optional[V2XAlert]]:
        """Store a unique event and return whether it was new and its alert."""
        ...

    def summary(self) -> Dict[str, Any]:
        """Return dashboard state and monitoring metrics."""
        ...

    def record_sns_alert(self) -> None:
        """Record a successful cloud alert publication."""
        ...

    def reset(self) -> None:
        """Clear stored state and metrics."""
        ...


class InMemoryStorage:
    """Thread-safe in-process event store for local runs and controlled experiments."""

    def __init__(self) -> None:
        """Initialize records, duplicate tracking, and experiment metrics."""
        self._records: Dict[str, Dict[str, Any]] = {}
        self._duplicates = 0
        self._lock = threading.Lock()
        self._metrics = ExperimentMetrics()

    def process_event(self, event: V2XEvent) -> Tuple[bool, Optional[V2XAlert]]:
        """Persist a unique event once and return any rule-generated alert.

        Args:
            event: Validated event from direct HTTP ingestion or the SQS worker.

        Returns:
            ``(False, None)`` for a duplicate ID; otherwise ``(True, alert)`` where
            alert is ``None`` if no cloud rule matched.
        """
        with self._lock:
            if event.event_id in self._records:
                self._duplicates += 1
                return False, None
            events = [V2XEvent.model_validate(record["event"]) for record in self._records.values()]
            record = make_record(event, events)
            self._records[event.event_id] = record
            self._metrics.record_event(event)
            return True, V2XAlert.model_validate(record["alert"]) if record["alert"] else None

    def summary(self) -> Dict[str, Any]:
        """Return a dashboard summary with the current in-memory metrics."""
        with self._lock:
            return build_summary(
                list(self._records.values()), self._duplicates, self._metrics.summary()
            )

    def record_sns_alert(self) -> None:
        """Record a successfully published alert in the experiment metrics."""
        self._metrics.record_sns_alert()

    def reset(self) -> None:
        """Remove all event records and reset duplicate and experiment counters."""
        with self._lock:
            self._records.clear()
            self._duplicates = 0
            self._metrics.reset()


class DynamoDBStorage:
    """Persist idempotent event records in DynamoDB with conditional insertion."""

    def __init__(self, table_name: str, region: str = "us-east-1", table: Any = None) -> None:
        """Bind to an existing DynamoDB table or create a regional table resource.

        Args:
            table_name: Table holding event records and duplicate metrics.
            region: AWS region used when creating the DynamoDB resource.
            table: Optional compatible table object for tests or injection.
        """
        self.table = table if table is not None else boto3.resource("dynamodb", region_name=region).Table(table_name)
        self._metrics = ExperimentMetrics()

    def _records(self) -> List[Dict[str, Any]]:
        """Scan all stored event records with consistent reads and pagination."""
        records: List[Dict[str, Any]] = []
        params: Dict[str, Any] = {"ConsistentRead": True}
        while True:
            response = self.table.scan(**params)
            records.extend(
                json.loads(json.dumps(item, default=lambda value: float(value) if isinstance(value, Decimal) else value))
                for item in response.get("Items", []) if "event" in item
            )
            key = response.get("LastEvaluatedKey")
            if not key:
                return records
            params["ExclusiveStartKey"] = key

    def _count_duplicate(self) -> None:
        """Atomically increment the reserved DynamoDB duplicate counter."""
        self.table.update_item(
            Key={"event_id": METRICS_KEY},
            UpdateExpression="ADD duplicates_ignored :one",
            ExpressionAttributeValues={":one": 1},
        )

    def process_event(self, event: V2XEvent) -> Tuple[bool, Optional[V2XAlert]]:
        """Conditionally persist a unique event and compute its cloud alert.

        A conditional write makes event-ID idempotency safe across concurrent
        consumers; timestamp-based state selection keeps late arrivals from
        replacing newer vehicle state.
        """
        if self.table.get_item(Key={"event_id": event.event_id}, ConsistentRead=True).get("Item"):
            self._count_duplicate()
            return False, None
        records = self._records()
        events = [V2XEvent.model_validate(record["event"]) for record in records]
        record = make_record(event, events)
        # DynamoDB rejects floats; Decimal preserves JSON numeric values.
        item = json.loads(json.dumps({"event_id": event.event_id, **record}), parse_float=Decimal)
        try:
            self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(event_id)")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            self._count_duplicate()
            return False, None
        self._metrics.record_event(event)
        return True, V2XAlert.model_validate(record["alert"]) if record["alert"] else None

    def summary(self) -> Dict[str, Any]:
        """Return a timestamp-ordered dashboard summary from the DynamoDB table."""
        records = self._records()
        metrics = self.table.get_item(Key={"event_id": METRICS_KEY}, ConsistentRead=True).get("Item", {})
        summary = build_summary(
            records, int(metrics.get("duplicates_ignored", 0)), self._metrics.summary()
        )
        return json.loads(json.dumps(summary, default=lambda value: float(value) if isinstance(value, Decimal) else value))

    def record_sns_alert(self) -> None:
        """Record a successful SNS publication in process-local experiment metrics."""
        self._metrics.record_sns_alert()

    def reset(self) -> None:
        """Delete every table item, including the duplicate counter, and reset metrics."""
        params: Dict[str, Any] = {"ProjectionExpression": "event_id", "ConsistentRead": True}
        while True:
            response = self.table.scan(**params)
            for item in response.get("Items", []):
                self.table.delete_item(Key={"event_id": item["event_id"]})
            key = response.get("LastEvaluatedKey")
            if not key:
                break
            params["ExclusiveStartKey"] = key
        self._metrics.reset()
