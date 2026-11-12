import json
import threading
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Protocol, Tuple

import boto3
from botocore.exceptions import ClientError

from common.schemas import V2XAlert, V2XEvent
from v2x_service.rules import evaluate_event_for_alert

METRICS_KEY = "__v2x_duplicates__"


def event_time(event: V2XEvent) -> datetime:
    return datetime.fromisoformat(event.event_time.replace("Z", "+00:00"))


def latest_states(events: List[V2XEvent]) -> Dict[str, V2XEvent]:
    states: Dict[str, V2XEvent] = {}
    for event in events:
        previous = states.get(event.source_id)
        if previous is None or event_time(event) > event_time(previous):
            states[event.source_id] = event
    return states


def build_summary(records: List[Dict[str, Any]], duplicates: int) -> Dict[str, Any]:
    events = [V2XEvent.model_validate(record["event"]) for record in records]
    states = latest_states(events)
    alerts = [record["alert"] for record in records if record.get("alert")]
    events.sort(key=event_time, reverse=True)
    alerts.sort(key=lambda alert: alert["created_at"], reverse=True)
    corridor_event_ids = {
        record["event"]["event_id"] for record in records
        if record.get("alert") and record["alert"]["alert_type"] == "EMERGENCY_CORRIDOR_ALERT"
    }
    corridors = sorted({state.location for state in states.values() if state.event_id in corridor_event_ids})
    return {
        "processed_events": len(events),
        "duplicates_ignored": duplicates,
        "active_emergency_corridors": corridors,
        "vehicles": {key: value.model_dump(mode="json") for key, value in states.items()},
        "recent_alerts": alerts[:50],
        "recent_events": [event.model_dump(mode="json") for event in events[:50]],
    }


def make_record(event: V2XEvent, events: List[V2XEvent]) -> Dict[str, Any]:
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
    def process_event(self, event: V2XEvent) -> Tuple[bool, Optional[V2XAlert]]: ...
    def summary(self) -> Dict[str, Any]: ...


class InMemoryStorage:
    def __init__(self) -> None:
        self._records: Dict[str, Dict[str, Any]] = {}
        self._duplicates = 0
        self._lock = threading.Lock()

    def process_event(self, event: V2XEvent) -> Tuple[bool, Optional[V2XAlert]]:
        with self._lock:
            if event.event_id in self._records:
                self._duplicates += 1
                return False, None
            events = [V2XEvent.model_validate(record["event"]) for record in self._records.values()]
            record = make_record(event, events)
            self._records[event.event_id] = record
            return True, V2XAlert.model_validate(record["alert"]) if record["alert"] else None

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            return build_summary(list(self._records.values()), self._duplicates)


class DynamoDBStorage:
    def __init__(self, table_name: str, region: str = "us-east-1", table: Any = None) -> None:
        self.table = table if table is not None else boto3.resource("dynamodb", region_name=region).Table(table_name)

    def _records(self) -> List[Dict[str, Any]]:
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
        self.table.update_item(
            Key={"event_id": METRICS_KEY},
            UpdateExpression="ADD duplicates_ignored :one",
            ExpressionAttributeValues={":one": 1},
        )

    def process_event(self, event: V2XEvent) -> Tuple[bool, Optional[V2XAlert]]:
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
        return True, V2XAlert.model_validate(record["alert"]) if record["alert"] else None

    def summary(self) -> Dict[str, Any]:
        records = self._records()
        metrics = self.table.get_item(Key={"event_id": METRICS_KEY}, ConsistentRead=True).get("Item", {})
        summary = build_summary(records, int(metrics.get("duplicates_ignored", 0)))
        return json.loads(json.dumps(summary, default=lambda value: float(value) if isinstance(value, Decimal) else value))
