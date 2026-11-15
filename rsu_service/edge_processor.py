"""RSU edge processing for local safety alerts and resilient cloud forwarding."""

import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from common.schemas import V2XEvent, V2XAlert


class EdgeProcessor:
    """Implement the RSU fast edge loop, telemetry aggregation, and outage recovery.

    Incoming validated events are deduplicated and sequence-tracked; safety events
    can produce local alerts immediately, while routine telemetry is summarized
    and cloud-bound events are buffered in FIFO order during a disconnection.
    """

    def __init__(
        self,
        rsu_id: str = "rsu-1",
        assigned_sector: str = "sector-A12-tunnel",
        aggregation_window_size: int = 5,
        buffer_max_size: int = 1000,
    ):
        """Initialize per-RSU counters and bounded event, telemetry, and alert state.

        Args:
            rsu_id: Identifier attached to local summaries and alerts.
            assigned_sector: Road segment represented by this RSU.
            aggregation_window_size: Number of telemetry samples per summary.
            buffer_max_size: Maximum number of forwarded events retained offline.
        """
        self.rsu_id = rsu_id
        self.assigned_sector = assigned_sector
        self.aggregation_window_size = aggregation_window_size
        self.buffer_max_size = buffer_max_size

        self.received_events = 0
        self.forwarded_events = 0
        self.filtered_telemetry_count = 0
        self.duplicates_ignored = 0
        self.reordered_events = 0
        self.estimated_lost_events = 0
        self.buffered_count = 0
        self.rejected_events = 0
        self.flushed_events_count = 0
        self.last_recovery_time_ms = 0
        self.cloud_disconnected = False
        self.local_alerts_count = 0
        self.relayed_cloud_alerts_count = 0

        self.seen_event_ids: set = set()
        self.vehicle_seq_tracking: Dict[str, Tuple[int, str]] = {}
        self.telemetry_buffer: List[V2XEvent] = []
        self.buffered_events: List[V2XEvent] = []
        self.recent_local_alerts: List[Dict[str, Any]] = []
        self.recent_relayed_alerts: List[Dict[str, Any]] = []

    def process_event(self, event: V2XEvent) -> Tuple[Optional[V2XEvent], Optional[V2XAlert]]:
        """Process one event through deduplication, edge rules, and forwarding policy.

        Args:
            event: Validated vehicle or RSU event received at this edge.

        Returns:
            A pair of the event selected for cloud forwarding and any immediate
            local alert. Either value may be ``None`` when no item is emitted.
        """
        self.received_events += 1

        if event.event_id in self.seen_event_ids:
            self.duplicates_ignored += 1
            return None, None

        self.seen_event_ids.add(event.event_id)

        local_alert = None
        event_to_forward = None

        # Track sequence for all event types
        self._track_sequence(event)

        if event.event_type in ("emergency_vehicle", "sudden_braking", "road_hazard"):
            event_to_forward = event
            local_alert = self._check_local_alert(event)
        elif event.event_type == "traffic_telemetry":
            self.telemetry_buffer.append(event)
            if len(self.telemetry_buffer) >= self.aggregation_window_size:
                event_to_forward = self._aggregate_telemetry()
            else:
                self.filtered_telemetry_count += 1
        elif event.event_type == "traffic_density":
            event_to_forward = event
        else:
            event_to_forward = event

        if event_to_forward:
            self.forwarded_events += 1

        return event_to_forward, local_alert

    def _track_sequence(self, event: V2XEvent) -> None:
        """Update per-vehicle sequence state and estimate gaps or timestamp reordering."""
        if event.seq_num is None:
            return

        vehicle_id = event.source_id
        event_time_str = event.event_time

        if vehicle_id not in self.vehicle_seq_tracking:
            self.vehicle_seq_tracking[vehicle_id] = (event.seq_num, event_time_str)
            return

        last_seq, last_time = self.vehicle_seq_tracking[vehicle_id]

        try:
            current_time = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
            last_time_dt = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
            
            if current_time < last_time_dt:
                self.reordered_events += 1
        except (ValueError, TypeError):
            pass

        if event.seq_num > last_seq + 1:
            gap = event.seq_num - last_seq - 1
            self.estimated_lost_events += gap

        self.vehicle_seq_tracking[vehicle_id] = (event.seq_num, event_time_str)

    def _check_local_alert(self, event: V2XEvent) -> Optional[V2XAlert]:
        """Evaluate emergency, severe-braking, and hazard rules in the fast edge loop.

        Args:
            event: Event to evaluate without waiting for cloud processing.

        Returns:
            A local alert for a matched safety condition, otherwise ``None``.
        """
        alert = None

        if event.event_type == "emergency_vehicle" and event.payload.get("siren_active"):
            alert = V2XAlert(
                alert_type="LOCAL_EMERGENCY_CORRIDOR_ALERT",
                severity="CRITICAL",
                location=event.location,
                triggered_by_event_id=event.event_id,
                source_id=event.source_id,
                message=f"Emergency vehicle {event.source_id} detected with active siren in {event.location}",
                action_instruction="Vehicles on LEFT_LANE move left, MIDDLE and RIGHT_LANE move right",
                origin="local",
                rsu_id=self.rsu_id,
            )
            self.local_alerts_count += 1

        elif event.event_type == "sudden_braking" and event.payload.get("deceleration_ms2", 0) >= 6.0:
            alert = V2XAlert(
                alert_type="LOCAL_COLLISION_RISK_ALERT",
                severity="CRITICAL",
                location=event.location,
                triggered_by_event_id=event.event_id,
                source_id=event.source_id,
                message=f"Severe braking detected on vehicle {event.source_id} with deceleration {event.payload['deceleration_ms2']} m/s²",
                action_instruction="Reduce speed, increase following distance, prepare for potential collision",
                origin="local",
                rsu_id=self.rsu_id,
            )
            self.local_alerts_count += 1

        elif event.event_type == "road_hazard":
            alert = V2XAlert(
                alert_type="LOCAL_ROAD_HAZARD_ALERT",
                severity="WARNING",
                location=event.location,
                triggered_by_event_id=event.event_id,
                source_id=event.source_id,
                message=f"Road hazard detected: {event.payload.get('hazard_type', 'unknown')}",
                action_instruction="Reduce speed, exercise caution, avoid hazard area",
                origin="local",
                rsu_id=self.rsu_id,
            )
            self.local_alerts_count += 1

        if alert:
            self.recent_local_alerts.append(alert.model_dump(mode="json"))
            if len(self.recent_local_alerts) > 50:
                self.recent_local_alerts.pop(0)

        return alert

    def _aggregate_telemetry(self) -> V2XEvent:
        """Convert the current count-window of telemetry into one density summary.

        The summary reports sample and vehicle counts, available average speed, and
        per-vehicle details; the consumed telemetry buffer is cleared.
        """
        if not self.telemetry_buffer:
            return None

        speeds = [e.payload.get("speed_kmh", 0) for e in self.telemetry_buffer if "speed_kmh" in e.payload]
        avg_speed = sum(speeds) / len(speeds) if speeds else 0
        vehicle_count = len(self.telemetry_buffer)
        sample_count = len(self.telemetry_buffer)

        vehicles_info = [
            {
                "vehicle_id": e.source_id,
                "speed_kmh": e.payload.get("speed_kmh"),
                "lane": e.payload.get("lane"),
            }
            for e in self.telemetry_buffer
        ]

        aggregated_event = V2XEvent(
            source_id=self.rsu_id,
            event_type="traffic_density",
            location=self.assigned_sector,
            payload={
                "avg_speed_kmh": avg_speed,
                "vehicle_count": vehicle_count,
                "sample_count": sample_count,
                "vehicles": vehicles_info,
            },
            rsu_id=self.rsu_id,
        )

        self.telemetry_buffer.clear()
        return aggregated_event

    def buffer_event(self, event: V2XEvent) -> bool:
        """Append a cloud-bound event to the bounded FIFO outage buffer.

        Args:
            event: Event to retain until cloud connectivity is restored.

        Returns:
            ``True`` if the event was queued, or ``False`` if capacity was reached.
        """
        if self.buffered_count >= self.buffer_max_size:
            self.rejected_events += 1
            return False
        self.buffered_events.append(event)
        self.buffered_count += 1
        return True

    def flush_buffer(self) -> List[V2XEvent]:
        """Drain all buffered events in arrival order and record recovery metrics.

        Returns:
            The FIFO batch to retry against the cloud event path.
        """
        start_time = time.perf_counter()
        events = list(self.buffered_events)
        self.buffered_events.clear()
        self.flushed_events_count = len(events)
        self.last_recovery_time_ms = max(1, int((time.perf_counter() - start_time) * 1000))
        self.buffered_count = 0
        return events

    def relay_cloud_alert(self, alert: V2XAlert) -> None:
        """Record a cloud alert relayed through this RSU for edge subscribers.

        Args:
            alert: Validated alert received from the cloud alert path.
        """
        self.relayed_cloud_alerts_count += 1
        self.recent_relayed_alerts.append(alert.model_dump(mode="json"))
        if len(self.recent_relayed_alerts) > 50:
            self.recent_relayed_alerts.pop(0)

    def get_status(self) -> Dict[str, Any]:
        """Return RSU identity, edge counters, connectivity, and recent alerts.

        Returns:
            A JSON-compatible snapshot used by the cloud dashboard and experiments.
        """
        return {
            "rsu_id": self.rsu_id,
            "sector": self.assigned_sector,
            "received_events": self.received_events,
            "forwarded_events": self.forwarded_events,
            "filtered_telemetry_count": self.filtered_telemetry_count,
            "duplicates_ignored": self.duplicates_ignored,
            "reordered_events": self.reordered_events,
            "estimated_lost_events": self.estimated_lost_events,
            "buffered_count": self.buffered_count,
            "rejected_events": self.rejected_events,
            "flushed_events_count": self.flushed_events_count,
            "last_recovery_time_ms": self.last_recovery_time_ms,
            "cloud_disconnected": self.cloud_disconnected,
            "local_alerts_count": self.local_alerts_count,
            "relayed_cloud_alerts_count": self.relayed_cloud_alerts_count,
            "recent_local_alerts": self.recent_local_alerts[-10:],
            "recent_relayed_alerts": self.recent_relayed_alerts[-10:],
        }
