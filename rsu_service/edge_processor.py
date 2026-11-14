import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from common.schemas import V2XEvent, V2XAlert


class EdgeProcessor:
    """RSU edge processing for local validation, aggregation, buffering, and alert generation."""

    def __init__(
        self,
        rsu_id: str = "rsu-1",
        assigned_sector: str = "sector-A12-tunnel",
        aggregation_window_size: int = 5,
        buffer_max_size: int = 1000,
    ):
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
        """
        Process incoming event:
        - Validate and check for duplicates
        - Track sequence numbers
        - Generate local alerts if critical
        - Filter/aggregate telemetry or forward critical events
        
        Returns (forwarded_event, local_alert) or (None, None) if duplicate/ignored
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
        """Track sequence numbers for out-of-order and lost packet detection."""
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
        """Check if event triggers a local alert."""
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
        """Aggregate buffered telemetry events into a traffic_density event."""
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
        """Buffer event for offline storage. Returns False if buffer full."""
        if self.buffered_count >= self.buffer_max_size:
            self.rejected_events += 1
            return False
        self.buffered_events.append(event)
        self.buffered_count += 1
        return True

    def flush_buffer(self) -> List[V2XEvent]:
        """Flush all buffered events and return them in order."""
        events = list(self.buffered_events)
        self.buffered_events.clear()
        self.flushed_events_count = len(events)
        self.last_recovery_time_ms = int(time.time() * 1000)
        self.buffered_count = 0
        return events

    def relay_cloud_alert(self, alert: V2XAlert) -> None:
        """Record a relayed cloud alert."""
        self.relayed_cloud_alerts_count += 1
        self.recent_relayed_alerts.append(alert.model_dump(mode="json"))
        if len(self.recent_relayed_alerts) > 50:
            self.recent_relayed_alerts.pop(0)

    def get_status(self) -> Dict[str, Any]:
        """Return complete RSU status."""
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
