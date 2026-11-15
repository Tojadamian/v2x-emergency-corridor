"""Validated V2X event and alert models shared by vehicles, RSUs, and cloud services.

The event and alert fields implement the assignment's wire contracts. ``EventType``,
``AlertType``, and ``AlertSeverity`` are literal vocabularies for event categories,
alert categories, and severity levels. Event types include routine telemetry, safety
events, and RSU traffic-density summaries; alert types distinguish cloud notifications
from fast local RSU warnings.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional
from uuid import uuid4
from pydantic import BaseModel, Field, field_validator, model_validator
import math

EventType = Literal[
    "traffic_telemetry",
    "sudden_braking",
    "emergency_vehicle",
    "road_hazard",
    "traffic_density",
]
AlertType = Literal[
    "EMERGENCY_CORRIDOR_ALERT",
    "SEVERE_BRAKING_CONGESTION_ALERT",
    "ROAD_HAZARD_ALERT",
    "LOCAL_EMERGENCY_CORRIDOR_ALERT",
    "LOCAL_COLLISION_RISK_ALERT",
    "LOCAL_ROAD_HAZARD_ALERT",
]
AlertSeverity = Literal["WARNING", "CRITICAL"]

class V2XEvent(BaseModel):
    """Validated event envelope carried on the V2X event path.

    Vehicle and RSU producers provide a source, event type, timestamp, location,
    and payload; optional RSU and sequence metadata supports edge routing and
    delivery-anomaly measurement.
    """

    event_id: str = Field(default_factory=lambda: f"evt-{uuid4().hex}", min_length=1)
    source_id: str = Field(..., min_length=1, description="Synthetic vehicle ID, e.g., veh-001")
    event_type: EventType
    event_time: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    location: str = Field(..., min_length=1, description="Road segment or RSU zone, e.g., sector-A12-tunnel")
    payload: Dict[str, Any] = Field(
        ..., description="Speed, lane, deceleration_ms2, density, or emergency attributes"
    )
    rsu_id: Optional[str] = None
    seq_num: Optional[int] = None

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, value: str) -> str:
        """Reject the reserved identifier used for cloud duplicate metrics."""
        if value == "__v2x_duplicates__":
            raise ValueError("event_id is reserved")
        return value

    @field_validator("event_time")
    @classmethod
    def validate_event_time(cls, value: str) -> str:
        """Require an ISO 8601 event timestamp that includes a timezone."""
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("event_time must be an ISO 8601 timestamp") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("event_time must include a timezone")
        return value

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        """Validate numeric telemetry and boolean siren fields without reshaping payloads."""
        for key in ("speed_kmh", "deceleration_ms2"):
            if key in value:
                number = value[key]
                if isinstance(number, bool) or not isinstance(number, (int, float)):
                    raise ValueError(f"{key} must be a non-negative number")
                if not math.isfinite(number) or number < 0:
                    raise ValueError(f"{key} must be a finite non-negative number")
        if "siren_active" in value and not isinstance(value["siren_active"], bool):
            raise ValueError("siren_active must be a boolean")
        return value

    @model_validator(mode="after")
    def validate_event_payload(self):
        """Require deceleration data for sudden-braking events."""
        if self.event_type == "sudden_braking" and "deceleration_ms2" not in self.payload:
            raise ValueError("sudden_braking requires deceleration_ms2")
        return self

class V2XAlert(BaseModel):
    """Cloud or RSU safety alert sent to subscribers and dashboard clients.

    The alert identifies its trigger, affected source and location, severity,
    recommended action, and whether it originated in the local edge loop or cloud.
    """

    alert_id: str = Field(default_factory=lambda: f"alt-{uuid4().hex}")
    alert_type: AlertType
    severity: AlertSeverity
    location: str
    triggered_by_event_id: str
    source_id: str
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    message: str
    action_instruction: str
    origin: str = "cloud"
    rsu_id: Optional[str] = None
