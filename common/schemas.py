from datetime import datetime, timezone
from typing import Any, Dict, Literal
from uuid import uuid4
from pydantic import BaseModel, Field

EventType = Literal["traffic_telemetry", "sudden_braking", "emergency_vehicle"]

class V2XEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: f"evt-{uuid4().hex[:8]}")
    source_id: str = Field(..., description="Synthetic vehicle ID, e.g., veh-001")
    event_type: EventType
    event_time: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    location: str = Field(..., description="Road segment or RSU zone, e.g., sector-A12-tunnel")
    payload: Dict[str, Any] = Field(
        ..., description="Speed, lane, deceleration_ms2, density, or emergency attributes"
    )

class V2XAlert(BaseModel):
    alert_id: str = Field(default_factory=lambda: f"alt-{uuid4().hex[:8]}")
    alert_type: Literal["EMERGENCY_CORRIDOR_ALERT", "SEVERE_BRAKING_CONGESTION_ALERT"]
    severity: Literal["WARNING", "CRITICAL"]
    location: str
    triggered_by_event_id: str
    source_id: str
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    message: str
    action_instruction: str
