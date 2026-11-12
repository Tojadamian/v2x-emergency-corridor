from typing import Optional
from common.schemas import V2XEvent, V2XAlert

def evaluate_event_for_alert(event: V2XEvent) -> Optional[V2XAlert]:
    """Evaluates an incoming V2XEvent and returns a V2XAlert if safety conditions are met."""
    
    # 1. Emergency Corridor Priority Rule
    if event.event_type == "emergency_vehicle" and event.payload.get("siren_active", True):
        speed = event.payload.get("speed_kmh", 90)
        return V2XAlert(
            alert_type="EMERGENCY_CORRIDOR_ALERT",
            severity="CRITICAL",
            location=event.location,
            triggered_by_event_id=event.event_id,
            source_id=event.source_id,
            message=f"Emergency vehicle ({event.source_id}) approaching {event.location} at {speed} km/h!",
            action_instruction="FORM EMERGENCY CORRIDOR: Lane 1 pull LEFT, Lanes 2 & 3 pull RIGHT immediately."
        )

    # 2. Sudden Braking / Congestion Hazard Rule
    if event.event_type == "sudden_braking":
        decel = float(event.payload.get("deceleration_ms2", 0.0))
        if decel >= 6.0:
            return V2XAlert(
                alert_type="SEVERE_BRAKING_CONGESTION_ALERT",
                severity="WARNING",
                location=event.location,
                triggered_by_event_id=event.event_id,
                source_id=event.source_id,
                message=f"Harsh braking ({decel} m/s²) detected by {event.source_id} in {event.location}.",
                action_instruction="REDUCE SPEED to 40 km/h and increase following distance."
            )

    return None
