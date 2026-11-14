from typing import Optional, Sequence
from common.schemas import V2XEvent, V2XAlert

def evaluate_event_for_alert(
    event: V2XEvent, vehicle_states: Sequence[V2XEvent] = (),
    previous_vehicle_state: Optional[V2XEvent] = None,
) -> Optional[V2XAlert]:
    """Evaluates an incoming V2XEvent and returns a V2XAlert if safety conditions are met."""

    if event.event_type == "road_hazard":
        return V2XAlert(
            alert_type="ROAD_HAZARD_ALERT",
            severity="WARNING",
            location=event.location,
            triggered_by_event_id=event.event_id,
            source_id=event.source_id,
            message=f"Road hazard reported in {event.location}; reduce speed and proceed with caution.",
            action_instruction="REDUCE SPEED and proceed with caution through this sector.",
            origin="cloud",
        )
    
    # 1. Emergency Corridor Priority Rule
    entered_active_segment = (
        event.payload.get("vehicle_type") == "ambulance"
        and previous_vehicle_state is not None
        and previous_vehicle_state.location != event.location
        and any(state.source_id != event.source_id and state.location == event.location for state in vehicle_states)
    )
    if (event.event_type == "emergency_vehicle" and event.payload.get("siren_active", True)) or entered_active_segment:
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
    decel = event.payload.get("deceleration_ms2", 0.0)
    speeds = [
        state.payload["speed_kmh"]
        for state in vehicle_states
        if state.location == event.location and "speed_kmh" in state.payload
    ]
    severe_braking = event.event_type == "sudden_braking" and decel >= 6.0
    congestion = bool(speeds) and sum(speeds) / len(speeds) < 25.0
    if severe_braking or congestion:
        message = (
            f"Harsh braking ({decel} m/s²) detected by {event.source_id} in {event.location}."
            if severe_braking
            else f"Average speed in {event.location} is {sum(speeds) / len(speeds):.1f} km/h."
        )
        return V2XAlert(
                alert_type="SEVERE_BRAKING_CONGESTION_ALERT",
                severity="WARNING",
                location=event.location,
                triggered_by_event_id=event.event_id,
                source_id=event.source_id,
                message=message,
                action_instruction="REDUCE SPEED to 40 km/h and increase following distance."
        )

    return None
