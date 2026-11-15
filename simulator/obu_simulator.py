"""Generate controlled OBU traffic and transmit it over V2X transport modes.

The simulator assigns vehicles to RSUs, attaches per-vehicle sequence numbers,
and can inject delivery anomalies before sending events by SQS, HTTP, RSU HTTP,
or MQTT. MQTT uses the configured publish QoS and subscribes to RSU alert topics.
"""

import argparse
from datetime import datetime, timedelta, timezone
from functools import partial
import json
import math
import os
import random
import sys
import time
import urllib.request
from collections.abc import Callable, MutableMapping
from pathlib import Path
from typing import Optional

import boto3

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.schemas import V2XEvent


VEHICLES = [
    ("veh-001", "car", "sector-A12-tunnel", "rsu-1"),
    ("veh-002", "truck", "sector-A12-tunnel", "rsu-1"),
    ("veh-003", "car", "sector-A12-tunnel", "rsu-1"),
    ("veh-004", "truck", "sector-B04-bridge", "rsu-2"),
    ("veh-005", "ambulance", "sector-B04-bridge", "rsu-2"),
]
_vehicle_sequences: dict[str, int] = {}
received_alerts = 0


def generate_event(
    vehicle,
    sequence_numbers: Optional[MutableMapping[str, int]] = None,
    rng=random,
) -> V2XEvent:
    """Generate one typed vehicle event with routing and sequence metadata.

    Args:
        vehicle: Vehicle tuple containing ID, type, sector, and optional RSU ID.
        sequence_numbers: Mutable per-vehicle sequence state; global state is used
            when omitted.
        rng: Random source used for reproducible event type and payload generation.

    Returns:
        A validated V2X event for simulator transport or experiment injection.
    """
    vehicle_id, vehicle_type, location = vehicle[:3]
    rsu_id = vehicle[3] if len(vehicle) > 3 else (
        "rsu-2" if location == "sector-B04-bridge" else "rsu-1"
    )
    sequences = sequence_numbers if sequence_numbers is not None else _vehicle_sequences
    seq_num = sequences.get(vehicle_id, 0) + 1
    sequences[vehicle_id] = seq_num

    event_types = (
        ["traffic_telemetry", "sudden_braking", "emergency_vehicle", "road_hazard"]
        if vehicle_type == "ambulance"
        else ["traffic_telemetry", "sudden_braking", "road_hazard"]
    )
    weights = [7, 1, 2, 1] if vehicle_type == "ambulance" else [9, 1, 1]
    event_type = rng.choices(event_types, weights=weights, k=1)[0]
    payload = {
        "vehicle_type": vehicle_type,
        "speed_kmh": rng.randint(20, 100),
        "lane": rng.randint(1, 3),
        "traffic_density": rng.choice(["low", "medium", "high"]),
    }
    if event_type == "sudden_braking":
        payload["deceleration_ms2"] = round(rng.uniform(4.0, 9.0), 1)
    if event_type == "emergency_vehicle":
        payload.update(siren_active=True, target_lane=2)
    if event_type == "road_hazard":
        payload["hazard_type"] = rng.choice(["debris", "pothole", "obstruction"])
    return V2XEvent(
        source_id=vehicle_id,
        event_type=event_type,
        location=location,
        payload=payload,
        rsu_id=rsu_id,
        seq_num=seq_num,
    )


def generate_burst(
    burst_size: int,
    vehicle_index: int = 0,
    sequence_numbers: Optional[MutableMapping[str, int]] = None,
    rng=random,
    roam_sectors: bool = False,
) -> tuple[list[V2XEvent], int]:
    """Generate a rotating batch of events and return the next vehicle index.

    Args:
        burst_size: Number of events to generate; must be positive.
        vehicle_index: Starting index in the configured vehicle list.
        sequence_numbers: Optional sequence state shared across generated bursts.
        rng: Random source for deterministic or standard simulation.
        roam_sectors: Whether even-numbered events alternate to the other RSU.

    Returns:
        A pair containing generated events and the next starting vehicle index.
    """
    if burst_size < 1:
        raise ValueError("burst_size must be at least 1")
    events = []
    for offset in range(burst_size):
        event = generate_event(
            VEHICLES[(vehicle_index + offset) % len(VEHICLES)],
            sequence_numbers=sequence_numbers,
            rng=rng,
        )
        if roam_sectors and event.seq_num % 2 == 0:
            event.location, event.rsu_id = (
                ("sector-B04-bridge", "rsu-2")
                if event.location == "sector-A12-tunnel"
                else ("sector-A12-tunnel", "rsu-1")
            )
        events.append(event)
    return events, (vehicle_index + burst_size) % len(VEHICLES)


class AnomalyInjector:
    """Apply controlled delivery anomalies before transport to the receiver.

    Delay, loss, duplication, and reordering controls support reproducible
    delivery-anomaly experiments; ``flush`` releases any held reordered events.
    """

    def __init__(
        self,
        delay_ms: int = 0,
        loss_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        reorder_rate: float = 0.0,
        rng=random,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Configure anomaly probabilities and the clock/sleep dependencies.

        Args:
            delay_ms: Simulated event-time delay and requested sleep duration.
            loss_rate: Probability that an event is dropped.
            duplicate_rate: Probability that a transmitted event is sent twice.
            reorder_rate: Probability that an event is held for later reordering.
            rng: Random source controlling anomaly decisions.
            sleep: Injectable sleep function for real or virtual delay.

        Raises:
            ValueError: If delay is negative or a probability is outside [0, 1].
        """
        if delay_ms < 0:
            raise ValueError("delay_ms must be non-negative")
        for name, value in (
            ("loss_rate", loss_rate),
            ("duplicate_rate", duplicate_rate),
            ("reorder_rate", reorder_rate),
        ):
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0.0 and 1.0")
        self.delay_ms = delay_ms
        self.loss_rate = loss_rate
        self.duplicate_rate = duplicate_rate
        self.reorder_rate = reorder_rate
        self.rng = rng
        self.sleep = sleep
        self.held_events: dict[str, V2XEvent] = {}

    def _send(self, event: V2XEvent, sender: Callable[[V2XEvent], None]) -> int:
        """Send an event once and apply the configured duplicate decision."""
        sender(event)
        count = 1
        if self.rng.random() < self.duplicate_rate:
            sender(event)
            count += 1
        return count

    def transmit(self, event: V2XEvent, sender: Callable[[V2XEvent], None]) -> int:
        """Apply loss, delay, duplication, and reordering before invoking a sender.

        Args:
            event: Event to transmit or hold for a later out-of-order delivery.
            sender: Transport callback for SQS, HTTP, or MQTT delivery.

        Returns:
            Number of sender calls made for this event; held or dropped events
            return zero until or unless they are flushed.
        """
        if self.rng.random() < self.loss_rate:
            return 0
        if self.delay_ms:
            original_time = datetime.fromisoformat(event.event_time.replace("Z", "+00:00"))
            event.event_time = (
                original_time - timedelta(milliseconds=self.delay_ms)
            ).astimezone(timezone.utc).isoformat()
            self.sleep(self.delay_ms / 1000)

        held = self.held_events.get(event.source_id)
        if held is not None and event.seq_num is not None and held.seq_num is not None:
            if event.seq_num > held.seq_num:
                del self.held_events[event.source_id]
                return self._send(event, sender) + self._send(held, sender)
        if self.rng.random() < self.reorder_rate:
            self.held_events[event.source_id] = event
            return 0
        return self._send(event, sender)

    def flush(self, sender: Callable[[V2XEvent], None]) -> int:
        """Send all events still held for reordering and return send count."""
        held_events = list(self.held_events.values())
        self.held_events.clear()
        return sum(self._send(event, sender) for event in held_events)


def _http_post(url: str, event: V2XEvent) -> None:
    """POST an event's JSON representation to a cloud or RSU HTTP endpoint."""
    request = urllib.request.Request(
        url,
        data=event.model_dump_json().encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def _mqtt_alert_callback(client, userdata, message) -> None:
    """Count and print a decoded alert received on an RSU MQTT topic."""
    global received_alerts
    try:
        alert = json.loads(message.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        print(f"Received invalid alert payload on {message.topic}", flush=True)
        return
    if not isinstance(alert, dict):
        print(f"Received invalid alert payload on {message.topic}", flush=True)
        return
    received_alerts += 1
    print(
        f"Received alert #{received_alerts} on {message.topic}: "
        f"{alert.get('alert_type', 'unknown')} at {alert.get('location', 'unknown')} "
        f"({alert.get('origin', 'unknown')})",
        flush=True,
    )


def _mqtt_connect_callback(client, userdata, flags, reason_code, properties, *, qos=1) -> None:
    """Subscribe the simulator to RSU alert topics at the configured MQTT QoS."""
    client.subscribe("v2x/+/alerts/#", qos=qos)


def main() -> None:
    """Parse simulator options and run the selected transport/anomaly workload.

    The command-line entry point controls event rate, burst generation, optional
    sector roaming, anomaly probabilities, and SQS/HTTP/RSU-HTTP/MQTT transport.
    """
    parser = argparse.ArgumentParser(description="Publish simulated V2X vehicle events")
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second")
    parser.add_argument("--mode", choices=("sqs", "http", "mqtt", "rsu-http"), default="sqs")
    parser.add_argument("--queue-url", default="", help="SQS queue URL (or SQS_QUEUE_URL environment variable)")
    parser.add_argument("--http-url", default="http://127.0.0.1:8000/api/events/ingest")
    parser.add_argument("--mqtt-host", default="127.0.0.1")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--mqtt-qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--rsu1-url", default="http://127.0.0.1:8001")
    parser.add_argument("--rsu2-url", default="http://127.0.0.1:8002")
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--loss-rate", type=float, default=0.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--reorder-rate", type=float, default=0.0)
    parser.add_argument("--burst-size", type=int, default=1)
    parser.add_argument("--roam-sectors", action="store_true", help="Alternate each vehicle between the two sectors")
    parser.add_argument("--iterations", type=int, default=0, help="Burst cycles; zero runs indefinitely")
    args = parser.parse_args()

    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be a positive finite number")
    if args.delay_ms < 0:
        parser.error("--delay-ms must be non-negative")
    if args.burst_size < 1:
        parser.error("--burst-size must be at least 1")
    if args.iterations < 0:
        parser.error("--iterations must be non-negative")
    for name in ("loss_rate", "duplicate_rate", "reorder_rate"):
        value = getattr(args, name)
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            parser.error(f"--{name.replace('_', '-')} must be between 0.0 and 1.0")

    queue_url = args.queue_url or os.getenv("SQS_QUEUE_URL", "")
    if args.mode == "sqs" and not queue_url:
        parser.error("--queue-url or SQS_QUEUE_URL is required in SQS mode")

    sqs_client = (
        boto3.client("sqs", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        if args.mode == "sqs"
        else None
    )
    mqtt = None
    mqtt_client = None
    if args.mode == "mqtt":
        import paho.mqtt.client as mqtt

        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client.on_connect = partial(_mqtt_connect_callback, qos=args.mqtt_qos)
        mqtt_client.on_message = _mqtt_alert_callback
        mqtt_client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
        mqtt_client.loop_start()

    def send_event(event: V2XEvent) -> None:
        """Send an event through the selected SQS, MQTT, or HTTP transport."""
        body = event.model_dump_json()
        if sqs_client:
            sqs_client.send_message(QueueUrl=queue_url, MessageBody=body)
        elif args.mode == "mqtt":
            result = mqtt_client.publish(f"v2x/{event.rsu_id}/telemetry", body, qos=args.mqtt_qos)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"MQTT publish failed with result code {result.rc}")
        elif args.mode == "rsu-http":
            base_url = args.rsu1_url if event.rsu_id == "rsu-1" else args.rsu2_url
            _http_post(f"{base_url.rstrip('/')}/api/events", event)
        else:
            _http_post(args.http_url, event)

    injector = AnomalyInjector(
        delay_ms=args.delay_ms,
        loss_rate=args.loss_rate,
        duplicate_rate=args.duplicate_rate,
        reorder_rate=args.reorder_rate,
    )
    cycles = 0
    vehicle_index = 0
    transmitted = 0
    try:
        while args.iterations == 0 or cycles < args.iterations:
            burst, vehicle_index = generate_burst(args.burst_size, vehicle_index, roam_sectors=args.roam_sectors)
            for event in burst:
                transmitted += injector.transmit(event, send_event)
                print(
                    f"Generated {event.event_type} from {event.source_id} "
                    f"({event.event_id}, seq={event.seq_num}, rsu={event.rsu_id})",
                    flush=True,
                )
                time.sleep(1 / args.rate)
            cycles += 1
        transmitted += injector.flush(send_event)
    except KeyboardInterrupt:
        transmitted += injector.flush(send_event)
        print("Simulator stopped.", flush=True)
    finally:
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
    print(f"Transmitted events: {transmitted}; received_alerts: {received_alerts}", flush=True)


if __name__ == "__main__":
    main()
