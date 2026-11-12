import argparse
import os
import random
import sys
import time
import urllib.request
from pathlib import Path

import boto3

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.schemas import V2XEvent


VEHICLES = [
    ("veh-001", "car", "sector-A12-tunnel"),
    ("veh-002", "truck", "sector-A12-tunnel"),
    ("veh-003", "car", "sector-B04-bridge"),
    ("veh-004", "truck", "sector-B04-bridge"),
    ("veh-005", "ambulance", "sector-A12-tunnel"),
]


def generate_event(vehicle):
    vehicle_id, vehicle_type, location = vehicle
    event_type = random.choices(
        ["traffic_telemetry", "sudden_braking", "emergency_vehicle"]
        if vehicle_type == "ambulance" else ["traffic_telemetry", "sudden_braking"],
        weights=[7, 1, 2] if vehicle_type == "ambulance" else [9, 1],
    )[0]
    payload = {
        "vehicle_type": vehicle_type,
        "speed_kmh": random.randint(20, 100),
        "lane": random.randint(1, 3),
        "traffic_density": random.choice(["low", "medium", "high"]),
    }
    if event_type == "sudden_braking":
        payload["deceleration_ms2"] = round(random.uniform(4.0, 9.0), 1)
    if event_type == "emergency_vehicle":
        payload.update(siren_active=True, target_lane=2)
    return V2XEvent(source_id=vehicle_id, event_type=event_type, location=location, payload=payload)


def main():
    parser = argparse.ArgumentParser(description="Publish simulated V2X vehicle events")
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second across all vehicles")
    parser.add_argument("--mode", choices=("sqs", "http"), default="sqs")
    parser.add_argument("--queue-url", default="", help="SQS queue URL (or SQS_QUEUE_URL environment variable)")
    parser.add_argument("--http-url", default="http://127.0.0.1:8000/api/events/ingest")
    args = parser.parse_args()
    if not 0 < args.rate < float("inf"):
        parser.error("--rate must be a positive finite number")
    queue_url = args.queue_url or os.getenv("SQS_QUEUE_URL", "")
    if args.mode == "sqs" and not queue_url:
        parser.error("--queue-url or SQS_QUEUE_URL is required in SQS mode")
    client = boto3.client("sqs", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")) if args.mode == "sqs" else None
    try:
        index = 0
        while True:
            event = generate_event(VEHICLES[index % len(VEHICLES)])
            body = event.model_dump_json()
            if client:
                client.send_message(QueueUrl=queue_url, MessageBody=body)
            else:
                request = urllib.request.Request(
                    args.http_url, data=body.encode(), headers={"Content-Type": "application/json"}, method="POST"
                )
                with urllib.request.urlopen(request, timeout=10) as response:
                    response.read()
            print(f"Sent {event.event_type} from {event.source_id} ({event.event_id})", flush=True)
            index += 1
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("\nSimulator stopped.")


if __name__ == "__main__":
    main()
