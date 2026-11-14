import json
import os
from typing import Optional

import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common.schemas import V2XEvent, V2XAlert
from rsu_service.edge_processor import EdgeProcessor

app = FastAPI(title="V2X RSU Edge Service")

rsu_id = os.getenv("RSU_ID", "rsu-1")
assigned_sector = os.getenv("ASSIGNED_SECTOR", "sector-A12-tunnel")
mqtt_broker_host = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
mqtt_broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
enable_mqtt = os.getenv("ENABLE_MQTT", "false").lower() == "true"
sqs_queue_url = os.getenv("SQS_QUEUE_URL", "")
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
aggregation_window_size = int(os.getenv("AGGREGATION_WINDOW_SIZE", "5"))
buffer_max_size = int(os.getenv("BUFFER_MAX_SIZE", "1000"))

processor = EdgeProcessor(
    rsu_id=rsu_id,
    assigned_sector=assigned_sector,
    aggregation_window_size=aggregation_window_size,
    buffer_max_size=buffer_max_size,
)

mqtt_client = None
if enable_mqtt:
    try:
        import paho.mqtt.client as mqtt
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"Warning: Failed to connect to MQTT broker: {e}")
        mqtt_client = None

sqs_client = None
if sqs_queue_url:
    try:
        sqs_client = boto3.client("sqs", region_name=aws_region)
    except Exception as e:
        print(f"Warning: Failed to initialize SQS client: {e}")
        sqs_client = None


def publish_mqtt(topic: str, payload: dict) -> None:
    """Publish message to MQTT topic."""
    if mqtt_client:
        try:
            mqtt_client.publish(topic, json.dumps(payload))
        except Exception as e:
            print(f"Warning: MQTT publish failed for topic {topic}: {e}")


def send_to_sqs(event: V2XEvent) -> bool:
    """Send event to SQS queue. Returns False if cloud_disconnected or error."""
    if processor.cloud_disconnected or not sqs_client or not sqs_queue_url:
        return False
    try:
        sqs_client.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=event.model_dump_json(),
        )
        return True
    except Exception as e:
        print(f"Error sending to SQS: {e}")
        return False


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/api/status")
def status():
    return processor.get_status()


@app.post("/api/events")
def ingest_event(event: V2XEvent):
    """Ingest a V2XEvent via HTTP (for testing or fallback)."""
    forwarded_event, local_alert = processor.process_event(event)

    if local_alert:
        publish_mqtt(f"v2x/{rsu_id}/alerts/local", local_alert.model_dump(mode="json"))
        publish_mqtt(f"v2x/{rsu_id}/alerts", local_alert.model_dump(mode="json"))

    if forwarded_event:
        success = send_to_sqs(forwarded_event)
        if not success and processor.cloud_disconnected:
            processor.buffer_event(forwarded_event)

    return {
        "processed": forwarded_event is not None,
        "local_alert": local_alert.model_dump(mode="json") if local_alert else None,
    }


@app.post("/api/control/disconnection")
def control_disconnection(payload: dict):
    """Simulate/restore edge-cloud disconnection."""
    disconnected = payload.get("disconnected", False)
    processor.cloud_disconnected = disconnected
    
    if not disconnected:
        buffered = processor.flush_buffer()
        sent = 0
        for event in buffered:
            if send_to_sqs(event):
                sent += 1
        return {
            "status": "reconnected",
            "flushed_events": len(buffered),
            "sent_to_sqs": sent,
        }
    else:
        return {"status": "disconnected"}


@app.post("/api/control/reset")
def control_reset():
    """Reset all counters for clean experiment runs."""
    global processor
    processor = EdgeProcessor(
        rsu_id=rsu_id,
        assigned_sector=assigned_sector,
        aggregation_window_size=aggregation_window_size,
        buffer_max_size=buffer_max_size,
    )
    return {"status": "reset"}


@app.post("/sns/alerts")
def sns_alerts(request: dict):
    """Handle AWS SNS messages (alert relay)."""
    message_type = request.get("Type")

    if message_type == "SubscriptionConfirmation":
        subscribe_url = request.get("SubscribeURL")
        if subscribe_url:
            try:
                import requests
                requests.get(subscribe_url)
            except Exception as e:
                print(f"Warning: SNS subscription confirmation failed: {e}")
        return {"status": "subscription_confirmed"}

    elif message_type == "Notification":
        try:
            message = request.get("Message")
            if isinstance(message, str):
                alert_data = json.loads(message)
            else:
                alert_data = message

            alert = V2XAlert.model_validate(alert_data)
            processor.relay_cloud_alert(alert)
            publish_mqtt(f"v2x/{rsu_id}/alerts/cloud", alert.model_dump(mode="json"))
            publish_mqtt(f"v2x/{rsu_id}/alerts", alert.model_dump(mode="json"))
            return {"status": "alert_relayed"}
        except Exception as e:
            print(f"Error processing SNS notification: {e}")
            return {"status": "error", "message": str(e)}, 400

    else:
        try:
            alert = V2XAlert.model_validate(request)
            processor.relay_cloud_alert(alert)
            publish_mqtt(f"v2x/{rsu_id}/alerts/cloud", alert.model_dump(mode="json"))
            publish_mqtt(f"v2x/{rsu_id}/alerts", alert.model_dump(mode="json"))
            return {"status": "alert_relayed"}
        except Exception as e:
            print(f"Error processing direct alert: {e}")
            return {"status": "error", "message": str(e)}, 400


@app.post("/api/alerts/relay")
def relay_alert(alert: V2XAlert):
    """Direct alert relay endpoint."""
    processor.relay_cloud_alert(alert)
    publish_mqtt(f"v2x/{rsu_id}/alerts/cloud", alert.model_dump(mode="json"))
    publish_mqtt(f"v2x/{rsu_id}/alerts", alert.model_dump(mode="json"))
    return {"status": "alert_relayed"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
