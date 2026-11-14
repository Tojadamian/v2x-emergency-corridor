import asyncio
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional
from urllib.request import urlopen

import boto3
from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError

from common.schemas import V2XEvent, V2XAlert
from rsu_service.edge_processor import EdgeProcessor

logger = logging.getLogger(__name__)

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
mqtt_thread: Optional[threading.Thread] = None
mqtt_stop_event = threading.Event()

sqs_client = None
if sqs_queue_url:
    try:
        sqs_client = boto3.client("sqs", region_name=aws_region)
    except Exception:
        logger.exception("Failed to initialize SQS client")
        sqs_client = None


def publish_mqtt(topic: str, payload: dict) -> None:
    """Publish message to MQTT topic."""
    if mqtt_client:
        try:
            result = mqtt_client.publish(topic, json.dumps(payload))
            if result.rc != 0:
                logger.error("MQTT publish failed for topic %s with result code %s", topic, result.rc)
        except Exception:
            logger.exception("MQTT publish failed for topic %s", topic)


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
    except Exception:
        logger.exception("Error sending event to SQS")
        return False


def ingest_v2x_event(event: V2XEvent) -> dict:
    """Run the common HTTP and MQTT event ingestion path."""
    forwarded_event, local_alert = processor.process_event(event)

    if local_alert:
        alert_payload = local_alert.model_dump(mode="json")
        publish_mqtt(f"v2x/{rsu_id}/alerts/local", alert_payload)
        publish_mqtt(f"v2x/{rsu_id}/alerts", alert_payload)

    if forwarded_event:
        if not send_to_sqs(forwarded_event):
            processor.buffer_event(forwarded_event)

    return {
        "processed": forwarded_event is not None or local_alert is not None,
        "local_alert": local_alert.model_dump(mode="json") if local_alert else None,
    }


def handle_mqtt_payload(payload: bytes) -> Optional[dict]:
    """Validate an MQTT event payload and pass it through the shared ingestion path."""
    try:
        event = V2XEvent.model_validate_json(payload)
    except (ValidationError, ValueError):
        logger.exception("Discarding invalid MQTT V2X event")
        return None
    return ingest_v2x_event(event)


def _mqtt_on_connect(client, userdata, flags, reason_code, properties) -> None:
    client.subscribe(
        [
            (f"v2x/{rsu_id}/telemetry", 0),
            (f"v2x/telemetry/{rsu_id}", 0),
        ]
    )


def _mqtt_on_message(client, userdata, message) -> None:
    handle_mqtt_payload(message.payload)


def _mqtt_subscriber_loop() -> None:
    global mqtt_client
    import paho.mqtt.client as mqtt

    while not mqtt_stop_event.is_set():
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.on_connect = _mqtt_on_connect
        client.on_message = _mqtt_on_message
        mqtt_client = client
        try:
            client.connect(mqtt_broker_host, mqtt_broker_port, keepalive=60)
            client.loop_forever()
        except Exception:
            if not mqtt_stop_event.is_set():
                logger.exception("MQTT connection failed; retrying")
        finally:
            mqtt_client = None
            try:
                client.disconnect()
            except Exception:
                logger.exception("Failed to disconnect MQTT client")
        mqtt_stop_event.wait(2)


async def _start_mqtt_subscriber() -> None:
    global mqtt_thread
    if enable_mqtt and (mqtt_thread is None or not mqtt_thread.is_alive()):
        mqtt_stop_event.clear()
        mqtt_thread = threading.Thread(
            target=_mqtt_subscriber_loop,
            name=f"{rsu_id}-mqtt-subscriber",
            daemon=True,
        )
        mqtt_thread.start()


async def _stop_mqtt_subscriber() -> None:
    mqtt_stop_event.set()
    if mqtt_client is not None:
        try:
            mqtt_client.disconnect()
        except Exception:
            logger.exception("Failed to stop MQTT subscriber")
    if mqtt_thread is not None:
        await asyncio.to_thread(mqtt_thread.join, 5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _start_mqtt_subscriber()
    try:
        yield
    finally:
        await _stop_mqtt_subscriber()


app = FastAPI(title="V2X RSU Edge Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/api/status")
def status():
    return processor.get_status()


@app.post("/api/events")
def ingest_event(event: V2XEvent):
    """Ingest a V2XEvent via HTTP (for testing or fallback)."""
    return ingest_v2x_event(event)


@app.post("/api/control/disconnection")
def control_disconnection(payload: dict):
    """Simulate/restore edge-cloud disconnection."""
    disconnected = payload.get("disconnected", False)
    processor.cloud_disconnected = disconnected
    
    if not disconnected:
        buffered = processor.flush_buffer()
        sent = 0
        for index, event in enumerate(buffered):
            if send_to_sqs(event):
                sent += 1
            else:
                for pending in buffered[index:]:
                    processor.buffer_event(pending)
                break
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
async def sns_alerts(request: Request):
    """Handle AWS SNS messages (alert relay)."""
    try:
        body = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid SNS JSON body") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="SNS body must be a JSON object")
    message_type = body.get("Type") or request.headers.get("x-amz-sns-message-type")

    if message_type == "SubscriptionConfirmation":
        subscribe_url = body.get("SubscribeURL")
        if subscribe_url:
            try:
                with urlopen(subscribe_url, timeout=5) as response:
                    response.read()
            except Exception:
                logger.exception("SNS subscription confirmation failed")
        return {"status": "subscription_confirmed"}

    elif message_type == "Notification":
        try:
            message = body.get("Message")
            if isinstance(message, str):
                alert_data = json.loads(message)
            else:
                alert_data = message

            alert = V2XAlert.model_validate(alert_data)
            relay_cloud_alert(alert)
            return {"status": "alert_relayed"}
        except (ValidationError, ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail="Invalid SNS alert notification") from exc

    else:
        try:
            alert = V2XAlert.model_validate(body)
            relay_cloud_alert(alert)
            return {"status": "alert_relayed"}
        except (ValidationError, ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail="Invalid direct alert payload") from exc


def relay_cloud_alert(alert: V2XAlert) -> None:
    processor.relay_cloud_alert(alert)
    alert_payload = alert.model_dump(mode="json")
    publish_mqtt(f"v2x/{rsu_id}/alerts/cloud", alert_payload)
    publish_mqtt(f"v2x/{rsu_id}/alerts", alert_payload)


@app.post("/api/alerts/relay")
def relay_alert(alert: V2XAlert):
    """Direct alert relay endpoint."""
    relay_cloud_alert(alert)
    return {"status": "alert_relayed"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
