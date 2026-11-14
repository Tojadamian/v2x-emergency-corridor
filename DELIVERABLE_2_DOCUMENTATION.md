# V2X Emergency Corridor — Deliverable 2 Documentation

## 1. End-to-End Distributed Architecture (Figure 1 Implementation)

The implementation has three cooperating paths:

1. **Event Path:** `OBUs -> MQTT -> RSU-1 / RSU-2 -> SQS v2x-events -> V2X-SERVICE -> DynamoDB`
   - Each OBU generates a typed `V2XEvent` with a unique `event_id`, timestamp, vehicle identifier, sequence number, sector, and payload.
   - An OBU publishes to `v2x/{rsu_id}/telemetry`. The assigned RSU validates and deduplicates the event, tracks sequence numbers, and handles safety-critical events locally.
   - RSUs forward selected events and aggregated traffic summaries to the Amazon SQS Standard queue `v2x-events`.
   - The V2X-SERVICE long-polls the queue, applies the cloud rules, persists event state and alerts in DynamoDB (`v2x-events-store`), and acknowledges successfully handled messages.
   - A memory-backed service is available for local development; it does not persist data across process restarts.
   - HTTP (`/api/events`) RSU ingress, direct cloud HTTP ingestion, and the simulator's direct SQS mode are also available for compatibility and testing.
2. **Alert Path:** `V2X-SERVICE -> SNS v2x-alerts -> HTTP/HTTPS RSU /sns/alerts -> MQTT -> OBUs` plus a **Fast Edge Loop (< 10 ms)** for local RSU alerts.
   - The RSU evaluates emergency-vehicle, severe-braking, and road-hazard events at the edge and publishes local alerts without waiting for SQS, the cloud rules, or SNS.
   - The edge-loop target is under 10 ms. This is a design target, not a hard real-time guarantee; the isolated experiments do not measure production network or device latency.
   - Cloud alerts are published to SNS topic `v2x-alerts`. RSU `/sns/alerts` endpoints handle SNS `SubscriptionConfirmation` and notifications, then publish the alert to the RSU's MQTT alert topics for OBU subscribers.
   - The cloud service also attempts direct HTTP relay to configured RSU endpoints. This is an additional relay path when SNS subscription delivery is unavailable, rather than a replacement for SNS.
3. **DevOps Path:** `GitHub Actions -> pytest -> Docker build -> Amazon ECR + S3 bundle -> EC2 vdeploy`
   - GitHub Actions runs the complete test suite and controlled experiments before its push pipeline builds the shared Docker image.
   - The image is pushed to both `v2x-service` and `v2x-rsu` repositories in Amazon ECR; dashboard assets and deployment scripts are synchronized to S3.
   - On EC2, `vdeploy` (when configured on the host) or `deploy/deploy_ec2.sh` installs/replaces the cloud and RSU containers and configures the MQTT broker and AWS messaging resources.

### Vehicle-to-sector topology

| Sector | RSU | Vehicles |
| --- | --- | --- |
| `sector-A12-tunnel` | `rsu-1` | `veh-001`, `veh-002`, `veh-003` |
| `sector-B04-bridge` | `rsu-2` | `veh-004`, `veh-005` |

`veh-005` is the ambulance. The simulator attaches an `rsu_id` and monotonically increasing per-vehicle `seq_num` to generated events. With sector roaming enabled, subsequent events may be routed to the other sector's RSU.

## 2. Event & Alert Schemas

The schemas are defined in `common/schemas.py`. Timestamps are ISO 8601 strings and must include a timezone. `event_id` and `alert_id` are generated when omitted. `rsu_id` and `seq_num` are optional on events; `origin` defaults to `cloud` and `rsu_id` is optional on alerts.

### `V2XEvent`

| Field | Type | Required/default | Description |
| --- | --- | --- | --- |
| `event_id` | string | Generated if omitted | Unique event identifier used for deduplication. |
| `source_id` | string | Required | Vehicle identifier, or the RSU identifier for an aggregated summary. |
| `event_type` | string enum | Required | One of the five event types below. |
| `event_time` | ISO 8601 string | Current UTC time if omitted | Event occurrence time; timezone is required. |
| `location` | string | Required | Road segment or RSU sector. |
| `rsu_id` | string or `null` | Optional; `null` if omitted | RSU associated with the event. |
| `seq_num` | integer or `null` | Optional; `null` if omitted | Vehicle sequence number used by edge monitoring. |
| `payload` | object | Required | Event-specific measurements and attributes. |

| `event_type` | Common payload fields | Meaning |
| --- | --- | --- |
| `traffic_telemetry` | `vehicle_type`, `speed_kmh`, `lane`, `traffic_density` | Routine vehicle telemetry. The edge buffers samples and aggregates each configured count of samples. |
| `sudden_braking` | `deceleration_ms2` plus available vehicle telemetry | Braking event; the schema requires `deceleration_ms2`. |
| `emergency_vehicle` | `siren_active`, `target_lane` plus available vehicle telemetry | Emergency vehicle or ambulance event. |
| `road_hazard` | `hazard_type` plus optional context | Road hazard such as debris, pothole, or obstruction. |
| `traffic_density` | `avg_speed_kmh`, `vehicle_count`, `sample_count`, `vehicles` | RSU-generated summary of buffered telemetry samples. |

Example `traffic_telemetry`:

```json
{
  "event_id": "evt-001",
  "source_id": "veh-001",
  "event_type": "traffic_telemetry",
  "event_time": "2026-11-14T16:00:00+00:00",
  "location": "sector-A12-tunnel",
  "rsu_id": "rsu-1",
  "seq_num": 1,
  "payload": {
    "vehicle_type": "car",
    "speed_kmh": 72,
    "lane": 2,
    "traffic_density": "medium"
  }
}
```

Example `sudden_braking`:

```json
{
  "event_id": "evt-002",
  "source_id": "veh-002",
  "event_type": "sudden_braking",
  "event_time": "2026-11-14T16:00:01+00:00",
  "location": "sector-A12-tunnel",
  "rsu_id": "rsu-1",
  "seq_num": 8,
  "payload": {
    "vehicle_type": "truck",
    "speed_kmh": 48,
    "lane": 1,
    "deceleration_ms2": 7.2
  }
}
```

Example `emergency_vehicle`:

```json
{
  "event_id": "evt-003",
  "source_id": "veh-005",
  "event_type": "emergency_vehicle",
  "event_time": "2026-11-14T16:00:02+00:00",
  "location": "sector-B04-bridge",
  "rsu_id": "rsu-2",
  "seq_num": 14,
  "payload": {
    "vehicle_type": "ambulance",
    "speed_kmh": 86,
    "lane": 2,
    "siren_active": true,
    "target_lane": 2
  }
}
```

Example `road_hazard`:

```json
{
  "event_id": "evt-004",
  "source_id": "veh-003",
  "event_type": "road_hazard",
  "event_time": "2026-11-14T16:00:03+00:00",
  "location": "sector-A12-tunnel",
  "rsu_id": "rsu-1",
  "seq_num": 3,
  "payload": {
    "hazard_type": "debris",
    "lane": 1
  }
}
```

Example RSU-generated `traffic_density`:

```json
{
  "event_id": "evt-005",
  "source_id": "rsu-1",
  "event_type": "traffic_density",
  "event_time": "2026-11-14T16:00:04+00:00",
  "location": "sector-A12-tunnel",
  "rsu_id": "rsu-1",
  "seq_num": null,
  "payload": {
    "avg_speed_kmh": 68.5,
    "vehicle_count": 2,
    "sample_count": 2,
    "vehicles": [
      {"vehicle_id": "veh-001", "speed_kmh": 72, "lane": 2},
      {"vehicle_id": "veh-002", "speed_kmh": 65, "lane": 1}
    ]
  }
}
```

### `V2XAlert`

| Field | Type | Required/default | Description |
| --- | --- | --- | --- |
| `alert_id` | string | Generated if omitted | Unique alert identifier. |
| `alert_type` | string enum | Required | Cloud: `EMERGENCY_CORRIDOR_ALERT`, `SEVERE_BRAKING_CONGESTION_ALERT`, `ROAD_HAZARD_ALERT`; local: `LOCAL_EMERGENCY_CORRIDOR_ALERT`, `LOCAL_COLLISION_RISK_ALERT`, `LOCAL_ROAD_HAZARD_ALERT`. |
| `severity` | string enum | Required | `WARNING` or `CRITICAL`. |
| `origin` | string | Defaults to `cloud` | `cloud` for cloud-generated alerts; `local` for RSU-generated alerts. |
| `rsu_id` | string or `null` | Optional; `null` if omitted | Originating RSU for local alerts, when applicable. |
| `location` | string | Required | Road segment or sector affected. |
| `triggered_by_event_id` | string | Required | ID of the event that caused the alert. |
| `source_id` | string | Required | Vehicle or source associated with the triggering event. |
| `created_at` | ISO 8601 string | Current UTC time if omitted | Alert creation time. |
| `message` | string | Required | Human-readable alert details. |
| `action_instruction` | string | Required | Recommended driver response. |

Cloud alert example:

```json
{
  "alert_id": "alt-001",
  "alert_type": "ROAD_HAZARD_ALERT",
  "severity": "WARNING",
  "origin": "cloud",
  "rsu_id": null,
  "location": "sector-A12-tunnel",
  "triggered_by_event_id": "evt-004",
  "source_id": "veh-003",
  "created_at": "2026-11-14T16:00:03+00:00",
  "message": "Road hazard reported in sector-A12-tunnel; reduce speed and proceed with caution.",
  "action_instruction": "REDUCE SPEED and proceed with caution through this sector."
}
```

Local alert example:

```json
{
  "alert_id": "alt-002",
  "alert_type": "LOCAL_EMERGENCY_CORRIDOR_ALERT",
  "severity": "CRITICAL",
  "origin": "local",
  "rsu_id": "rsu-2",
  "location": "sector-B04-bridge",
  "triggered_by_event_id": "evt-003",
  "source_id": "veh-005",
  "created_at": "2026-11-14T16:00:02+00:00",
  "message": "Emergency vehicle veh-005 detected with active siren in sector-B04-bridge",
  "action_instruction": "Vehicles on LEFT_LANE move left, MIDDLE and RIGHT_LANE move right"
}
```

## 3. Messaging Configuration, QoS, Idempotency & Edge Buffering Justification

### MQTT at the edge

MQTT is a lightweight publish/subscribe protocol suited to vehicle-to-RSU links: it avoids repeated HTTP request headers and uses topic routing to isolate RSUs and message purposes. The implemented topics are:

| Direction/purpose | Topic |
| --- | --- |
| OBU telemetry to RSU | `v2x/{rsu_id}/telemetry` |
| Local RSU safety alert | `v2x/{rsu_id}/alerts/local` |
| Relayed cloud alert | `v2x/{rsu_id}/alerts/cloud` |
| Compatibility alert topic also published by RSUs | `v2x/{rsu_id}/alerts` |

The RSU also accepts the legacy telemetry form `v2x/telemetry/{rsu_id}`. OBU simulators subscribe to `v2x/+/alerts/#` to receive alerts from either RSU.

MQTT publishes and subscriptions use QoS 1 (at-least-once delivery) by default. Configure the RSU with `MQTT_QOS` and the simulator with `--mqtt-qos` (0, 1, or 2); actual delivery QoS is limited by both the publisher and subscription settings. QoS 1 can deliver duplicates, which the RSU suppresses by `event_id` during its process lifetime. `seq_num` tracking helps detect sequence gaps and timestamp-based reordering; it cannot recover a message lost before RSU receipt. MQTT delivery depends on connectivity and session behavior; once received and selected for forwarding, events use SQS and edge buffering for stronger cloud-path resilience.

### Amazon SQS Standard: `v2x-events`

SQS decouples RSU event production from cloud processing, absorbs temporary rate mismatches, and lets the service retry work independently. The worker long-polls with `WaitTimeSeconds=5` and requests up to 10 messages at a time. SQS visibility timeout hides a received message while it is being processed; if processing fails and the message is not deleted, it becomes visible again for retry. Configure a dead-letter queue and a suitable visibility timeout for deployment workloads.

SQS Standard is at-least-once and does not guarantee ordering. The V2X-SERVICE storage layer deduplicates by `event_id`; duplicate IDs do not apply the event twice. Each RSU also keeps a process-local set of seen event IDs and ignores duplicates during that RSU process lifetime. This second check protects the edge from duplicate MQTT/HTTP deliveries but is not persistent across RSU restarts.

### Amazon SNS: `v2x-alerts`

SNS provides a pub/sub fan-out point so the same cloud alert can reach multiple subscribed RSUs without the cloud service needing to manage each subscriber's MQTT connection. RSU `/sns/alerts` endpoints process automatic SNS `SubscriptionConfirmation` requests and `Notification` messages, then publish accepted cloud alerts over MQTT. The V2X-SERVICE also attempts direct HTTP relay to configured RSU endpoints, providing a second relay path if SNS delivery or subscription setup is unavailable. HTTPS endpoints should be used when the deployment's network and SNS configuration support them.

### Edge preprocessing and offline store-and-forward

Routine `traffic_telemetry` is buffered at each RSU and aggregated after `AGGREGATION_WINDOW_SIZE` samples (default 5) into one `traffic_density` event containing sample count, vehicle count, average speed, and per-vehicle sample details. These non-overlapping batches are tumbling count windows, not timer-based windows. Critical `emergency_vehicle`, `sudden_braking`, and `road_hazard` events are forwarded immediately and evaluated for local alerts before cloud delivery.

If SQS is unavailable or the RSU is marked disconnected, forwarded events enter a bounded FIFO store-and-forward buffer. `BUFFER_MAX_SIZE` sets its capacity (default 1000). When full, new events are rejected and `rejected_events` is incremented; the buffer does not silently evict older entries. On reconnection, the RSU flushes queued events in order, tracks `flushed_events_count`, and records `last_recovery_time_ms`. A persistent outage beyond capacity can therefore cause explicit, measurable event loss, while local alerts continue to operate.

## 4. Summary of Controlled Experiments

The results below are from `EXPERIMENT_RESULTS.md`. These are fixed-workload, isolated in-process experiments using two real edge processors, simulator-generated events, memory storage, and the cloud rule engine. They do not measure deployed AWS services, real network behavior, or production capacity. Random seeds and workload sizes are fixed; latency and memory measurements vary by host.

| Experiment family | Workload and quantitative findings | Interpretation |
| --- | --- | --- |
| **Delivery anomalies** | Nine conditions delivered 100 critical hazards each. Baseline processed 100/100. Delays of 50 ms and 250 ms increased median latency to 50.115 ms and 250.119 ms without event loss. At 10% and 30% configured loss probabilities, 90 and 68 events were accepted; the edge estimated 10 and 27 sequence gaps. At duplication rates 15% and 40%, 13 and 36 duplicate deliveries were ignored. Reordering rates 15% and 40% yielded 12 and 33 reordered events and 21 and 61 estimated gaps. | `event_id` deduplication handles duplicate delivery. Sequence-gap counts are estimates, not ground-truth loss; reordering can inflate them, and tail losses may not be detected. Final state was inconsistent in the higher-loss trial. |
| **Edge-cloud disconnection** | Both scenarios sent 40 critical events during an outage and generated 40 local alerts. With buffer capacity 1000, 40 were buffered and flushed, with 0 rejected and 0 data loss. With capacity 15, 15 were buffered/flushed and 25 rejected, producing 25 data loss. Order was preserved in both scenarios. | Local safety alerts remain available during the partition, but a bounded buffer trades eventual delivery for bounded storage when capacity is exceeded. |
| **Component failure** | A simulated FIFO queue paused 25 and 75 unique events and injected 5 and 15 retry deliveries. Recovery processed all 25 and 75 unique events, ignored all 5 and 15 duplicates, and reached consistent final state. In-process recovery measured 1.054 ms and 7.234 ms. | Idempotency suppresses repeated deliveries after recovery. It does not restore lost data or model real SQS timing or ordering. |
| **Load increase** | Workloads of 100, 300, and 600 events at 5, 15, and 30 vehicles respectively filtered 65, 194, and 399 telemetry events at the edge. Event-count reduction was 65.0%, 64.67%, and 66.5%; cloud processed 35, 106, and 201 events with 0% measured errors. p95 forwarded-event latency was 0.092, 0.412, and 1.07 ms. | Aggregation reduces cloud event count but loses individual routine samples from the cloud view. Throughput, memory, and latency are local harness measurements and do not establish production capacity. |

The experiment report defines each metric and its limitations. For example, load-test bandwidth savings mean fewer forwarded events, not measured bytes; the in-process queue depth is not AWS queue depth; and a zero peak-RSS delta does not mean no allocations occurred.

## 5. Reproducible Deployment & Demonstration Runbook

### A. Run tests and automated experiments locally

From the repository root, with the project virtual environment and dependencies available:

```sh
./.venv/bin/pytest -q
./.venv/bin/python -m experiments.run_experiments
```

The first command runs all tests in `tests/`, including the documentation verification. The experiment runner regenerates `EXPERIMENT_RESULTS.md` and the local-only `dev_notes/experiment_results.json`. Workloads are reproducible; measured timings and memory are not.

### B. Run the four-container Docker Compose stack

```sh
docker compose up --build -d
docker compose ps
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health
./.venv/bin/python simulator/obu_simulator.py \
  --mode mqtt --mqtt-host 127.0.0.1 --rate 2 --iterations 5
docker compose logs -f
```

The stack consists of Mosquitto, `rsu-1`, `rsu-2`, and `v2x-service`. With default Compose settings, storage is in memory, the SQS worker is disabled, and no AWS queue is configured. The RSUs can receive MQTT events and issue local alerts, but cloud forwarding needs an accessible SQS queue. For an AWS-connected run, set `SQS_QUEUE_URL`, `SNS_TOPIC_ARN`, `ENABLE_SQS_WORKER=true`, and the appropriate `STORAGE_BACKEND`/DynamoDB table, region, and AWS credentials before starting Compose. Supply temporary AWS Academy credentials, including `AWS_SESSION_TOKEN`, through the environment; never commit them to a `.env` file or source control. Stop the stack with `docker compose down`.

### C. Deploy the complete system on Amazon EC2

1. Use AWS Academy credentials in `us-east-1` and ensure ECR, S3, EC2, SQS, SNS, and DynamoDB permissions are available. The EC2 instance needs Docker, AWS CLI access (preferably through an instance role), inbound rules for the dashboard/API port 8000, and RSU ports 8001/8002 as required by the deployment.
2. Ensure the ECR repositories `v2x-service` and `v2x-rsu` and DynamoDB table `v2x-events-store` exist. The table partition key is string `event_id`. The deployment script creates the SQS Standard queue `v2x-events` and SNS topic `v2x-alerts`, then starts the MQTT broker, both RSUs, and the cloud service.
3. Build and publish the service image from a machine with Docker and AWS CLI access:

   ```sh
   export AWS_DEFAULT_REGION=us-east-1
   export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
   export ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
   export IMAGE_TAG="$(git rev-parse --short HEAD)"
   aws ecr get-login-password --region "$AWS_DEFAULT_REGION" \
     | docker login --username AWS --password-stdin "$ECR_REGISTRY"
   docker build \
     -t "$ECR_REGISTRY/v2x-service:$IMAGE_TAG" \
     -t "$ECR_REGISTRY/v2x-service:latest" \
     -t "$ECR_REGISTRY/v2x-rsu:$IMAGE_TAG" \
     -t "$ECR_REGISTRY/v2x-rsu:latest" .
   docker push "$ECR_REGISTRY/v2x-service:$IMAGE_TAG"
   docker push "$ECR_REGISTRY/v2x-service:latest"
   docker push "$ECR_REGISTRY/v2x-rsu:$IMAGE_TAG"
   docker push "$ECR_REGISTRY/v2x-rsu:latest"
   ```

4. On EC2, use the configured `vdeploy` helper if present. Otherwise, copy the deployment script from the deployment bundle or check out the repository, then run:

   ```sh
   ./deploy/deploy_ec2.sh
   ```

   The script deploys the `latest` tag by default and also accepts an explicit image tag. If the RSU image cannot be pulled, it warns and uses the cloud service image for the RSUs. It creates or reuses the shared Docker network and replaces the four containers. It attempts external SNS HTTP subscriptions when it can determine the instance's public IP; direct RSU HTTP alert relay is configured as well.
5. Open `http://<EC2-PUBLIC-IP>:8000`. Verify the dashboard, service health, and containers with `curl http://localhost:8000/health` and `docker ps`.

The GitHub Actions push pipeline builds and pushes `latest` and the commit-SHA tag to ECR and syncs `static/` and `deploy/` to the configured S3 bucket. Temporary credentials expire and must be refreshed before later deployment operations.

### D. Run live defense demonstrations

1. Open the dashboard at `http://localhost:8000` for Compose or `http://<EC2-PUBLIC-IP>:8000` for EC2.
2. Use the Digital Twin's per-sector controls to select the tunnel (`rsu-1`) or bridge (`rsu-2`) and trigger the emergency corridor, sudden braking, road hazard, normal traffic, and duplicate-burst scenarios. Where available, toggle the selected RSU's cloud connection to demonstrate local alerting and buffer recovery.
3. In a terminal with the simulator dependencies installed, inject delivery anomalies over MQTT:

   ```sh
   ./.venv/bin/python simulator/obu_simulator.py \
     --mode mqtt --mqtt-host 127.0.0.1 --rate 2 --burst-size 3 --iterations 10 \
     --delay-ms 50 --loss-rate 0.1 --duplicate-rate 0.15 --reorder-rate 0.15
   ```

   Replace `127.0.0.1` with the reachable MQTT broker host when running outside the Compose network. Each anomaly rate is a probability, not an exact event count. The simulator prints generated event IDs and received alerts. Inspect dashboard RSU counters for duplicates, sequence gaps, local alerts, buffered/rejected events, and recovery. Interpret sequence-gap counters as estimates, especially during reordering.
