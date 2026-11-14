# Project Context: V2X Distributed Monitoring & Alerting (Emergency Corridor Scenario)

## Current Milestone: Deliverable 2 (Complete System & Experiments) + Deliverable 1 (MVP) Compatibility
We are implementing Deliverable 2 for the Distributed Systems course at Universidade de Vigo while preserving 100% backward compatibility with Deliverable 1 (MVP).
All code, comments, logs, and Dashboard UI text MUST be in **English**.

## Repository Hygiene & Strict Environment Rules
- Do not use emojis in terminal output, summaries, or Markdown files, except `✅` and `❌`.
- Keep development summaries, checklists, and implementation manifests in the local `dev_notes/` directory.
- Keep `dev_notes/` in `.gitignore` so development notes remain local and do not enter Git.
- **Strict Virtualenv Isolation**: NEVER run global `python`, `python3`, `pip`, or `/Library/Developer/CommandLineTools/usr/bin/python3`. ALWAYS use `./.venv/bin/python`, `./.venv/bin/pip`, and `./.venv/bin/pytest`.
- **Backward Compatibility**: All existing unit and integration tests in `tests/` must continue to pass after every change.

### Architecture & Data Flow (Deliverable 1 MVP + Deliverable 2 Edge/Cloud Continuum)
1. **Simulator (`simulator/obu_simulator.py`)**:
   - Simulates 5 distinct vehicles (`veh-001` to `veh-005`) assigned across 2 RSU groups and road segments:
     - `RSU 1` (`rsu-1`, `sector-A12-tunnel`): `veh-001`, `veh-002`, `veh-003`
     - `RSU 2` (`rsu-2`, `sector-B04-bridge`): `veh-004`, `veh-005` (emergency ambulance)
   - Supports configurable event rate, sequence numbers (`seq_num`), event types (`traffic_telemetry`, `sudden_braking`, `emergency_vehicle`, `road_hazard`), transport modes (`sqs`, `http`, `rsu-http`, `mqtt`), and controlled anomaly injection (`delay`, `loss`, `duplication`, `reordering`, `burst`).

2. **Edge Layer — Roadside Units (`rsu_service/`)**:
   - Two containerized RSUs (`rsu-1`, `rsu-2`) receiving vehicle events via MQTT (`v2x/{rsu_id}/telemetry`) or HTTP (`/api/events`).
   - Validates schemas, tracks duplicates (`event_id`) and sequence anomalies (`seq_num`), generates immediate local alerts (`origin="local"`) published over MQTT, aggregates routine `traffic_telemetry` into `traffic_density` summaries, forwards selected events to SQS `v2x-events`, buffers events offline during cloud disconnection, and relays cloud alerts from SNS `v2x-alerts` (`/sns/alerts`) to vehicles over MQTT.

3. **Cloud Service (`v2x_service/`)**:
   - Containerized Python FastAPI application running on Amazon EC2.
   - Background worker consumes messages from SQS queue `v2x-events` using `boto3` with idempotency by `event_id`.
   - **Persistence (`v2x_service/storage.py`)**: Uses Amazon DynamoDB (`v2x-events-store` table with `event_id` as Partition Key) in AWS, with a seamless `InMemoryStorage` fallback when `STORAGE_BACKEND=memory`.
   - **Automatic Alert Generation (`v2x_service/rules.py`) & SNS Distribution**:
     - Triggers `EMERGENCY_CORRIDOR_ALERT`, `SEVERE_BRAKING_CONGESTION_ALERT`, and `ROAD_HAZARD_ALERT`, and publishes cloud alerts to Amazon SNS topic `v2x-alerts` (and RSU relay endpoints).

4. **Dashboard & Static Files (`static/` + Amazon S3)**:
   - Static frontend files (`index.html`, `styles.css`, `app.js`) are stored in `static/` and synced to Amazon S3, with local `/ui` mount fallback.

5. **AWS Academy Learner Lab Constraints**:
   - Default region MUST be `us-east-1`.
   - Credentials use temporary session tokens (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`). Never hardcode credentials.
   - Container images are pushed to **Amazon ECR**, and static/deployment bundles are uploaded to **Amazon S3**.
