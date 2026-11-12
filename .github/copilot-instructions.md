# Project Context: V2X Distributed Monitoring & Alerting (Emergency Corridor Scenario)

## Current Milestone: Deliverable 1 (MVP)
We are implementing Deliverable 1 (MVP) for the Distributed Systems course at Universidade de Vigo.
All code, comments, logs, and Dashboard UI text MUST be in **English**.

### Architecture & Data Flow for Deliverable 1 (MVP)
1. **Simulator (`simulator/obu_simulator.py`)**:
   - Simulates at least 5 distinct vehicles (`veh-001` to `veh-005`) assigned across 2 logical road segments (`sector-A12-tunnel` and `sector-B04-bridge`).
   - `veh-005` is an emergency ambulance.
   - Supports a configurable event rate and 3 event types:
     1. `traffic_telemetry` (routine speed, lane, traffic density)
     2. `sudden_braking` (hard braking with deceleration in m/s^2)
     3. `emergency_vehicle` (ambulance approaching with active siren and target lane)
   - In Deliverable 1 (MVP), the simulator publishes events directly to the Amazon SQS queue `v2x-events` (MQTT and RSUs will be added in Deliverable 2).

2. **Cloud Service (`v2x_service/`)**:
   - Containerized Python FastAPI application running on Amazon EC2.
   - Background worker consumes messages from SQS queue `v2x-events` using `boto3`.
   - **Idempotency**: SQS Standard is at-least-once. Every event has a globally unique `event_id`. The service MUST check if `event_id` was already processed before storing or triggering alerts, counting duplicate deliveries for metrics.
   - **Persistence (`v2x_service/storage.py`)**: Uses Amazon DynamoDB (`v2x-events-store` table with `event_id` as Partition Key) in AWS, with a seamless `InMemoryStorage` fallback when `STORAGE_BACKEND=memory` (used in local dev and CI unit tests).
   - **Automatic Alert Generation (`v2x_service/rules.py`)**:
     - Rule 1 (`EMERGENCY_CORRIDOR_ALERT`): Triggered automatically when an `emergency_vehicle` event is received or when an emergency vehicle enters a segment with active vehicles. Instructs vehicles on Left Lane to move left and Middle/Right Lanes to move right.
     - Rule 2 (`SEVERE_BRAKING_CONGESTION_ALERT`): Triggered when `sudden_braking` has deceleration >= 6.0 m/s^2 or average segment speed drops below 25 km/h.

3. **Dashboard & Static Files (`static/` + Amazon S3)**:
   - Static frontend files (`index.html`, `styles.css`, `app.js`) are stored in `static/` and synced to Amazon S3.
   - FastAPI also mounts `static/` at `/ui` and exposes `/api/state`, `/api/events`, and `/api/alerts` so the Dashboard works reliably even if S3 Block Public Access is active in AWS Academy.
   - Dashboard UI can optionally load assets from `S3_STATIC_BASE_URL` if configured.

4. **AWS Academy Learner Lab Constraints**:
   - Default region MUST be `us-east-1`.
   - Credentials in AWS Academy use temporary session tokens (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`). Never hardcode credentials.
   - Container images are built and pushed to **Amazon ECR**.
   - Static files (`static/`) and deployment scripts (`deploy/`) are uploaded to **Amazon S3**.
