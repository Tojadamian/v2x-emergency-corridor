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
   - Simulates 5 distinct vehicles (`veh-001` to `veh-005`) assigned across 2 RSU groups (`rsu-1` in `sector-A12-tunnel` for `veh-001..003`, `rsu-2` in `sector-B04-bridge` for `veh-004..005` where `veh-005` is an ambulance).
   - Supports `seq_num`, 4 event types (`traffic_telemetry`, `sudden_braking`, `emergency_vehicle`, `road_hazard`), modes (`sqs`, `http`, `rsu-http`, `mqtt`), and anomaly injection (`delay`, `loss`, `duplication`, `reordering`, `burst`).
2. **Edge Layer — Roadside Units (`rsu_service/`)**:
   - Two containerized RSUs (`rsu-1`, `rsu-2`) receiving vehicle events via MQTT (`v2x/{rsu_id}/telemetry`) or HTTP (`/api/events`).
   - Generates immediate local alerts (`origin="local"`), aggregates routine `traffic_telemetry` into `traffic_density` summaries, forwards selected events to SQS `v2x-events`, buffers offline during disconnection, and relays cloud alerts from SNS `v2x-alerts` (`/sns/alerts`) over MQTT.
3. **Cloud Service (`v2x_service/`)**:
   - FastAPI service consuming SQS `v2x-events` with `event_id` idempotency, storing state in DynamoDB (`v2x-events-store`) or `InMemoryStorage`, generating automatic alerts, and publishing them to SNS `v2x-alerts`.
4. **AWS Academy Constraints**:
   - Region `us-east-1`, temporary session credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), images in ECR, static/deploy bundles in S3.
