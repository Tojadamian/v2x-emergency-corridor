# V2X Emergency Corridor MVP

## Local setup

Use Python 3.11, as in the container and CI (local tests also run on Python 3.9):

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn v2x_service.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000. In another terminal, run:

```sh
source .venv/bin/activate
python simulator/obu_simulator.py --mode http --rate 2
```

The rate is the total number of events per second across the five vehicles. Use `--http-url` to target another service. Run tests with `python -m pytest`.

Use `--roam-sectors` to alternate each vehicle's successive events between the tunnel and bridge, routed to the corresponding RSU. Without it, each vehicle keeps its default sector.

## AWS configuration

| Variable | Default | Description |
| --- | --- | --- |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS region |
| `STORAGE_BACKEND` | `memory` | `memory` or `dynamodb` |
| `DYNAMODB_TABLE` | `v2x-events-store` | DynamoDB table name |
| `ENABLE_SQS_WORKER` | `false` | Set to `true` to consume SQS |
| `SQS_QUEUE_URL` | empty | Required when the worker is enabled |
| `S3_STATIC_BASE_URL` | empty | Optional HTTP(S) base URL for dashboard assets |

Create a DynamoDB table with a string partition key named `event_id`, and an SQS Standard queue named `v2x-events`. Provide AWS Academy credentials through the normal AWS credential chain, including `AWS_SESSION_TOKEN` for temporary credentials. The service needs DynamoDB GetItem, PutItem, Scan and UpdateItem permissions, and SQS ReceiveMessage and DeleteMessage permissions. Run the simulator with `python simulator/obu_simulator.py --mode sqs --queue-url <queue-url> --rate 2`; it needs SQS SendMessage permission.

The service provides `/api/summary`, `/api/state`, `/api/events`, `/api/alerts`, and `POST /api/events/ingest`, as well as static files under `/ui`. The summary retains the most recent 50 events and alerts, with counts across all stored events. Dashboard state is reconstructed from all stored events, so the DynamoDB implementation scans the table and is intended for MVP-scale datasets. Memory storage is process-local and is lost on restart; use DynamoDB for persistent or multi-instance deployments.

Event timestamps must include a timezone. Vehicle state is the event with the latest timestamp for each source. A corridor is active while a vehicle's latest event generated an emergency corridor alert; a later event without such an alert clears it. Emergency events with an active siren generate an alert, as does an ambulance entering a segment containing other vehicles. Alerts remain in history. Segment speed is the mean of the latest reported speeds for vehicles in that segment. Concurrent DynamoDB writers commit each event and its alert atomically and suppress duplicate event IDs; rule evaluation uses the current scan snapshot and is not serialized across different events.

The highway map tracks vehicle state independently in both sectors. Each sector starts with four passenger vehicles in normal traffic; the ambulance remains on standby until a corridor is triggered there. Use each sector's action menu to trigger an emergency corridor, sudden braking, a road hazard, normal traffic, or a duplicate burst. These actions can also be requested through `POST /api/control/simulate-scenario` with an optional `sector` field; omitting it retains the scenario's default sector. Use the dashboard's Day Mode/Night Mode button to switch themes; the selection is saved in browser storage.

Invalid SQS messages are logged and deleted; transient processing failures are logged and left on the queue for retry. Configure an SQS dead-letter queue for repeatedly failing valid messages. The worker long-polls for five seconds and handles up to ten messages per receive.

## EC2 deployment

Install Docker and the AWS CLI on the EC2 instance, grant its AWS credentials access to ECR, SQS and DynamoDB, and run `deploy/deploy_ec2.sh [image-tag]` (default: `latest`). The script resolves the account and queue, pulls the image, and replaces the `v2x-service` container. Publish the image to ECR before deploying. It uses the instance role if no credentials are supplied; IMDSv2 access from a bridged container may require a metadata hop limit of at least 2. Alternatively, it passes explicitly exported AWS credential variables or mounts existing AWS shared credentials and config files read-only, including the selected `AWS_PROFILE`. Temporary credentials expire and must be refreshed. The container has access to any mounted credentials.

The script defaults `S3_STATIC_BASE_URL` to the account's S3 static path; inaccessible S3 assets fall back to `/ui` on the service. An EC2 security group must allow inbound TCP port 8000 from intended clients. The API has no authentication; restrict access accordingly.
