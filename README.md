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

Invalid SQS messages are logged and deleted; transient processing failures are logged and left on the queue for retry. Configure an SQS dead-letter queue for repeatedly failing valid messages. The worker long-polls for five seconds and handles up to ten messages per receive.
