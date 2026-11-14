# V2X Emergency Corridor

A distributed vehicle-to-everything monitoring and alerting system for an emergency corridor scenario. The project includes the Deliverable 1 MVP, Deliverable 2 RSU edge/cloud continuum, a dashboard Digital Twin, deployment automation, and controlled experiments.

## Documentation

- [MVP documentation](./MVP_DOCUMENTATION.md) — Deliverable 1 architecture, configuration, and runbook.
- [Deliverable 2 documentation](./DELIVERABLE_2_DOCUMENTATION.md) — distributed architecture, schemas, messaging, experiment findings, deployment, and demonstrations.
- [Experiment results](./EXPERIMENT_RESULTS.md) — methodology, complete results tables, and measurement limitations.

## Quick start

The commands below assume the project virtual environment and dependencies are available. Install project dependencies with `./.venv/bin/pip install -r requirements.txt`.

### Deliverable 1: MVP service

Start the cloud service and send simulator events directly over HTTP:

```sh
STORAGE_BACKEND=memory ./.venv/bin/python -m uvicorn v2x_service.main:app \
  --host 0.0.0.0 --port 8000
```

In another terminal:

```sh
./.venv/bin/python simulator/obu_simulator.py \
  --mode http --http-url http://127.0.0.1:8000/api/events/ingest --rate 2
```

Open [http://localhost:8000](http://localhost:8000) for the dashboard. Stop the simulator with Ctrl-C.

### Deliverable 2: local distributed stack

Start Mosquitto, both RSUs, and the cloud service:

```sh
docker compose up --build -d
docker compose ps
curl http://localhost:8000/health
```

Publish simulator events to the local MQTT broker:

```sh
./.venv/bin/python simulator/obu_simulator.py \
  --mode mqtt --mqtt-host 127.0.0.1 --rate 2 --iterations 5
```

The default Compose configuration is AWS-independent: the service uses in-memory storage and its SQS worker is disabled. Local MQTT ingestion and RSU alerts work; configure AWS queue, topic, storage, and credentials to exercise cloud forwarding and persistence. See the [Deliverable 2 runbook](./DELIVERABLE_2_DOCUMENTATION.md#5-reproducible-deployment--demonstration-runbook) for AWS-connected Compose, EC2 deployment, and anomaly-injection demonstrations.

Stop the stack with `docker compose down`.

## Verification

Run the complete test suite and the isolated automated experiment runner:

```sh
./.venv/bin/pytest -q
./.venv/bin/python -m experiments.run_experiments
```

See [EXPERIMENT_RESULTS.md](./EXPERIMENT_RESULTS.md) for the generated metrics and caveats.
