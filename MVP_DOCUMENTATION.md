# V2X Emergency Corridor — Deliverable 1 (MVP) Documentation

## 1. System Architecture & Data Flow
This system implements an **Emergency Corridor** and **Severe Braking Congestion** V2X monitoring platform deployed on AWS (`us-east-1`):

1. **OBU Simulator (`simulator/obu_simulator.py`)**: Simulates 5 connected vehicles (`veh-001` to `veh-005`, where `veh-005` operates as an ambulance) across two road sectors (`sector-A12-tunnel` and `sector-B04-bridge`), publishing JSON telemetry and hazard events to **Amazon SQS** or directly via HTTP REST.
2. **Message Ingestion (Amazon SQS — `v2x-events`)**: Decouples vehicle telemetry producers from the backend processing service.
3. **Core Backend Service (`v2x_service/main.py` on Amazon EC2)**: A containerized FastAPI (`0.115.0`) and Uvicorn (`0.30.6`) application running on an Amazon EC2 `t3.micro` instance (`v2x-cloud`) using the `LabInstanceProfile` IAM role. A background SQS worker polls messages, enforces idempotency by `event_id`, and evaluates safety rules:
   - `emergency_vehicle` (`siren_active=true`) -> Triggers an `EMERGENCY CORRIDOR ALERT` instructing Lane 1 to pull LEFT and Lanes 2 & 3 to pull RIGHT.
   - `sudden_braking` (`deceleration >= 6.0 m/s²`) -> Triggers a `SEVERE BRAKING CONGESTION ALERT` instructing vehicles to reduce speed to 40 km/h.
4. **Persistence Layer (Amazon DynamoDB — `v2x-events-store`)**: Persists processed V2X events, active alerts, and the global idempotency counter (`Duplicates ignored`).
5. **Static Dashboard (Amazon S3 — `v2x-mvp-assets-975049955120`)**: Hosts the static dashboard assets (`index.html`, `styles.css`, `app.js`) and the EC2 deployment script (`deploy_ec2.sh`), with an automatic local container fallback (`/ui/*`) when S3 public access is restricted by AWS Academy policies.

## 2. AWS Infrastructure Summary (`us-east-1`)
- **AWS Account ID**: `975049955120`
- **EC2 Instance**: `i-0cc62658beb69a897` (`v2x-service-mvp` / host `v2x-cloud`), Amazon Linux 2023 (64-bit x86_64), `t3.micro` (2 vCPU, 1 GiB RAM + 2.0 GiB Swap), Public IP: `100.53.177.124`
- **Security Group**: `sg-030e8367fa5160b70` (`launch-wizard-1` — Inbound TCP `22` for SSH, TCP `8000` for HTTP Dashboard)
- **IAM Instance Profile**: `LabInstanceProfile`
- **SQS Queue URL**: `https://sqs.us-east-1.amazonaws.com/975049955120/v2x-events`
- **DynamoDB Table**: `v2x-events-store` (Partition key: `event_id`)
- **ECR Repository**: `975049955120.dkr.ecr.us-east-1.amazonaws.com/v2x-service:latest`
- **S3 Bucket**: `s3://v2x-mvp-assets-975049955120`

## 3. CI/CD Pipeline (GitHub Actions — `.github/workflows/mvp-ci-cd.yml`)
Every push to the `main` branch in `Tojadamian/v2x-emergency-corridor` triggers a two-stage pipeline:
1. **Quality Gates & Unit Tests**: Runs on `ubuntu-latest` with Python 3.11, installs dependencies from `requirements.txt`, and executes the 22 unit and integration tests via `pytest` (`STORAGE_BACKEND=memory`).
2. **Publish Image to ECR & Static Files to S3**: Authenticates with AWS, builds the Docker image, pushes `v2x-service:latest` (and the commit SHA tag) to Amazon ECR, and syncs `static/` and `deploy/` to Amazon S3.

## 4. Operational Runbook

### A. Run Unit Tests Locally
    PYTHONPATH=. .venv/bin/python -m pytest -v

### B. Run Container Locally in Docker Desktop (macOS)
    docker build -t v2x-service:local .
    docker run -d --name v2x-local -p 8080:8000 -e STORAGE_BACKEND=memory v2x-service:local
    # Local Dashboard: http://localhost:8080

### C. Deploy or Update Container on EC2 (`damian@v2x-cloud`)
    ssh v2x-cloud
    vdeploy   # Downloads deploy_ec2.sh from S3, pulls latest ECR image, restarts container
    dps       # Displays running container status and ports
    vlogs     # Streams live container logs

### D. Stream Live V2X Traffic to AWS SQS
    AWS_DEFAULT_REGION=us-east-1 SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/975049955120/v2x-events .venv/bin/python simulator/obu_simulator.py --mode sqs --rate 2
