#!/usr/bin/env bash
set -euo pipefail

export AWS_DEFAULT_REGION="us-east-1"
IMAGE_TAG="${1:-latest}"
ECR_REPO="v2x-service"
DYNAMODB_TABLE="${DYNAMODB_TABLE:-v2x-events-store}"
QUEUE_NAME="v2x-events"
SNS_TOPIC_NAME="v2x-alerts"
NETWORK_NAME="v2x-net"

echo "[1/6] Ensuring AWS messaging resources exist in ${AWS_DEFAULT_REGION}..."
SQS_QUEUE_URL=$(aws sqs create-queue \
  --queue-name "${QUEUE_NAME}" \
  --region "${AWS_DEFAULT_REGION}" \
  --query QueueUrl \
  --output text)
SNS_TOPIC_ARN=$(aws sns create-topic \
  --name "${SNS_TOPIC_NAME}" \
  --region "${AWS_DEFAULT_REGION}" \
  --query TopicArn \
  --output text)

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
IMAGE_URI="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"
S3_BUCKET="v2x-mvp-assets-${ACCOUNT_ID}"
S3_STATIC_BASE_URL="${S3_STATIC_BASE_URL:-https://${S3_BUCKET}.s3.${AWS_DEFAULT_REGION}.amazonaws.com/static}"

credential_options=()
if [[ -n "${AWS_ACCESS_KEY_ID:-}" ]]; then credential_options+=(-e AWS_ACCESS_KEY_ID); fi
if [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then credential_options+=(-e AWS_SECRET_ACCESS_KEY); fi
if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then credential_options+=(-e AWS_SESSION_TOKEN); fi
if [[ -n "${AWS_PROFILE:-}" ]]; then credential_options+=(-e AWS_PROFILE); fi
credentials_file="${AWS_SHARED_CREDENTIALS_FILE:-${HOME}/.aws/credentials}"
config_file="${AWS_CONFIG_FILE:-${HOME}/.aws/config}"
if [[ -f "${credentials_file}" ]]; then
  credential_options+=(--mount "type=bind,src=${credentials_file},dst=/root/.aws/credentials,readonly")
fi
if [[ -f "${config_file}" ]]; then
  credential_options+=(--mount "type=bind,src=${config_file},dst=/root/.aws/config,readonly")
fi

echo "[2/6] Logging in to Amazon ECR (${ECR_REGISTRY})..."
aws ecr get-login-password --region "${AWS_DEFAULT_REGION}" |
  docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "[3/6] Pulling ${IMAGE_URI} and preparing the shared RSU image tag..."
docker pull "${IMAGE_URI}"
docker tag "${IMAGE_URI}" "v2x-rsu:latest"

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  docker network create "${NETWORK_NAME}" >/dev/null
fi

echo "[4/6] Preparing Mosquitto and replacing the existing stack..."
printf 'listener 1883 0.0.0.0\nallow_anonymous true\n' >/tmp/mosquitto.conf
for container in v2x-service rsu-1 rsu-2 v2x-mqtt; do
  if [[ "$(docker ps -a --filter "name=^/${container}$" --format '{{.Names}}')" == "${container}" ]]; then
    docker rm -f "${container}" >/dev/null
  fi
done

docker run -d \
  --name v2x-mqtt \
  --restart unless-stopped \
  --network "${NETWORK_NAME}" \
  -p 1883:1883 \
  -v /tmp/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro \
  eclipse-mosquitto:2 >/dev/null

docker run -d \
  --name rsu-1 \
  --restart unless-stopped \
  --network "${NETWORK_NAME}" \
  -p 8001:8001 \
  -e RSU_ID="rsu-1" \
  -e ASSIGNED_SECTOR="sector-A12-tunnel" \
  -e ENABLE_MQTT="true" \
  -e MQTT_BROKER_HOST="v2x-mqtt" \
  -e MQTT_BROKER_PORT="1883" \
  -e SQS_QUEUE_URL="${SQS_QUEUE_URL}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
  "${credential_options[@]}" \
  "v2x-rsu:latest" \
  uvicorn rsu_service.main:app --host 0.0.0.0 --port 8001 >/dev/null

docker run -d \
  --name rsu-2 \
  --restart unless-stopped \
  --network "${NETWORK_NAME}" \
  -p 8002:8002 \
  -e RSU_ID="rsu-2" \
  -e ASSIGNED_SECTOR="sector-B04-bridge" \
  -e ENABLE_MQTT="true" \
  -e MQTT_BROKER_HOST="v2x-mqtt" \
  -e MQTT_BROKER_PORT="1883" \
  -e SQS_QUEUE_URL="${SQS_QUEUE_URL}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
  "${credential_options[@]}" \
  "v2x-rsu:latest" \
  uvicorn rsu_service.main:app --host 0.0.0.0 --port 8002 >/dev/null

echo "[5/6] Starting the cloud service..."
docker run -d \
  --name v2x-service \
  --restart unless-stopped \
  --network "${NETWORK_NAME}" \
  -p 8000:8000 \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
  "${credential_options[@]}" \
  -e STORAGE_BACKEND="dynamodb" \
  -e DYNAMODB_TABLE="${DYNAMODB_TABLE}" \
  -e SQS_QUEUE_URL="${SQS_QUEUE_URL}" \
  -e SNS_TOPIC_ARN="${SNS_TOPIC_ARN}" \
  -e RSU_1_URL="http://rsu-1:8001" \
  -e RSU_2_URL="http://rsu-2:8002" \
  -e RSU_ALERT_URLS="http://rsu-1:8001/sns/alerts,http://rsu-2:8002/sns/alerts" \
  -e ENABLE_SQS_WORKER="true" \
  -e S3_STATIC_BASE_URL="${S3_STATIC_BASE_URL}" \
  "${IMAGE_URI}" >/dev/null

echo "[6/6] Subscribing RSU endpoints to the SNS alert topic..."
EC2_PUBLIC_IP="${EC2_PUBLIC_IP:-}"
if [[ -z "${EC2_PUBLIC_IP}" ]]; then
  metadata_token=$(curl -fsS -X PUT \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" \
    --max-time 2 \
    http://169.254.169.254/latest/api/token 2>/dev/null || true)
  if [[ -n "${metadata_token}" ]]; then
    EC2_PUBLIC_IP=$(curl -fsS \
      -H "X-aws-ec2-metadata-token: ${metadata_token}" \
      --max-time 2 \
      http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || true)
  fi
fi

if [[ -n "${EC2_PUBLIC_IP}" ]]; then
  for port in 8001 8002; do
    if ! aws sns subscribe \
      --topic-arn "${SNS_TOPIC_ARN}" \
      --protocol http \
      --notification-endpoint "http://${EC2_PUBLIC_IP}:${port}/sns/alerts" \
      --region "${AWS_DEFAULT_REGION}"; then
      echo "Warning: SNS subscription for RSU port ${port} could not be created; internal alert relaying remains enabled." >&2
    fi
  done
else
  echo "Warning: EC2 public IP unavailable; skipped external RSU SNS subscriptions. Internal alert relaying remains enabled." >&2
fi

echo "Deployment complete. Dashboard available at http://<EC2-PUBLIC-IP>:8000"
