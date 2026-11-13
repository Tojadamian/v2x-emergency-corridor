#!/usr/bin/env bash
set -euo pipefail

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
IMAGE_TAG="${1:-latest}"
ECR_REPO="v2x-service"
DYNAMODB_TABLE="${DYNAMODB_TABLE:-v2x-events-store}"
QUEUE_NAME="${QUEUE_NAME:-v2x-events}"

echo "[1/5] Resolving AWS Account ID and resources in ${AWS_DEFAULT_REGION}..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
IMAGE_URI="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

SQS_QUEUE_URL="${SQS_QUEUE_URL:-$(aws sqs get-queue-url --queue-name "${QUEUE_NAME}" --region "${AWS_DEFAULT_REGION}" --query QueueUrl --output text)}"
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

echo "[2/5] Logging in to Amazon ECR (${ECR_REGISTRY})..."
aws ecr get-login-password --region "${AWS_DEFAULT_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "[3/5] Pulling container image ${IMAGE_URI}..."
docker pull "${IMAGE_URI}"

echo "[4/5] Stopping previous v2x-service container (if running)..."
existing_container=$(docker ps -a --filter name='^/v2x-service$' --format '{{.Names}}')
if [[ "${existing_container}" == "v2x-service" ]]; then
  docker rm -f v2x-service >/dev/null
fi

echo "[5/5] Starting v2x-service container on port 8000..."
docker run -d \
  --name v2x-service \
  --restart unless-stopped \
  -p 8000:8000 \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
  "${credential_options[@]}" \
  -e STORAGE_BACKEND="dynamodb" \
  -e ENABLE_SQS_WORKER="true" \
  -e SQS_QUEUE_URL="${SQS_QUEUE_URL}" \
  -e DYNAMODB_TABLE="${DYNAMODB_TABLE}" \
  -e S3_STATIC_BASE_URL="${S3_STATIC_BASE_URL}" \
  "${IMAGE_URI}"

echo "Deployment complete! Dashboard available at http://<EC2-PUBLIC-IP>:8000"
