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

SQS_QUEUE_URL=$(aws sqs get-queue-url --queue-name "${QUEUE_NAME}" --region "${AWS_DEFAULT_REGION}" --query QueueUrl --output text)
S3_BUCKET="v2x-mvp-assets-${ACCOUNT_ID}"
S3_STATIC_BASE_URL="https://${S3_BUCKET}.s3.${AWS_DEFAULT_REGION}.amazonaws.com/static"

echo "[2/5] Logging in to Amazon ECR (${ECR_REGISTRY})..."
aws ecr get-login-password --region "${AWS_DEFAULT_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "[3/5] Pulling container image ${IMAGE_URI}..."
docker pull "${IMAGE_URI}"

echo "[4/5] Stopping previous v2x-service container (if running)..."
docker rm -f v2x-service >/dev/null 2>&1 || true

echo "[5/5] Starting v2x-service container on port 8000..."
docker run -d \
  --name v2x-service \
  --restart unless-stopped \
  -p 8000:8000 \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
  -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
  -e STORAGE_BACKEND="dynamodb" \
  -e ENABLE_SQS_WORKER="true" \
  -e SQS_QUEUE_URL="${SQS_QUEUE_URL}" \
  -e DYNAMODB_TABLE="${DYNAMODB_TABLE}" \
  -e S3_STATIC_BASE_URL="${S3_STATIC_BASE_URL}" \
  "${IMAGE_URI}"

echo "Deployment complete! Dashboard available at http://<EC2-PUBLIC-IP>:8000"
