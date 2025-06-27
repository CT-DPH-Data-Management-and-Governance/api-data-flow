#!/usr/bin/env bash

set -e

IMAGE_NAME="dmag-api-data-flow"
IMAGE_TAG="0.0.1"
LOG_FILE="dmag_api_data_flow.log"

echo "Running container from image ${IMAGE_NAME}:${IMAGE_TAG}..."

docker run \
    --rm \
    --env-file .env \
    --volume .:/app \
    --volume /app/.venv \
    "${IMAGE_NAME}:${IMAGE_TAG}" \
    2>&1 | tee -a  "${LOG_FILE}"