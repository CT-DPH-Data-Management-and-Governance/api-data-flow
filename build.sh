#!/usr/bin/env bash

set -e

IMAGE_NAME="dmag-api-data-flow"
IMAGE_TAG="0.0.1"

echo "Building ${IMAGE_NAME}:${IMAGE_TAG}..."

docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

echo "Build Complete."