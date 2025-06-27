#!/usr/bin/env sh

docker run \
    --rm \
    --env-file .env \
    --volume .:/app \
    --volume /app/.venv \
    census 