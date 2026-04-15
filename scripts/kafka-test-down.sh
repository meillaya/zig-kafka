#!/usr/bin/env bash
set -euo pipefail

container_name="${KAFKA_TEST_CONTAINER_NAME:-zig-kafka-test-broker}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Kafka integration harness" >&2
  exit 1
fi

if docker inspect "$container_name" >/dev/null 2>&1; then
  echo "Stopping Kafka test broker '$container_name' ..."
  docker rm -f "$container_name" >/dev/null
  echo "Kafka test broker '$container_name' removed."
else
  echo "Kafka test broker '$container_name' is not running."
fi
