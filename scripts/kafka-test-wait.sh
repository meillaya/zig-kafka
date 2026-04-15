#!/usr/bin/env bash
set -euo pipefail

container_name="${KAFKA_TEST_CONTAINER_NAME:-zig-kafka-test-broker}"
wait_timeout="${KAFKA_TEST_WAIT_TIMEOUT_SECONDS:-90}"
topic_name="${KAFKA_TEST_TOPIC:-zig-kafka-v1-test}"
require_topic="${KAFKA_TEST_REQUIRE_TOPIC:-0}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Kafka integration harness" >&2
  exit 1
fi

if ! docker inspect "$container_name" >/dev/null 2>&1; then
  echo "Kafka test container '$container_name' does not exist" >&2
  exit 1
fi

deadline=$((SECONDS + wait_timeout))
while (( SECONDS < deadline )); do
  if docker exec "$container_name" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    if [[ "$require_topic" != "1" ]]; then
      echo "Kafka test broker '$container_name' is accepting requests."
      exit 0
    fi

    if docker exec "$container_name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$topic_name" >/dev/null 2>&1; then
      echo "Kafka test broker '$container_name' is ready with topic '$topic_name'."
      exit 0
    fi
  fi
  sleep 2
done

echo "Timed out waiting for Kafka test broker '$container_name' to become ready" >&2
docker logs "$container_name" --tail 200 >&2 || true
exit 1
