#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container_name="${KAFKA_TEST_CONTAINER_NAME:-zig-kafka-test-broker}"
image="${KAFKA_TEST_IMAGE:-apache/kafka:4.0.2}"
host="${KAFKA_TEST_HOST:-127.0.0.1}"
port="${KAFKA_TEST_PORT:-9092}"
topic_name="${KAFKA_TEST_TOPIC:-zig-kafka-v1-test}"
partition_count="${KAFKA_TEST_PARTITIONS:-1}"
replication_factor="${KAFKA_TEST_REPLICATION_FACTOR:-1}"
bootstrap_server="${KAFKA_TEST_BOOTSTRAP_SERVER:-${host}:${port}}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Kafka integration harness" >&2
  exit 1
fi

existing_status=""
if docker inspect "$container_name" >/dev/null 2>&1; then
  existing_status="$(docker inspect -f '{{.State.Status}}' "$container_name")"
fi

if [[ "$existing_status" != "running" ]]; then
  if [[ -n "$existing_status" ]]; then
    docker rm -f "$container_name" >/dev/null 2>&1 || true
  fi

  echo "Starting Kafka test broker '$container_name' from $image ..."
  docker pull "$image" >/dev/null
  docker run \
    --detach \
    --name "$container_name" \
    --label "org.zig-kafka.harness=true" \
    --publish "${port}:9092" \
    "$image" >/dev/null
else
  echo "Reusing running Kafka test broker '$container_name'."
fi

echo "Waiting for Kafka test broker readiness ..."
"$repo_root/scripts/kafka-test-wait.sh"

echo "Ensuring topic '$topic_name' exists ..."
docker exec "$container_name" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$topic_name" \
  --partitions "$partition_count" \
  --replication-factor "$replication_factor" >/dev/null

KAFKA_TEST_REQUIRE_TOPIC=1 "$repo_root/scripts/kafka-test-wait.sh"

docker exec "$container_name" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic "$topic_name"

cat <<INFO
Kafka integration harness is ready.
  bootstrap server: $bootstrap_server
  topic:            $topic_name
  partitions:       $partition_count
  container:        $container_name

Suggested next steps:
  zig test tests/integration/kafka_roundtrip_test.zig
  # or later, once build.zig wires the target:
  # zig build test-integration
INFO
