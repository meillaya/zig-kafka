# Test Spec: Zig Kafka Educational Broker Concurrent Clients v4

## Scope
Validation plan for the broker concurrent-clients milestone defined in `.omx/plans/prd-zig-kafka-broker-concurrent-clients-v4.md`.

## Test Objectives
1. Prove the broker still passes all prior milestone behavior.
2. Prove the broker can handle multiple simultaneous client connections.
3. Prove mutable broker state remains correct under concurrent Produce/Fetch activity.
4. Prove request handling stays serial within a connection.

## Test Layers

### 1) Unit tests
#### Storage synchronization
- concurrent append/read smoke tests against shared storage
- no corruption of record count / ordering invariants

### 2) Integration tests
#### Parallel ApiVersions
- multiple clients connect at once and all receive valid responses

#### Parallel Produce
- multiple clients produce to the same topic/partition and/or different partitions
- final fetch view reflects all appended records without corruption

#### Produce + Fetch overlap
- one client produces while another fetches
- responses remain valid and shared state remains coherent

### 3) Full regression checks
- `zig build`
- `zig build test`
- `zig build test-integration`
- existing external Kafka client roundtrip path remains green

## Planned Verification Commands
```bash
zig build
zig build test
zig build test-integration
./scripts/kafka-test-up.sh
KAFKA_BROKER=127.0.0.1:9092 KAFKA_TOPIC=zig-kafka-v1-test KAFKA_PARTITION=0 zig build run-roundtrip
./scripts/kafka-test-down.sh
```

## Exit Criteria
This milestone is complete when:
1. All prior broker milestone tests still pass.
2. Concurrent-client integration tests pass reliably.
3. Shared in-memory state stays coherent under concurrent Produce/Fetch.
4. No per-connection request pipelining is introduced accidentally.
