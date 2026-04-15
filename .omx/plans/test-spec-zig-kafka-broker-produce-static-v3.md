# Test Spec: Zig Kafka Educational Broker Produce Static v3

## Scope
Validation plan for the broker Produce milestone defined in `.omx/plans/prd-zig-kafka-broker-produce-static-v3.md`.

## Test Objectives
1. Prove the broker still passes bootstrap + topic-listing + fetch behavior.
2. Prove the broker can accept deterministic Produce requests into mutable in-memory state.
3. Prove produced data becomes visible through Fetch.
4. Prove unknown-topic and invalid-partition Produce behavior is deterministic.

## Test Layers

### 1) Unit tests
#### Mutable storage
- append single record to known topic/partition
- append multiple records if supported in this slice
- invalid topic/partition mutation errors are deterministic
- existing seeded records remain readable after append

#### Produce handler / codec
- successful Produce response for known topic/partition
- deterministic error response for unknown topic
- deterministic error response for invalid partition
- `ApiVersions` advertisement includes `Produce`

### 2) Integration tests
#### Known-topic Produce
- start broker
- send `Produce` request for known topic/partition
- assert success response with deterministic base offset

#### Produce -> Fetch roundtrip
- produce a record
- fetch from the returned/expected offset
- assert payload roundtrip through broker state

#### Error cases
- unknown topic -> deterministic error response
- invalid partition -> deterministic error response

#### Regression path
- existing `ApiVersions`, unsupported-API close, topic-listing, and fetch tests still pass

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
1. Existing broker bootstrap/topic-listing/fetch tests still pass.
2. Produce success integration test passes.
3. Produce -> Fetch roundtrip integration test passes.
4. Unknown-topic and invalid-partition Produce tests pass.
5. `ApiVersions` accurately reflects the supported API set.
6. No durable storage, compression, or transactional behavior is introduced accidentally.
