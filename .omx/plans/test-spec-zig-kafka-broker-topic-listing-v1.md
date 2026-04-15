# Test Spec: Zig Kafka Educational Broker Topic Listing v1

## Scope
Validation plan for the broker topic-listing milestone defined in `.omx/plans/prd-zig-kafka-broker-topic-listing-v1.md`.

## Test Objectives
1. Prove the broker still passes the bootstrap wire slice.
2. Prove the broker can answer `DescribeTopicPartitions` for a known topic.
3. Prove the broker handles unknown topics deterministically.
4. Prove the broker advertises only the API set it actually supports.

## Test Layers

### 1) Unit tests
#### Topic catalog
- static catalog lookup by topic name
- partition count / leader mapping are deterministic
- unknown topic lookup returns the expected not-found result

#### Protocol codec
- `DescribeTopicPartitions` request parsing for a minimal supported request shape
- response encoding for a known topic
- response encoding for an unknown topic/error case

#### ApiVersions advertisement
- `ApiVersions` advertised list includes:
  - `ApiVersions`
  - `DescribeTopicPartitions`
- no extra unsupported APIs are advertised

### 2) Integration tests
#### Known topic path
- start broker on a test port
- send `DescribeTopicPartitions` request for a known topic
- assert:
  - response frame is valid
  - correlation ID matches
  - topic name and partition listing match the static catalog

#### Unknown topic path
- send `DescribeTopicPartitions` request for an unknown topic
- assert the broker returns the documented deterministic error/listing result

#### Regression path
- existing `ApiVersions` integration test still passes
- existing unsupported-API close-path test still passes

### 3) Full regression checks
- `zig build`
- `zig build test`
- `zig build test-integration`
- existing client roundtrip path remains green

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
1. Broker bootstrap tests still pass.
2. Known-topic `DescribeTopicPartitions` integration test passes.
3. Unknown-topic `DescribeTopicPartitions` integration test passes.
4. `ApiVersions` advertisement accurately reflects the supported API set.
5. No Produce/Fetch/storage behavior is introduced accidentally.
