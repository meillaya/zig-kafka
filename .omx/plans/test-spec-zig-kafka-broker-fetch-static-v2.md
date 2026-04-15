# Test Spec: Zig Kafka Educational Broker Fetch Static v2

## Scope
Validation plan for the broker fetch milestone defined in `.omx/plans/prd-zig-kafka-broker-fetch-static-v2.md`.

## Test Objectives
1. Prove the broker still passes bootstrap + topic-listing behavior.
2. Prove the broker can return deterministic static records for `Fetch`.
3. Prove empty/unknown/invalid-partition fetch behavior is deterministic.
4. Prove the advertised API set stays honest.

## Test Layers

### 1) Unit tests
#### Static storage
- known topic/partition record lookup
- offset-based slice selection
- beyond-end offset returns empty
- unknown topic / invalid partition returns expected errors

#### Fetch handler / codec
- encode fetch response with a record batch for known data
- encode fetch response with empty records for beyond-end offset
- encode deterministic error responses for unknown topic / invalid partition

#### ApiVersions advertisement
- advertised set includes:
  - `ApiVersions`
  - `DescribeTopicPartitions`
  - `Fetch`
- no unsupported extras are advertised

### 2) Integration tests
#### Known topic fetch
- start broker
- send `Fetch` request for a known topic/partition/offset
- assert:
  - correlation ID matches
  - response is well-formed
  - returned record payloads match the static record catalog

#### Beyond-end fetch
- fetch from an offset larger than the last static record offset
- assert valid empty response

#### Error cases
- unknown topic -> deterministic broker error response
- invalid partition -> deterministic broker error response

#### Regression path
- existing `ApiVersions` integration test still passes
- existing unsupported-API close-path test still passes
- existing `DescribeTopicPartitions` known/unknown topic tests still pass

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
1. Existing broker bootstrap + topic-listing tests still pass.
2. Known-topic fetch integration test passes.
3. Beyond-end fetch integration test passes.
4. Unknown-topic and invalid-partition fetch tests pass.
5. `ApiVersions` accurately reflects the supported API set.
6. No Produce or durable storage behavior is introduced accidentally.
