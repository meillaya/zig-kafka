# Test Spec: Zig Kafka Educational Broker Error Semantics v7

## Scope
Validation plan for deterministic unsupported-version responses.

## Test Objectives
1. Prove all prior broker/client milestones still pass.
2. Prove supported APIs return protocol-valid unsupported-version responses.
3. Prove unknown API-key close behavior remains unchanged.

## Test Layers
### Integration tests
- DescribeTopicPartitions unsupported-version response
- Fetch unsupported-version response
- Produce unsupported-version response
- regression: unknown API-key still closes connection

### Full regression checks
- `zig build`
- `zig build test`
- `zig build test-integration`
- existing external Kafka client roundtrip path remains green

## Exit Criteria
1. Existing broker/client tests still pass.
2. Unsupported-version integration tests pass for the supported APIs in scope.
3. Unknown API-key close-path test still passes.
