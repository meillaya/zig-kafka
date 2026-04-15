# Test Spec: Zig Kafka Educational Broker Pipelined Requests v5

## Scope
Validation plan for the broker single-connection multi-request milestone.

## Test Objectives
1. Prove all prior broker milestones still pass.
2. Prove one socket can carry multiple requests successfully.
3. Prove responses remain ordered and valid on the same connection.
4. Prove produce->fetch on one connection sees shared broker state correctly.

## Test Layers
### Integration tests
- one connection, multiple sequential ApiVersions/DescribeTopicPartitions requests
- one connection, Produce then Fetch
- optional back-to-back request send before first response is read
- regression: concurrent-client tests still pass

### Full regression checks
- zig build
- zig build test
- zig build test-integration
- existing external Kafka client roundtrip path remains green

## Exit Criteria
1. Existing broker/client tests still pass.
2. Single-connection sequential multi-request tests pass.
3. Produce->Fetch on one connection passes.
4. Response ordering stays deterministic.
