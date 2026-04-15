# Test Spec: Zig Kafka Educational Broker Graceful Shutdown v6

## Scope
Validation plan for graceful shutdown and worker tracking.

## Test Objectives
1. Prove all prior broker/client milestones still pass.
2. Prove shutdown waits for in-flight worker completion.
3. Prove no deadlock/regression in normal broker operation.

## Test Layers
### Integration tests
- start broker with artificial request delay
- issue a request
- call `requestStop()` while request is in flight
- assert the response still succeeds and the broker thread exits cleanly

### Full regression checks
- `zig build`
- `zig build test`
- `zig build test-integration`
- existing external Kafka client roundtrip path remains green

## Exit Criteria
1. Existing broker/client tests still pass.
2. Graceful-shutdown integration test passes.
3. Broker exits cleanly after waiting for workers.
