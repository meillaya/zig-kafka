# Test Spec: Zig Kafka Educational Broker Malformed Requests v8

## Scope
Validation plan for malformed-request handling.

## Test Objectives
1. Prove all prior broker/client milestones still pass.
2. Prove malformed/truncated requests are handled deterministically.
3. Prove the broker closes cleanly when it cannot safely form a response.

## Test Layers
### Unit/integration tests
- truncated frame payload
- incomplete request header
- malformed request body for a supported API
- regression: supported-version mismatch responses still work

### Full regression checks
- zig build
- zig build test
- zig build test-integration
- existing external Kafka client roundtrip path remains green

## Exit Criteria
1. Existing broker/client tests still pass.
2. Malformed-request tests pass.
3. The broker never hangs on malformed input in the covered cases.
