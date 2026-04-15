# Test Spec: Zig Kafka Educational Broker Bootstrap v0

## Scope
Validation plan for the first educational broker milestone defined in `.omx/plans/prd-zig-kafka-broker-bootstrap-v0.md`.

## Test Objectives
1. Prove the broker can listen and accept Kafka-framed requests.
2. Prove request headers are parsed correctly.
3. Prove `ApiVersions` responses are framed correctly and preserve correlation IDs.
4. Prove the client track still builds/tests after broker code is added.

## Test Layers

### 1) Unit tests
#### Frame + header parsing
- decode 4-byte Kafka frame length
- reject truncated frames
- parse request header fields correctly:
  - `api_key`
  - `api_version`
  - `correlation_id`
  - `client_id`

#### Response construction
- response header writes the same `correlation_id` back
- `ApiVersions` response encoding matches the broker’s declared support set
- unsupported-version / unsupported-api behavior matches the documented v0 policy

### 2) Integration tests
#### Broker bootstrap
- start the broker on an ephemeral test port
- open a raw TCP connection or shared Zig transport
- send a valid `ApiVersions` request
- assert:
  - response frame is well-formed
  - correlation ID matches
  - advertised API set includes `ApiVersions`

#### Negative behavior
- send a truncated frame -> broker closes or errors deterministically
- send unsupported API key -> broker follows the documented v0 behavior

### 3) Regression checks
- `zig build`
- `zig build test`
- existing client tests stay green

## Planned Verification Commands
```bash
zig build
zig build test
zig build run-broker -- --port 9094
# plus a broker integration target added during implementation
```

## Exit Criteria
This milestone is complete when:
1. Broker unit tests pass.
2. Broker integration test proves `ApiVersions` roundtrip correctness.
3. Existing client tests still pass.
4. No Fetch/Produce/Metadata/storage behavior slips into this milestone unintentionally.
