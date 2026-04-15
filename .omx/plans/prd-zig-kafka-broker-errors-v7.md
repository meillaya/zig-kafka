# PRD: Zig Kafka Educational Broker Error Semantics v7

## Metadata
- Source context: `.omx/context/zig-kafka-broker-errors-20260415T032946Z.md`
- Prior milestone: broker graceful shutdown v6
- Scope: deterministic protocol-valid error responses for supported APIs
- Status: Planned

## Goal
Teach the educational broker to return deterministic protocol-valid error responses for unsupported versions of supported APIs, instead of silently closing those requests.

## Recommended user-visible milestone
“When a client sends a supported broker API with an unsupported version, the broker responds with a valid Kafka-framed error response rather than dropping the connection.”

## In Scope
- deterministic unsupported-version responses for:
  - ApiVersions (already present; preserve)
  - DescribeTopicPartitions
  - Fetch
  - Produce
- preserve unknown API-key close behavior
- add focused integration tests for unsupported-version responses

## Out of Scope / Non-goals
- new APIs
- durability
- distributed behavior
- broader retry policy or client-side semantics

## Proposed Design
1. Normalize unsupported-version handling in broker dispatch.
2. For supported APIs, parse enough request shape to build a valid error response body.
3. Reuse existing handler/response codecs to emit protocol-valid responses with error code 35 (`UNSUPPORTED_VERSION`).
4. Leave unknown API-key behavior unchanged (clean close).

## Acceptance Criteria
1. Existing broker/client tests still pass.
2. DescribeTopicPartitions unsupported-version request returns a valid response with deterministic unsupported-version signaling.
3. Fetch unsupported-version request returns a valid response with deterministic unsupported-version signaling.
4. Produce unsupported-version request returns a valid response with deterministic unsupported-version signaling.
5. Unknown API-key close behavior remains unchanged.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build test-integration`
- unsupported-version integration tests for DescribeTopicPartitions / Fetch / Produce
- existing Kafka client roundtrip still passes

## ADR
### Decision
Make protocol-valid unsupported-version responses the next broker milestone.

### Drivers
- This improves broker error realism without widening feature scope.
- It builds on the existing handler/codec structure already in place.
- It reduces silent close behavior for supported APIs while preserving unknown-API simplicity.

### Follow-ups
- richer error semantics for malformed requests if still desired later
- optional durability experiments later

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-errors-v7.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-errors-v7.md`
