# PRD: Zig Kafka Educational Broker Malformed Requests v8

## Metadata
- Source context: .omx/context/zig-kafka-broker-malformed-requests-20260415T040231Z.md
- Prior milestone: broker error semantics v7
- Scope: deterministic malformed-request handling
- Status: Planned

## Goal
Teach the educational broker to handle malformed frames/headers deterministically, with explicit close behavior when a valid protocol response cannot be formed.

## Recommended user-visible milestone
“When a client sends a truncated or malformed request, the broker handles it predictably instead of hanging or corrupting state.”

## In Scope
- deterministic handling for truncated frame length/payload cases
- deterministic handling for malformed/incomplete request headers
- connection close behavior when correlation-id-bearing responses are impossible
- focused unit/integration tests for malformed requests

## Out of Scope / Non-goals
- new broker APIs
- durability
- retry policy
- broad protocol error-body synthesis for unrecoverable malformed requests

## Proposed Design
1. Keep malformed-frame/header behavior simple: close the connection when the broker cannot safely parse enough structure to respond.
2. Preserve the newer unsupported-version response behavior only for requests that are parseable enough to identify supported API + version.
3. Add explicit tests for truncated payloads and malformed headers so this contract is documented.

## Acceptance Criteria
1. Existing broker/client tests still pass.
2. Truncated frame payload handling is deterministic and tested.
3. Malformed/incomplete header handling is deterministic and tested.
4. Broker does not hang or corrupt shared state on malformed input.

## Verification Steps
- zig build
- zig build test
- zig build test-integration
- malformed-request integration tests
- existing Kafka client roundtrip still passes

## ADR
### Decision
Make malformed-request handling the next broker milestone.

### Drivers
- Error semantics are now stronger for supported-version mismatches; malformed input is the next natural error class.
- This improves robustness without widening the feature surface.

## Recommended Execution Path
- Best next command: $ralph .omx/plans/prd-zig-kafka-broker-malformed-requests-v8.md
- Companion test spec: .omx/plans/test-spec-zig-kafka-broker-malformed-requests-v8.md
