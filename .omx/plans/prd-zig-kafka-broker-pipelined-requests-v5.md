# PRD: Zig Kafka Educational Broker Pipelined Requests v5

## Metadata
- Source context: .omx/context/zig-kafka-broker-pipelined-requests-20260415T025741Z.md
- Prior milestone: broker concurrent clients v4
- Scope: concurrent/sequential requests within a single connection
- Status: Planned

## Goal
Extend the educational broker so one client connection can issue multiple requests without reconnecting, and optionally pipeline requests while preserving deterministic response behavior.

## Recommended user-visible milestone
“One client connection can send multiple broker requests back-to-back and receive valid responses in the expected order without reconnecting.”

## In Scope
- multi-request handling on a single connection
- deterministic response ordering on a single connection
- optional limited pipelining/back-to-back framed requests
- integration tests for mixed request sequences on one socket
- preserve current multi-client concurrency support

## Out of Scope / Non-goals
- durable storage
- compression/transactions
- distributed behavior
- protocol reordering semantics that diverge from a simple educational FIFO model

## Proposed Design
1. Refactor the per-connection worker into a small request loop instead of one-request-and-close.
2. Preserve FIFO response ordering for requests received on the same socket.
3. Keep processing simple: sequential request execution per connection is acceptable first, with pipelined reads only if it remains small and testable.
4. Add deterministic tests that send ApiVersions/DescribeTopicPartitions/Fetch/Produce sequences over one socket.

## Acceptance Criteria
1. Existing broker bootstrap/topic-listing/fetch/produce/concurrent-client tests still pass.
2. One connection can successfully send multiple sequential requests without reconnecting.
3. A single connection can send back-to-back requests and receive valid ordered responses.
4. Produce then Fetch on the same connection works against the shared in-memory broker state.
5. No durability or protocol-reordering complexity is introduced accidentally.

## Risks and Mitigations
- Risk: response ordering bugs on one connection.  
  Mitigation: lock the behavior with ordered integration tests.
- Risk: accidental scope creep into fully general concurrent request scheduling.  
  Mitigation: keep the milestone FIFO and educational.
- Risk: connection-loop shutdown behavior becomes messy.  
  Mitigation: keep the first version simple and bounded.

## Verification Steps
- zig build
- zig build test
- zig build test-integration
- one-socket sequential request integration test
- one-socket produce-then-fetch integration test
- existing external Kafka client roundtrip still passes

## ADR
### Decision
Make concurrent/sequential requests within a single connection the next broker milestone.

### Drivers
- It is the next roadmap item after concurrent clients.
- It improves protocol realism without forcing durability or cluster complexity.
- It exercises the broker’s request framing/dispatch path more deeply on a single socket.

### Follow-ups
- richer shutdown/error semantics
- durability experiments if still desired later

## Recommended Execution Path
- Best next command: $ralph .omx/plans/prd-zig-kafka-broker-pipelined-requests-v5.md
- Companion test spec: .omx/plans/test-spec-zig-kafka-broker-pipelined-requests-v5.md
