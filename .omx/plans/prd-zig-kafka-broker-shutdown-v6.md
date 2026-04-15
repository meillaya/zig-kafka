# PRD: Zig Kafka Educational Broker Graceful Shutdown v6

## Metadata
- Source context: `.omx/context/zig-kafka-broker-shutdown-20260415T030546Z.md`
- Prior milestone: broker pipelined requests v5
- Scope: Graceful broker shutdown with worker tracking
- Status: Planned

## Goal
Teach the educational broker to shut down gracefully by tracking in-flight worker threads and waiting for them to finish before returning from shutdown.

## Recommended user-visible milestone
“When shutdown is requested, the broker stops accepting new work, lets in-flight requests complete, and exits cleanly.”

## In Scope
- Track per-connection worker lifecycle
- Graceful shutdown via worker wait/join coordination
- Deterministic tests for shutdown during in-flight request handling
- Preserve all existing request semantics

## Out of Scope / Non-goals
- Per-request cancellation
- Durable shutdown persistence
- Protocol-surface changes
- Major runtime framework changes

## Proposed Design
1. Add a `std.Thread.WaitGroup` to `Broker` for worker tracking.
2. Increment on worker spawn, decrement on worker completion.
3. `requestStop()` should wake the accept loop and then wait for in-flight workers to finish.
4. Keep worker behavior otherwise unchanged.

## Acceptance Criteria
1. Existing broker/client tests still pass.
2. Broker shutdown waits for an in-flight delayed request to finish.
3. Worker tracking does not deadlock the broker under normal shutdown.
4. No request semantics are changed beyond shutdown behavior.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build test-integration`
- graceful-shutdown integration test with delayed request
- existing Kafka client roundtrip still passes

## ADR
### Decision
Make graceful shutdown + worker tracking the next broker milestone.

### Drivers
- The broker now has detached worker threads and needs a clean lifecycle model.
- This is the natural runtime follow-up after concurrent clients and pipelined requests.

### Follow-ups
- richer error semantics
- optional durability experiments later

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-shutdown-v6.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-shutdown-v6.md`
