# PRD: Zig Kafka Educational Broker Produce Static v3

## Metadata
- Source context: `.omx/context/zig-kafka-broker-produce-static-20260415T022342Z.md`
- Upstream roadmap source: `.omx/specs/deep-interview-zig-kafka-from-scratch.md`
- Prior milestone: broker fetch static v2
- Scope: Educational single-broker Produce into in-memory/static storage
- Status: Planned

## Requirements Summary
- The roadmap places Produce after Fetch in the educational broker track. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- The broker already has static in-memory storage and a working Fetch path, making Produce the natural next write-path milestone.
- The client path already includes shared `Produce` protocol and record-batch machinery that should be reused where practical.

## Goal
Deliver the next educational broker slice that can accept **Produce** requests and append records into the same in-memory storage model that static Fetch already reads from.

## Recommended user-visible milestone
“Run the local Zig broker, produce records into a known topic/partition, then fetch them back from the same in-memory broker state.”

## Decision
Implement **Produce into mutable in-memory broker storage** next, without introducing durable persistence.

### Why this is next
- It directly follows the roadmap order.
- It closes the loop on the first educational read/write broker cycle.
- It reuses the static/in-memory model already established by the Fetch milestone.

## In Scope
- Add broker support for `Produce`
- Advertise `Produce` in `ApiVersions`
- Evolve the server storage model from static-only into mutable in-memory appendable state
- Support deterministic Produce behavior for:
  - known topic/partition success
  - unknown topic
  - invalid partition
  - single record append
  - small multi-record append if it stays narrow enough
- Prove that produced records are visible through the existing Fetch path

## Out of Scope / Non-goals
- Durable storage
- Compression
- Idempotence / transactions
- Replication / ISR realism
- Consumer groups / offset commits
- Admin APIs
- Background concurrency redesign
- Produce to multiple topics/partitions in one broad milestone unless it falls out naturally with low complexity

## Proposed Design
### 1. Mutable in-memory storage
Refactor `src/server/storage.zig` so broker state can append records at runtime while preserving deterministic bootstrap defaults.

### 2. Shared protocol-first Produce handler
Add a broker-side `Produce` handler that reuses the shared `Produce` codec and the existing record-batch decoding/encoding pipeline where feasible.

### 3. Honest ApiVersions advertisement
Update `ApiVersions` to advertise:
- `ApiVersions`
- `DescribeTopicPartitions`
- `Fetch`
- `Produce`

Only after the Produce handler is implemented and green.

### 4. Deterministic educational semantics
For v3 produce behavior:
- known topic + valid partition -> append records and return deterministic base offset
- unknown topic -> deterministic Kafka error response
- invalid partition -> deterministic Kafka error response
- appended data must be visible through subsequent Fetch requests in the same broker process

## Implementation Plan
1. **Refactor storage into mutable broker state**
   - Add append support for topic/partition record arrays
   - Preserve deterministic initial records for tests

2. **Add broker Produce handler**
   - New `src/server/handlers/produce.zig`
   - Route `Produce` in `src/server/broker.zig`

3. **Reuse shared protocol code**
   - Reuse/adapt `src/protocol/produce.zig`
   - Reuse `src/protocol/record_batch.zig` for record decoding from produce payloads

4. **Update ApiVersions advertisement**
   - Add `Produce` once the handler is wired and tested

5. **Add tests**
   - Unit tests for mutable append behavior
   - Integration tests for produce success/error paths
   - Integration test proving Produce -> Fetch roundtrip inside the broker

## Acceptance Criteria
1. Existing broker bootstrap, topic-listing, and fetch tests still pass.
2. `ApiVersions` now advertises `Produce` in addition to prior broker APIs.
3. A broker integration test can produce to a known topic/partition successfully.
4. Produced records can be fetched back via the broker’s existing Fetch path.
5. Unknown-topic and invalid-partition Produce behavior are deterministic and tested.
6. No durable storage, compression, or transactional semantics are introduced in this milestone.

## Risks and Mitigations
- **Risk:** mutable storage breaks deterministic read behavior.  
  **Mitigation:** preserve seeded initial state and add produce->fetch regression tests.
- **Risk:** Produce handling diverges from the shared client protocol layer.  
  **Mitigation:** reuse shared protocol codecs and record-batch logic where practical.
- **Risk:** storage mutation drifts toward durable design prematurely.  
  **Mitigation:** keep all state in-memory and process-local only.
- **Risk:** appending many records/partitions widens scope too fast.  
  **Mitigation:** keep the first slice narrow and right-size batch support.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build test-integration`
- Produce success integration test
- Produce -> Fetch roundtrip integration test
- Existing Kafka client roundtrip still passes

## Suggested File/Module Shape
```text
src/
  server/
    storage.zig
    handlers/
      produce.zig
tests/
  unit/
    server_storage_mutation_test.zig
    server_produce_handler_test.zig
  integration/
    broker_produce_static_test.zig
```

## ADR
### Decision
Make **Produce into mutable in-memory storage** the next broker milestone.

### Drivers
- It is the next roadmap item after Fetch. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- It completes the first broker read/write loop.
- It builds directly on the static in-memory storage model already in place.

### Alternatives considered
- **Jump to concurrency** — rejected because richer broker behavior matters more than concurrency right now.
- **Introduce durable storage first** — rejected because persistence would overwhelm the next educational milestone.
- **Broaden Metadata/listing instead** — rejected because Produce is the next roadmap step and delivers more learning value.

### Consequences
- The broker moves from read-only in-memory state to mutable in-memory state.
- Future milestones can explore richer semantics on top of a functioning Produce/Fetch loop.
- The system remains educational and intentionally non-durable.

### Follow-ups
- Milestone 5: richer broker error handling and maybe concurrent clients/requests
- Milestone 6: optional durability experiments if still desired later

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-produce-static-v3.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-produce-static-v3.md`
