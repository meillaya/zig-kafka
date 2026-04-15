# PRD: Zig Kafka Educational Broker Concurrent Clients v4

## Metadata
- Source context: `.omx/context/zig-kafka-broker-concurrent-clients-20260415T023857Z.md`
- Upstream roadmap source: `.omx/specs/deep-interview-zig-kafka-from-scratch.md`
- Prior milestone: broker produce static v3
- Scope: Concurrent clients with serial request handling per connection
- Status: Planned

## Requirements Summary
- The roadmap explicitly calls out concurrent clients / serial requests as a next broker capability. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- The current broker handles one accepted connection at a time, so concurrency across clients is the next natural runtime behavior upgrade.
- The broker now has mutable shared storage, so concurrency must preserve correctness of Produce/Fetch under simultaneous client activity.

## Goal
Deliver the next educational broker slice that can serve **multiple simultaneous client connections** while keeping request handling **serial within each connection**.

## Recommended user-visible milestone
“Run the broker, connect multiple clients at once, and have them successfully perform concurrent ApiVersions/DescribeTopicPartitions/Fetch/Produce operations without corrupting shared broker state.”

## Decision
Implement **concurrent clients, serial requests per connection** next.

### Why this is next
- It directly matches the roadmap order.
- It stress-tests the broker’s shared state without requiring request multiplexing within a single connection.
- It improves runtime realism while staying below the complexity of pipelined/concurrent requests per connection.

## In Scope
- Multiple simultaneous accepted client connections
- One handler flow per connection at a time
- Shared-state safety for Produce/Fetch/listing under concurrent clients
- Deterministic concurrent integration tests
- Storage synchronization sufficient for the current in-memory broker state

## Out of Scope / Non-goals
- Concurrent requests within a single connection
- Kafka request pipelining/reordering
- Durable storage
- Compression/transactions
- Major thread-pool/runtime framework introduction
- Cluster/distributed behavior

## Proposed Design
### 1. Per-connection concurrency
Move the broker from inline accept+handle to an accept loop that hands each accepted connection to its own worker thread (or equivalent lightweight concurrent lane).

### 2. Serial handling within a connection
Keep each connection’s request processing simple and serial. For this milestone, one connection may still process one request then close, or a simple loop, but no pipelined concurrency inside one connection.

### 3. Storage synchronization
Protect mutable in-memory storage with a straightforward synchronization primitive so Produce/Fetch cannot race and corrupt state.

### 4. Deterministic concurrency tests
Add integration tests that intentionally overlap operations from multiple clients but still assert deterministic end state and responses.

## Implementation Plan
1. **Refactor broker accept loop for concurrent clients**
   - spawn a worker per accepted connection
   - preserve current request dispatch behavior inside the worker

2. **Synchronize storage access**
   - add a mutex or equivalent around mutable broker state
   - keep reads/writes correct and simple

3. **Preserve protocol behavior**
   - do not widen request semantics while adding concurrency
   - existing handlers should continue to operate unchanged at the wire level

4. **Add concurrency-focused tests**
   - parallel ApiVersions requests
   - parallel Produce requests to same and/or different partitions
   - Produce + Fetch overlap proving shared-state consistency

## Acceptance Criteria
1. Existing broker bootstrap/topic-listing/fetch/produce tests still pass.
2. The broker can serve multiple client connections concurrently.
3. Concurrent Produce requests do not corrupt in-memory state.
4. Concurrent Produce/Fetch operations yield deterministic valid responses.
5. Request handling remains serial within a connection.
6. No pipelined per-connection concurrency is introduced accidentally.

## Risks and Mitigations
- **Risk:** unsafely shared mutable storage corrupts broker state.  
  **Mitigation:** add explicit synchronization around storage access.
- **Risk:** detached connection workers outlive broker shutdown unsafely.  
  **Mitigation:** keep the concurrency model minimal and document shutdown tradeoffs for this educational milestone.
- **Risk:** tests become flaky due to timing-sensitive assertions.  
  **Mitigation:** assert deterministic end state, not exact scheduling order.
- **Risk:** concurrency work accidentally widens request semantics.  
  **Mitigation:** keep all protocol behavior unchanged except connection scheduling.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build test-integration`
- new concurrency integration tests for parallel clients
- existing Kafka client roundtrip still passes

## Suggested File/Module Shape
```text
src/
  server/
    broker.zig
    storage.zig
tests/
  unit/
    server_storage_concurrency_test.zig
  integration/
    broker_concurrent_clients_test.zig
```

## ADR
### Decision
Make **concurrent clients with serial per-connection handling** the next broker milestone.

### Drivers
- It is the next roadmap item after Produce/Fetch/listing. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- It upgrades runtime realism without introducing full request pipelining complexity.
- It stress-tests the newly mutable broker state.

### Alternatives considered
- **Concurrent requests within a single connection now** — rejected as too complex for the next bounded step.
- **Durability next** — rejected because concurrency is the next roadmap/runtime concern.
- **Large thread-pool/runtime refactor** — rejected because a minimal concurrency model is enough for this educational milestone.

### Consequences
- The broker gains real multi-client serving behavior.
- Shared storage must now be explicitly synchronized.
- A later milestone can revisit per-connection pipelining if still desired.

### Follow-ups
- Milestone 5: concurrent requests within a connection, if still desired
- Milestone 6: richer error handling/shutdown semantics and maybe durability experiments

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-concurrent-clients-v4.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-concurrent-clients-v4.md`
