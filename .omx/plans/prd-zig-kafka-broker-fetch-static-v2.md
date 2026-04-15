# PRD: Zig Kafka Educational Broker Fetch Static v2

## Metadata
- Source context: `.omx/context/zig-kafka-broker-fetch-static-20260415T020003Z.md`
- Upstream roadmap source: `.omx/specs/deep-interview-zig-kafka-from-scratch.md`
- Prior milestone: broker topic listing v1
- Scope: Educational single-broker fetch over static in-memory records
- Status: Planned

## Requirements Summary
- The user roadmap places Fetch immediately after broker topic listing and before Produce. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- The repo already has shared protocol pieces for `Fetch` and record batches from the client path. `src/protocol/fetch.zig` `src/protocol/record_batch.zig`
- The broker already has a static topic catalog and request dispatch substrate, so the next natural expansion is to return deterministic record data for explicit fetch requests.

## Goal
Deliver the next educational broker slice that can answer **Fetch** requests against a small static in-memory record catalog, without yet implementing Produce or durable storage.

## Recommended user-visible milestone
“Run the local Zig broker, fetch from a known topic/partition/offset, and receive deterministic records back from static in-memory data.”

## Decision
Implement **Fetch over static in-memory records** next, reusing the shared `Fetch` and `RecordBatch` protocol layer where feasible.

### Why this is next
- It directly matches the roadmap order.
- It expands the broker into the first data-plane read path without forcing write/storage complexity.
- It creates the exact data model that a later Produce milestone can append into.

## In Scope
- Add broker support for `Fetch`
- Advertise `Fetch` in `ApiVersions`
- Introduce a static in-memory record catalog keyed by topic + partition
- Return deterministic responses for:
  - known topic/partition with records
  - known topic/partition with offset beyond end
  - unknown topic
  - known topic with invalid partition
- Reuse or adapt shared `Fetch` + `RecordBatch` codec logic
- Add unit and integration coverage for known/unknown/empty fetch behavior

## Out of Scope / Non-goals
- Produce
- Durable storage
- Replication / ISR realism beyond fixed single-broker values
- Consumer groups / offset commits
- Metadata overhaul beyond what already exists
- Compression
- TLS / SASL
- Background concurrency redesign

## Proposed Design
### 1. Static record catalog
Add a small in-memory record store, likely under `src/server/storage.zig` or an expanded `src/server/catalog.zig`, containing:
- topic name
- partition index
- ordered static records
- synthetic offsets based on array position

### 2. Shared protocol-first Fetch handler
Prefer reusing the existing shared `src/protocol/fetch.zig` and `src/protocol/record_batch.zig` rather than inventing broker-only framing.

### 3. Honest ApiVersions advertisement
Update the broker’s `ApiVersions` support set to include:
- `ApiVersions`
- `DescribeTopicPartitions`
- `Fetch`

### 4. Deterministic broker semantics
For v2 educational fetch behavior:
- known topic + partition + valid/earlier offset -> return all matching static records from that offset onward (bounded if necessary)
- known topic + partition + offset beyond end -> return empty records
- unknown topic -> deterministic Kafka error response
- invalid partition -> deterministic Kafka error response

## Implementation Plan
1. **Add static record storage**
   - New server module for static records
   - Tie records to the existing static topic catalog

2. **Add broker fetch handler**
   - New `src/server/handlers/fetch.zig`
   - Route `Fetch` in `src/server/broker.zig`

3. **Reuse shared protocol codecs**
   - Use or minimally extend `src/protocol/fetch.zig`
   - Use `src/protocol/record_batch.zig` for encoded records

4. **Update ApiVersions advertisement**
   - Add `Fetch` only once the handler is live and tested

5. **Add tests**
   - Unit tests for static record selection by offset
   - Integration tests for known-topic fetch, empty fetch, unknown topic, invalid partition
   - Regression checks for existing broker bootstrap + topic-listing paths

## Acceptance Criteria
1. Existing broker bootstrap and topic-listing tests still pass.
2. `ApiVersions` now advertises `Fetch` support in addition to existing broker APIs.
3. A broker integration test can fetch deterministic records for a known topic/partition.
4. Fetching beyond the end returns an empty-but-valid response.
5. Unknown topic and invalid partition behavior are deterministic and tested.
6. No Produce or durable storage behavior is introduced in this milestone.

## Risks and Mitigations
- **Risk:** broker-side Fetch diverges from the client’s shared protocol expectations.  
  **Mitigation:** reuse the shared `Fetch` and `RecordBatch` codec code where possible.
- **Risk:** static records turn into accidental storage design.  
  **Mitigation:** keep them hard-coded/in-memory and explicitly educational.
- **Risk:** fetch semantics become too broad too quickly.  
  **Mitigation:** support only explicit topic/partition/offset reads in v2.
- **Risk:** ApiVersions advertises support before behavior is solid.  
  **Mitigation:** add `Fetch` to the advertised set only with green tests.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build test-integration`
- broker integration test for known-topic fetch
- broker integration test for unknown-topic/invalid-partition fetch behavior
- existing client roundtrip still passes

## Suggested File/Module Shape
```text
src/
  server/
    storage.zig
    handlers/
      fetch.zig
tests/
  unit/
    server_storage_test.zig
    server_fetch_handler_test.zig
  integration/
    broker_fetch_static_test.zig
```

## ADR
### Decision
Make **Fetch over static in-memory records** the next broker milestone.

### Drivers
- It is the next roadmap item after topic listing. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- It adds the first broker read path without forcing writes or storage persistence.
- It sets up the data model that a later Produce milestone can mutate.

### Alternatives considered
- **Jump to Produce next** — rejected because write semantics without an established read path/storage model would be premature.
- **Do concurrency next** — rejected because richer broker behavior matters more than concurrency right now.
- **Expand Metadata further** — rejected because Fetch is the next roadmap milestone and adds more educational value.

### Consequences
- The broker grows from static listing into static read behavior.
- The data model stays intentionally in-memory and educational.
- A later Produce milestone can append to the same static/in-memory representation.

### Follow-ups
- Milestone 4: Produce into the same in-memory/static storage
- Milestone 5: richer broker error handling and maybe concurrent clients/requests

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-fetch-static-v2.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-fetch-static-v2.md`
