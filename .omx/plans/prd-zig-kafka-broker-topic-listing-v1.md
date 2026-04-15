# PRD: Zig Kafka Educational Broker Topic Listing v1

## Metadata
- Source context: `.omx/context/zig-kafka-broker-topic-listing-20260415T013842Z.md`
- Upstream roadmap source: `.omx/specs/deep-interview-zig-kafka-from-scratch.md`
- Prior milestone: broker bootstrap v0
- Scope: Educational single-broker topic/partition listing
- Status: Planned

## Requirements Summary
- The deep interview roadmap places partition listing immediately after the bootstrap wire slice and explicitly references `DescribeTopicPartitions`. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- The current broker already handles TCP accept, frame parsing, request-header parsing, `ApiVersions`, and deterministic unsupported-API close behavior, providing the right substrate for a second broker milestone. `src/server/broker.zig:8-80`
- The broker currently advertises only `ApiVersions`, so the next milestone must expand the advertised API set and add a second request handler without destabilizing the bootstrap slice. `src/server/handlers/api_versions.zig:6-12`

## Goal
Deliver the next educational broker slice that can answer **topic/partition listing** requests for a small, static topic catalog, while keeping the server single-broker and storage-free.

## Recommended user-visible milestone
“Run the local Zig broker, ask it for partition information for a known topic, and get a deterministic topic/partition listing response back.”

## Decision
Implement **`DescribeTopicPartitions`** as the next broker milestone, not Produce/Fetch/Metadata/storage.

### Why this is next
- It directly matches the user’s staged broker roadmap.
- It broadens request dispatch in a meaningful way without dragging in data-plane storage.
- It lets the broker start modeling a tiny topic catalog that later Fetch/Produce can build on.

## In Scope
- Add broker support for `DescribeTopicPartitions`
- Advertise `DescribeTopicPartitions` in `ApiVersions`
- Introduce a tiny static topic catalog (for example one or more configured topic definitions with partition counts)
- Return deterministic listing results for:
  - known topic
  - unknown topic
  - optionally multiple topics if it stays small enough
- Keep the broker single-node and in-memory/config-driven
- Add unit and integration coverage for the new request/response path

## Out of Scope / Non-goals
- Produce
- Fetch
- Disk-backed storage
- Replication / ISR / controller realism
- Dynamic topic creation or admin APIs beyond listing
- Consumer groups / offsets
- TLS / SASL
- Background concurrency redesign

## Proposed Design
### 1. Static topic catalog
Add a small server-side topic catalog module, likely under `src/server/catalog.zig` or `src/server/topics.zig`, with definitions like:
- topic name
- partition count
- optional per-partition leader id (fixed to the single broker)

This catalog should be deterministic and easy to extend later.

### 2. Shared protocol-first handler
Add a `DescribeTopicPartitions` protocol schema/handler path by reusing:
- existing frame parsing
- existing request header parsing
- existing broker dispatch structure

Add a new handler module, likely:
- `src/server/handlers/describe_topic_partitions.zig`

### 3. Honest ApiVersions advertisement
Update `src/server/handlers/api_versions.zig` so the broker advertises:
- `ApiVersions`
- `DescribeTopicPartitions`

Only advertise what the broker truly handles.

### 4. Deterministic error behavior
For v1 listing behavior:
- known topic -> return deterministic partition listing
- unknown topic -> return a deterministic Kafka error/listing form and lock it in tests

Recommendation: return an explicit unknown-topic response rather than silently closing the connection.

## Implementation Plan
1. **Add protocol schema support for DescribeTopicPartitions**
   - Introduce the request/response codec under `src/protocol/**` if needed.
   - Keep it narrow to only the fields required for the educational broker slice.

2. **Add a static topic catalog**
   - New module under `src/server/**`
   - Hard-code one or a few topic definitions for now

3. **Extend broker dispatch**
   - Route `DescribeTopicPartitions` requests in `src/server/broker.zig`
   - Preserve existing `ApiVersions` handling unchanged

4. **Update ApiVersions advertisement**
   - Add `DescribeTopicPartitions` to the supported API list only after the handler exists

5. **Add tests**
   - Unit tests for the topic catalog and response encoding
   - Integration tests for:
     - known topic
     - unknown topic
     - `ApiVersions` still advertising the correct API set

## Acceptance Criteria
1. The broker still passes all existing bootstrap tests.
2. `ApiVersions` now advertises `DescribeTopicPartitions` support in addition to `ApiVersions`.
3. A client/integration test can send a `DescribeTopicPartitions` request for a known topic and receive a deterministic partition listing.
4. A request for an unknown topic yields the documented deterministic error/listing behavior.
5. Existing client build/tests and broker bootstrap tests remain green.
6. No Produce/Fetch/storage logic is introduced in this milestone.

## Risks and Mitigations
- **Risk:** request/response schema scope expands too far.  
  **Mitigation:** implement only the minimal fields needed for known/unknown topic listing.
- **Risk:** broker starts advertising APIs it cannot fully support.  
  **Mitigation:** add `DescribeTopicPartitions` to `ApiVersions` only after the handler is wired and tested.
- **Risk:** topic catalog design becomes speculative storage architecture.  
  **Mitigation:** keep the catalog static and in-memory; no persistence yet.
- **Risk:** the broker/client shared protocol layer gets fragmented.  
  **Mitigation:** continue adding protocol definitions under shared `src/protocol/**` where practical.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build test-integration`
- broker integration test proving `DescribeTopicPartitions` known-topic path
- broker integration test proving unknown-topic behavior
- existing client roundtrip still passes

## Suggested File/Module Shape
```text
src/
  protocol/
    describe_topic_partitions.zig
  server/
    catalog.zig
    handlers/
      describe_topic_partitions.zig
tests/
  unit/
    server_catalog_test.zig
    describe_topic_partitions_codec_test.zig
  integration/
    broker_describe_topic_partitions_test.zig
```

## ADR
### Decision
Make **topic/partition listing via DescribeTopicPartitions** the next broker milestone.

### Drivers
- It is the next roadmap item after the bootstrap wire slice. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- It exercises the next level of request dispatch without introducing storage.
- It creates a topic catalog surface that later Fetch/Produce work can build on.

### Alternatives considered
- **Jump to Produce/Fetch** — rejected because storage and record handling would overwhelm the next slice.
- **Add Metadata instead of DescribeTopicPartitions** — viable, but rejected because the user’s roadmap explicitly calls out `DescribeTopicPartitions` for partition listing.
- **Do concurrency next** — rejected because richer request surface matters more than concurrency for the next educational step.

### Consequences
- The broker evolves from “wire bootstrap” to “wire bootstrap + static catalog”.
- Future milestones can reuse the static catalog when adding Produce/Fetch.
- The broker remains educational and deliberately narrow.

### Follow-ups
- Milestone 3: Fetch over in-memory/static records
- Milestone 4: Produce into the same in-memory/static storage
- Milestone 5: richer error handling and maybe concurrent clients/requests

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-topic-listing-v1.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-topic-listing-v1.md`
