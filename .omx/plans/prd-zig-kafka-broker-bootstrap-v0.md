# PRD: Zig Kafka Educational Broker Bootstrap v0

## Metadata
- Source context: `.omx/context/zig-kafka-broker-bootstrap-20260415T005622Z.md`
- Upstream scope reference: `.omx/specs/deep-interview-zig-kafka-from-scratch.md`
- Scope: First educational single-broker server milestone
- Status: Planned

## Requirements Summary
- The next track after the client is an **educational single-broker Kafka-like server** guided by the user’s staged roadmap, which begins with port binding, correlation IDs, API version parsing, and `ApiVersions` handling. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- The repo already contains reusable protocol modules for request headers and `ApiVersions`, so the server should build on those instead of duplicating them. `src/lib.zig:7-15` `src/protocol/header.zig:4-24` `src/protocol/api_versions.zig:5-58`
- The current build only exposes client-side test/example targets, so the server milestone must add a broker run target and broker-specific tests without regressing the client flow. `build.zig:13-57`

## Goal
Deliver the smallest useful educational broker slice that can:
1. bind and listen on a TCP port,
2. accept Kafka-framed requests,
3. parse request headers (`api_key`, `api_version`, `correlation_id`, `client_id`),
4. respond correctly to `ApiVersions`, and
5. echo the request correlation ID in the response header.

## In Scope
- Single-process, single-broker educational server
- Plaintext TCP listener
- Kafka frame-length handling
- Request-header parsing
- Minimal dispatcher keyed by `api_key`
- `ApiVersions` response generation advertising the broker’s currently supported APIs
- Deterministic unit and integration tests for the bootstrap slice
- Serial request handling per connection
- Reuse of shared protocol modules where appropriate

## Out of Scope / Non-goals
- Metadata / DescribeTopicPartitions
- Fetch / Produce
- Disk-backed logs or retention
- Topic or partition storage models
- Consumer groups / offset commits
- TLS / SASL / ACLs
- Compression / record-batch storage
- Cluster membership / replication
- Background concurrency beyond a simple accept loop

## Proposed Milestone Shape
### Recommended user-visible milestone
“Run a local Zig broker, send it an `ApiVersions` request, and receive a valid Kafka-framed response with the same correlation ID and a declared supported API set.”

### Design choice
Prefer a **shared-protocol, minimal-server-loop** approach:
- keep protocol definitions in `src/protocol/**`
- add a new `src/server/**` layer for listening, framing, parsing, dispatch, and response writing
- keep server behavior intentionally tiny until wire correctness is stable

## Implementation Plan
1. **Add broker executable target**
   - Extend `build.zig` with a broker executable and a `run-broker` step.
   - Add a broker integration test step if useful.
   - Likely files: `build.zig`, new `src/server/main.zig` or `examples/broker.zig`.

2. **Add server config + listener loop**
   - Create `src/server/config.zig` and `src/server/broker.zig`.
   - Support explicit host/port config.
   - Accept TCP connections and read Kafka length-prefixed frames.

3. **Parse request headers and dispatch**
   - Add a request parser that reuses `src/protocol/primitives.zig` and `src/protocol/header.zig`.
   - Represent inbound requests as a small internal `ServerRequest` struct.
   - Dispatch only `ApiVersions` in v0.

4. **Implement ApiVersions response path**
   - Add `src/server/handlers/api_versions.zig` or equivalent.
   - Reuse `src/protocol/api_versions.zig` definitions where possible.
   - Include supported API ranges for at least:
     - `ApiVersions`
   - Optionally advertise future placeholders only if the broker can truthfully handle them; otherwise keep the list honest and tiny.

5. **Handle unsupported APIs/versions deterministically**
   - For v0, prefer one explicit behavior and lock it in tests:
     - either return a valid error-coded response where practical, or
     - close the connection cleanly after parsing an unsupported request.
   - Recommendation: close cleanly for non-`ApiVersions` requests in v0 to avoid speculative protocol surface.

6. **Add test coverage**
   - Unit-test frame decoding, header parsing, and response-header correlation behavior.
   - Integration-test the broker by spinning it up on a local port and sending a raw or shared-code `ApiVersions` probe.

## Acceptance Criteria
1. `zig build run-broker` (or equivalent) starts a local broker on a configured port.
2. A client/test can open a TCP connection and send a Kafka-framed `ApiVersions` request.
3. The broker parses `api_key`, `api_version`, and `correlation_id` correctly.
4. The broker returns a valid Kafka-framed response header with the **same correlation ID**.
5. The broker returns a deterministic `ApiVersions` payload that matches the implemented server surface.
6. Unsupported APIs are handled in the documented v0 manner.
7. Existing client build/tests still pass after the server code lands.

## Risks and Mitigations
- **Risk:** server code forks protocol logic from the client path.  
  **Mitigation:** keep wire primitives/header/API schemas shared in `src/protocol/**`.
- **Risk:** unsupported API/version behavior becomes ambiguous.  
  **Mitigation:** choose one explicit v0 policy and codify it in integration tests.
- **Risk:** build targets become muddled between client and server flows.  
  **Mitigation:** add distinct broker run/test steps rather than overloading the existing client example path.
- **Risk:** concurrency creeps into the bootstrap milestone.  
  **Mitigation:** keep serial per-connection processing in v0; defer broader concurrency to the next broker milestone.

## Verification Steps
- `zig build`
- `zig build test`
- `zig build run-broker -- --port <test-port>`
- broker integration test proving ApiVersions roundtrip
- re-run existing client integration tests to confirm the client track stays green

## Suggested File/Module Shape
```text
src/
  server/
    config.zig
    broker.zig
    connection.zig
    request.zig
    handlers/
      api_versions.zig
examples/
  broker.zig
tests/
  unit/
    server_request_test.zig
    server_api_versions_test.zig
  integration/
    broker_api_versions_test.zig
```

## ADR
### Decision
Start the educational broker with a **bootstrap-only wire slice**: listener + frame parse + header parse + `ApiVersions` response.

### Drivers
- This is the earliest milestone in the user’s broker roadmap. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- The repo already has reusable protocol code from the client path. `src/lib.zig:7-15`
- The smallest stable server milestone should prove Kafka wire compatibility before storage and data-plane APIs.

### Alternatives considered
- **Jump directly to Metadata/ListPartitions** — rejected as too wide for the first broker slice.
- **Implement Produce/Fetch immediately** — rejected because storage concerns would overwhelm the bootstrap milestone.
- **Fork a separate server-only protocol stack** — rejected because it would duplicate the client’s shared protocol layer.

### Consequences
- The first broker milestone will be narrow but very testable.
- Future milestones can layer Metadata, topic listing, Fetch, and Produce onto a proven request/dispatch base.
- The client and server tracks now share a growing protocol foundation.

### Follow-ups
- Milestone 2: Metadata / topic-partition listing
- Milestone 3: Fetch from in-memory or simple disk-backed storage
- Milestone 4: Produce into the same storage path

## Recommended Execution Path
- Best next command: `$ralph .omx/plans/prd-zig-kafka-broker-bootstrap-v0.md`
- Companion test spec: `.omx/plans/test-spec-zig-kafka-broker-bootstrap-v0.md`
