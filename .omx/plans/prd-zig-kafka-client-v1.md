# PRD: Zig Kafka Client v1

## Metadata
- Source brief: `.omx/specs/deep-interview-zig-kafka-from-scratch.md`
- Context snapshot: `.omx/context/zig-kafka-from-scratch-20260414T232801Z.md`
- Planning mode: `$ralplan` (`$plan --consensus --direct`)
- Scope: Phase 1 only — Zig client library for real Apache Kafka
- Status: Approved plan artifact

## Requirements Summary
- The first deliverable is a **small but solid Zig client library**, not the educational broker. The broker is a later phase. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:24-25` `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`
- v1 must interoperate with a **real Kafka broker** and support **both produce and consume**. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:28-41`
- v1 must stay narrow and explicitly exclude consumer groups, EOS/idempotence, TLS/SASL, compression, admin APIs, and automatic cluster metadata refresh. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:54-82`
- Done means a Zig example or integration test can connect to a real broker, produce to a pre-created topic, and fetch those messages back from a chosen partition/offset. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:84-91`
- The repository is effectively greenfield for Zig implementation work: no Zig source files or build files exist yet, but local Zig reference docs are already present. `.omx/context/zig-kafka-from-scratch-20260414T232801Z.md:7-11` `.omx/context/zig-kafka-from-scratch-20260414T232801Z.md:23-25`

## RALPLAN-DR Summary

### Principles
1. **Real broker first** — phase 1 proves interoperability against Apache Kafka before optimizing for the future educational server.
2. **Narrow, typed core** — prefer a small protocol surface with strong Zig types over broad feature coverage.
3. **Layer for reuse** — make protocol primitives reusable later, but do not let server-oriented abstractions expand client-v1 scope.
4. **Test through the wire** — unit tests lock binary correctness; integration tests prove real-broker behavior.
5. **No hidden magic** — explicit broker list, explicit topic/partition/offset flows, explicit error propagation.

### Decision Drivers
1. **Immediate usefulness:** a real Kafka client produces practical value now. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:21-25`
2. **Scope control:** the accepted non-goals and narrow subset are what keep the project deliverable. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:54-82`
3. **Extensibility:** the user wants a small but solid base that can later inform the educational server. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:77-82`

### Viable Options
#### Option A — Protocol-first layered client (**Chosen**)
Build a reusable protocol codec layer (headers, primitives, request/response schemas, record batches), then place a thin transport/metadata/client API layer on top.
- Pros:
  - Best fit for “small but solid” and later extension.
  - Lets tests pin binary behavior at the codec boundary.
  - Preserves reusable pieces for the later educational server without prematurely designing the server.
- Cons:
  - More upfront structure than a one-file demo client.
  - Requires disciplined boundaries before any end-to-end success is visible.

#### Option B — Demo-first direct request assembly
Build only the exact happy-path requests/responses inline inside a high-level client until produce/fetch works.
- Pros:
  - Fastest route to a first green integration test.
  - Lowest initial code volume.
- Cons:
  - Harder to extend cleanly.
  - Risk of mixing transport, schema, and API decisions into brittle code.
  - Conflicts with the explicit “small but solid library” requirement.

#### Option C — Shared client/server protocol core from day one
Design the client library and future educational broker around a shared internal protocol/domain model immediately.
- Pros:
  - Maximizes theoretical reuse across both tracks.
  - Forces early protocol rigor.
- Cons:
  - Pulls future-server concerns into phase 1.
  - Delays practical Kafka interoperability.
  - Reintroduces the ambiguity the interview intentionally removed.

### Chosen Direction
Choose **Option A**. Start with a protocol-first layered client and deliberately stop at the narrow API subset needed for bootstrap metadata lookup, produce, and fetch.

## Consensus Review Summary
### Architect pass
- **Steelman antithesis:** Option C could reduce duplicate protocol work later by establishing a shared core now.
- **Real tradeoff tension:** early reuse vs. early interoperability. A shared core is attractive, but it would force speculative abstractions before the real client behavior is proven.
- **Synthesis:** build protocol primitives and schema codecs as reusable modules, but avoid any server-domain layer in phase 1.

### Critic pass
The draft needed three improvements before approval:
1. Make version-negotiation strategy explicit instead of hand-waving “compatibility.”
2. Clarify that metadata bootstrap is allowed, while **automatic** metadata refresh remains out of scope.
3. Define a concrete file layout and verification ladder so execution can start without re-planning.

### Improvements applied
- Added a specific wire/version strategy below.
- Split bootstrap metadata lookup from background/automatic metadata refresh.
- Added explicit file targets, risk mitigations, and a test-spec companion artifact.

## Product / Technical Decision Record (ADR)
### Decision
Implement a **layered Zig Kafka client** with:
- direct TCP transport,
- explicit request/response headers and correlation IDs,
- API version negotiation via `ApiVersions`,
- topic/partition leader discovery via `Metadata`,
- produce support for explicit topic+partition writes,
- fetch support for explicit topic+partition+offset reads,
- no auth/compression/groups/admin features in v1.

### Drivers
- The Kafka protocol is a binary TCP request/response protocol and clients should negotiate versions per API with `ApiVersions`, then use metadata to find partition leaders. This is documented in the official Kafka protocol guide. Source: https://kafka.apache.org/42/design/protocol/
- The user explicitly prioritized real Kafka compatibility, narrow scope, and a small but solid library. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:24-41` `.omx/specs/deep-interview-zig-kafka-from-scratch.md:77-91`
- The future educational server should be informed by the client work, not block it. `.omx/specs/deep-interview-zig-kafka-from-scratch.md:43-52`

### Alternatives considered
- **Demo-first direct assembly** — rejected for poor maintainability.
- **Shared client/server core now** — rejected for phase-1 scope bleed.

### Why chosen
This plan creates the smallest architecture that still supports correctness, testability, and later extension.

### Consequences
- More initial structure than a spike, but fewer rewrites when adding additional APIs later.
- v1 will intentionally require explicit broker/topic/partition/offset inputs rather than hiding complexity with broad automation.
- The later educational server can reuse protocol primitives and test fixtures, but not dictate client-v1 architecture.

### Follow-ups
- Phase 2: broaden client ergonomics only after phase-1 interoperability is stable.
- Phase 3: start the educational single-broker server roadmap from the user-provided capability ladder.

## Proposed Architecture

### Wire/version strategy
Inference from the official Kafka protocol docs: use the compatibility model Kafka already documents, but cap v1 at pre-flexible versions where practical to avoid compact/tagged-field complexity in the first milestone.

Planned strategy:
- Send **`ApiVersionsRequest v0`** on connection bootstrap for maximum compatibility and simple parsing.
- Record broker-supported version ranges per connection.
- Choose the highest mutual version up to client caps:
  - `ApiVersions`: request v0 / parse v0 response for bootstrap
  - `Metadata`: cap at **v8**
  - `Produce`: cap at **v8**
  - `Fetch`: cap at **v11**
- Rationale: versions beyond these caps introduce more flexible/tagged-field and topic-ID complexity in modern Kafka docs; capping here keeps v1 narrow while still interoperating with modern brokers. This is an inference from the official protocol pages, not a direct requirement from the user.

### Behavioral scope
- Bootstrap from a user-provided broker list.
- Open one TCP connection per broker as needed.
- Keep **serial requests per connection** for v1 correctness and simpler correlation handling.
- Fetch metadata during bootstrap and on explicit caller request; do **not** implement background/automatic metadata refresh.
- Require explicit topic and partition for first produce/fetch APIs.
- Require explicit fetch offset in the first consumer API.
- Support uncompressed records only in v1.
- Support only plaintext listeners in v1 (no TLS/SASL).

### Package / file layout
```text
build.zig
build.zig.zon
src/
  lib.zig
  net/
    connection.zig
    transport.zig
  protocol/
    primitives.zig
    header.zig
    error_codes.zig
    api_versions.zig
    metadata.zig
    produce.zig
    fetch.zig
    record_batch.zig
  cluster/
    metadata_cache.zig
  client/
    config.zig
    client.zig
    producer.zig
    consumer.zig
examples/
  roundtrip.zig
tests/
  unit/
    protocol_primitives_test.zig
    header_test.zig
    api_versions_test.zig
    metadata_codec_test.zig
    produce_codec_test.zig
    fetch_codec_test.zig
    record_batch_test.zig
  integration/
    kafka_roundtrip_test.zig
scripts/
  kafka-test-up.sh
  kafka-test-down.sh
  kafka-test-wait.sh
```

## Implementation Steps
1. **Scaffold the Zig project**
   - Add `build.zig`, `build.zig.zon`, `src/lib.zig`.
   - Establish module/test/example targets.

2. **Build protocol primitives + headers**
   - Implement big-endian primitive codecs, length-prefixed strings/bytes, varint/varlong helpers needed by record batches.
   - Implement request/response header encode/decode and correlation ID handling.
   - Files: `src/protocol/primitives.zig`, `src/protocol/header.zig`, `tests/unit/protocol_primitives_test.zig`, `tests/unit/header_test.zig`.

3. **Implement connection bootstrap + ApiVersions negotiation**
   - TCP connection abstraction with framed request/response I/O.
   - `ApiVersionsRequest v0` and response parsing.
   - Version-range selection helper used by all higher APIs.
   - Files: `src/net/connection.zig`, `src/net/transport.zig`, `src/protocol/api_versions.zig`, `tests/unit/api_versions_test.zig`.

4. **Implement metadata lookup**
   - Add `Metadata` request/response codec for topic/partition leader discovery.
   - Build a small metadata cache keyed by topic+partition.
   - Files: `src/protocol/metadata.zig`, `src/cluster/metadata_cache.zig`, `tests/unit/metadata_codec_test.zig`.

5. **Implement produce path**
   - Add record-batch encoder for uncompressed records.
   - Add produce request/response codec and a minimal producer facade requiring explicit topic+partition.
   - Files: `src/protocol/record_batch.zig`, `src/protocol/produce.zig`, `src/client/producer.zig`, `tests/unit/record_batch_test.zig`, `tests/unit/produce_codec_test.zig`.

6. **Implement fetch path**
   - Add fetch request/response codec and a minimal consumer facade requiring explicit topic+partition+offset.
   - Decode fetched records into Zig value types.
   - Files: `src/protocol/fetch.zig`, `src/client/consumer.zig`, `tests/unit/fetch_codec_test.zig`.

7. **Integrate into public client API + example**
   - Create `Client` config/bootstrap abstraction.
   - Add `examples/roundtrip.zig` showing connect -> metadata -> produce -> fetch.
   - Files: `src/client/config.zig`, `src/client/client.zig`, `examples/roundtrip.zig`.

8. **Add real Kafka integration harness**
   - Add scripts to bring up/down a real Kafka broker locally.
   - Add integration test that uses a pre-created topic and proves round-trip behavior.
   - Files: `scripts/kafka-test-up.sh`, `scripts/kafka-test-down.sh`, `scripts/kafka-test-wait.sh`, `tests/integration/kafka_roundtrip_test.zig`.

## Acceptance Criteria
1. `zig build test` passes unit tests for protocol primitives, headers, version negotiation, metadata, produce, fetch, and record batch encoding.
2. `zig build test-integration` (or equivalent) passes against a real Kafka broker started by repo scripts.
3. The integration harness proves:
   - bootstrap connection succeeds,
   - `ApiVersions` negotiation succeeds,
   - metadata lookup finds the leader for the target partition,
   - produce writes at least one message to a pre-created topic/partition,
   - fetch reads that message back from a specified offset.
4. The public API exposes explicit but clean entry points for:
   - client bootstrap,
   - produce(topic, partition, records),
   - fetch(topic, partition, offset, max_bytes).
5. No phase-1 non-goal feature is partially implemented behind hidden flags.

## Risks and Mitigations
- **Risk:** protocol version mismatches across broker releases.  
  **Mitigation:** negotiate versions via `ApiVersions`, cap supported versions explicitly, and fail with clear unsupported-version errors.
- **Risk:** record batch encoding/decoding is the hardest binary portion of the narrow feature set.  
  **Mitigation:** isolate `record_batch.zig`, lock it with focused unit tests before integration.
- **Risk:** metadata handling drifts into cluster-management scope.  
  **Mitigation:** support bootstrap discovery plus explicit refresh only; no background refresh logic in v1.
- **Risk:** integration tests become flaky or too heavyweight.  
  **Mitigation:** provide scripts with readiness polling and deterministic topic setup; keep the test scenario single-broker and single-topic.
- **Risk:** future server work pressures the client into premature shared abstractions.  
  **Mitigation:** reuse only protocol primitives/test fixtures in phase 1; defer server-specific planning until the client is green.

## Verification Steps
- Run `zig fmt` on all Zig files.
- Run `zig build test`.
- Run local Kafka harness, then `zig build test-integration`.
- Run the example round-trip program against the same broker/topic.
- Review public API surface in `src/lib.zig` / `src/client/*.zig` for unnecessary leakage of transport/protocol internals.

## Available-Agent-Types Roster
Useful follow-up agent roles available in this repo/runtime:
- `architect` — design review, boundaries, tradeoffs
- `executor` — primary implementation lane
- `test-engineer` — test strategy, harness, flaky-test hardening
- `debugger` — protocol/runtime failure diagnosis
- `verifier` — completion evidence and test adequacy
- `build-fixer` — build/type/toolchain failures
- `critic` — plan/design challenge
- `writer` — docs/examples/migration notes
- `explore` / `explorer` — fast codebase lookup

## Follow-up Staffing Guidance
### Recommended `$ralph` path
Use `$ralph` if you want one owner to implement sequentially with persistent verification.
- Primary lane: `executor` (high reasoning)
- Embedded verification: `test-engineer` (medium), `verifier` (high)
- Escalation lanes when needed: `debugger` for protocol bugs, `build-fixer` for Zig/tooling issues
- Why this path: the repo is greenfield and the phases are naturally sequential (codec foundation -> metadata -> produce -> fetch -> integration)

### Recommended `$team` path
Use `$team` if you want parallel implementation once scaffolding lands.
Suggested staffing after the initial scaffold is merged:
- Lane 1 — protocol primitives + headers (`executor`, high)
- Lane 2 — API codecs: ApiVersions + Metadata (`executor`, high)
- Lane 3 — Produce/Fetch + record batches (`executor`, high)
- Lane 4 — integration harness + tests (`test-engineer`, medium)
- Lane 5 — final verification/docs (`verifier` or `writer`, medium/high)

## Launch Hints
### Ralph
```bash
$ralph .omx/plans/prd-zig-kafka-client-v1.md
```

### Team
```bash
$team .omx/plans/prd-zig-kafka-client-v1.md
```

Recommended companion context for either path:
- `.omx/plans/test-spec-zig-kafka-client-v1.md`
- `.omx/specs/deep-interview-zig-kafka-from-scratch.md`

## Team Verification Path
Before team shutdown, the implementation team should prove:
1. All planned files exist and build targets are wired.
2. Unit tests for codec boundaries pass.
3. Real Kafka integration harness is reproducible from repo scripts.
4. End-to-end roundtrip against a pre-created topic passes.
5. Public API remains within phase-1 scope.

After a team handoff, Ralph/verifier should re-check:
- green tests from a clean workspace,
- example roundtrip output,
- no accidental implementation of non-goals,
- clear next-step notes for the future educational server track.

## Planning Changelog
- Added explicit version-cap strategy after consensus review.
- Clarified metadata bootstrap vs. out-of-scope automatic refresh.
- Expanded implementation steps into concrete file targets.
- Added execution handoff guidance for `$ralph` and `$team`.
