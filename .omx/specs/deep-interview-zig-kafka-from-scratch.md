# Deep Interview Spec: zig-kafka-from-scratch

## Metadata
- Profile: standard
- Rounds: 9
- Final ambiguity: 16%
- Threshold: 20%
- Context type: greenfield
- Context snapshot: `.omx/context/zig-kafka-from-scratch-20260414T232801Z.md`
- Transcript: `.omx/interviews/zig-kafka-from-scratch-20260414T232801Z.md`

## Clarity Breakdown
| Dimension | Score |
| --- | ---: |
| Intent | 0.70 |
| Outcome | 0.85 |
| Scope | 0.95 |
| Constraints | 0.85 |
| Success | 0.85 |

## Intent
Build a meaningful Zig Kafka project that has immediate practical value against real Apache Kafka while also serving as a learning vehicle for Kafka internals and an eventual end-to-end stack the project fully controls.

## Desired Outcome
Deliver phase 1 as a **small but solid Zig client library** that can talk to a real Kafka broker and demonstrate both producing and consuming successfully. After that, use the project as the foundation for a later **educational single-broker Kafka-like server**.

## In Scope
### Phase 1: client library
- New Zig project scaffold in this repository
- A narrow Kafka protocol subset sufficient for a first solid milestone
- Real-broker interoperability as the primary target
- Both produce and consume capability in the milestone
- Integration tests against real Kafka
- Clean API boundaries and room to extend later

### Suggested phase-1 capability envelope
- Enough protocol support to connect and operate against a real broker
- Metadata discovery and the minimum request/response handling needed to:
  - produce to a pre-created topic
  - consume/fetch from a chosen partition/offset
- Examples/tests proving end-to-end operation

### Phase 2+ direction: educational single-broker server roadmap
Treat the following as a guide for the later Kafka-like server roadmap, not as phase-1 client scope:
- Bind to a port
- Send/parse correlation ID
- Parse API version
- Handle ApiVersions requests
- Concurrent clients / serial requests, then concurrent requests
- Listing partitions / DescribeTopicPartitions
- Fetch progression: no topics, unknown topic, empty topic, single/multiple messages from disk
- Produce progression: API inclusion, invalid/valid topic/partition responses, single/multiple records, multiple partitions/topics

## Out of Scope / Non-goals (Phase 1)
- Consumer groups
- Exactly-once semantics
- Idempotent producer semantics
- TLS / SASL authentication
- Compression
- Admin APIs
- Automatic broker discovery / cluster-wide metadata refresh
- Broad Kafka API coverage beyond the narrow v1 subset
- Building the educational broker during phase 1

## Decision Boundaries
OMX may decide without further user confirmation:
- Kafka protocol/API version target for v1
- Project/package/module layout
- Wire-format and codec strategy
- Internal architecture and abstractions
- Test harness shape
- Local integration-test setup and automation details

Preference explicitly given by user:
- Integration tests should target real Kafka.

## Constraints
- Start with the client library first.
- Optimize for a **small but solid library**, not a quick disposable prototype.
- Prioritize real Kafka compatibility over early coupling to the future educational server.
- Keep v1 narrow and intentionally staged.
- Use no unnecessary dependencies unless later explicitly approved.

## Testable Acceptance Criteria
Phase 1 is done when all of the following are true:
1. The repository contains a working Zig client library scaffold with build/test entry points.
2. A Zig example or integration test can connect to a real Kafka broker.
3. The client can produce messages to a **pre-created topic**.
4. The client can consume/fetch those same messages from a **chosen partition/offset**.
5. The resulting API and code structure are clean enough to extend later rather than being a one-off demo.
6. The implementation stays within the declared phase-1 non-goals.

## Assumptions Exposed + Resolutions
- **Assumption:** “Kafka implementation in Zig” meant a broker.  
  **Resolution:** The project is two-track, but phase 1 is explicitly the client library.
- **Assumption:** The first milestone might optimize for learning over usefulness.  
  **Resolution:** Real Kafka interoperability is the primary practical target for phase 1.
- **Assumption:** v1 might expand into many Kafka features.  
  **Resolution:** A strict non-goal list was accepted to keep the first milestone narrow.
- **Assumption:** The pasted capability ladder might expand client-v1 scope.  
  **Resolution:** It is explicitly a roadmap for the later educational single-broker server.

## Pressure-Pass Findings
- Broad project intent was reduced to a specific first-phase deliverable.
- Scope creep was actively checked by proposing a non-goal list and confirming it.
- The relationship between the client library and the later server roadmap was explicitly resolved.

## Brownfield Evidence vs Inference Notes
- Evidence: repository currently has `docs/` and `scripts/`, with no Zig source/build files yet.
- Evidence: Zig language and stdlib references for `master` exist locally under `docs/`.
- Inference: this is effectively a greenfield implementation effort inside a minimal scaffold.
- Note: `omx explore` failed locally with a permission error, so shell inspection was used for repository context.

## Technical Context Findings
- Current repo state is suitable for greenfield scaffolding.
- Local Zig reference docs are already available for implementation support.
- Integration testing should be planned against a real Kafka broker.

## Condensed Transcript
See `.omx/interviews/zig-kafka-from-scratch-20260414T232801Z.md`.
