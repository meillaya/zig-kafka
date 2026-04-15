# Test Spec: Zig Kafka Client v1

## Scope
Validation plan for phase-1 Zig Kafka client work defined in `.omx/plans/prd-zig-kafka-client-v1.md`.

## Test Objectives
1. Prove binary correctness for the protocol pieces the client owns.
2. Prove end-to-end interoperability with a real Kafka broker.
3. Prove the public API satisfies the accepted milestone without scope creep.

## Acceptance Mapping
| Acceptance criterion | Test proof |
| --- | --- |
| Zig scaffold + build/test entry points exist | `zig build`, `zig build test`, `zig build test-integration` targets wired in `build.zig` |
| Connect to real Kafka broker | integration bootstrap test |
| Produce to pre-created topic | integration produce test |
| Fetch from chosen partition/offset | integration fetch test |
| Clean/extensible API surface | API-focused unit checks + code review against planned module boundaries |
| Non-goals stay out of scope | negative scope review checklist |

## Test Layers

### 1) Unit tests
#### Protocol primitives
- big-endian INT16/INT32/INT64 encode/decode
- nullable string / bytes helpers used by selected API versions
- varint / varlong helpers required by record batches
- short-read / invalid-length failure cases

#### Headers + framing
- request header encoding includes API key, API version, correlation ID, client ID as intended
- response header parsing preserves correlation ID
- length-prefixed frame I/O rejects truncated frames

#### ApiVersions negotiation
- parse `ApiVersionsResponse v0`
- choose highest mutual version subject to client caps
- reject absent required APIs with clear error

#### Metadata codec
- encode metadata request for target topic(s)
- parse brokers / topics / partitions / leader mapping
- reject unknown-topic or leader-missing states into explicit Zig errors

#### Record batch codec
- encode uncompressed single-record batch
- encode uncompressed multi-record batch
- decode fetched record batch back to expected payloads
- verify CRC / structural validation logic if implemented

#### Produce codec
- encode produce request with explicit topic+partition
- parse produce response success path
- parse invalid topic/partition error responses

#### Fetch codec
- encode fetch request with explicit topic+partition+offset
- parse empty fetch response
- parse single-message fetch response
- parse multi-message fetch response

### 2) Integration tests
Use a real Kafka broker started from repo scripts.

#### Harness assumptions
- Single broker
- Plaintext listener
- Topic is created by harness setup, not by the Zig client
- Fixed test topic and partition count for determinism

#### Integration scenarios
1. **Bootstrap + ApiVersions**
   - start broker
   - open connection
   - send `ApiVersionsRequest v0`
   - assert supported ranges include Metadata, Produce, Fetch

2. **Metadata lookup**
   - request metadata for pre-created topic
   - assert partition leader is resolved

3. **Produce single message**
   - write one record to explicit topic+partition
   - assert broker returns success / base offset

4. **Fetch from explicit offset**
   - fetch from the known topic+partition starting at expected offset
   - assert returned payload matches produced record

5. **Produce multiple messages / fetch multiple messages**
   - send a small batch
   - fetch from starting offset
   - assert ordered payload roundtrip

6. **Known-error cases**
   - produce to invalid partition -> explicit broker error surfaced
   - metadata/fetch for unknown topic -> explicit Zig error surfaced

### 3) Example / smoke test
- `examples/roundtrip.zig` should run against the harness broker and print/log a successful produce+fetch roundtrip.

## Verification Commands
Planned command surface (exact build target names can be chosen during implementation):
```bash
zig build
zig build test
./scripts/kafka-test-up.sh
zig build test-integration
zig build run-roundtrip
./scripts/kafka-test-down.sh
```

## Test Data / Environment Plan
- Use deterministic topic names like `zig-kafka-v1-test`.
- Keep messages small UTF-8 payloads plus optional binary payload case.
- Prefer ephemeral local broker state under temp directories or disposable containers.
- If Docker is used, pin a Kafka image tag during implementation for repeatability.

## Failure Triage Rules
- Unit failure in primitives/header/record-batch blocks all higher-level work.
- Integration failure after green unit tests likely indicates negotiation, broker targeting, or record-batch mismatch.
- Unknown-topic / invalid-partition failures should be treated as expected negative tests only when explicitly configured that way.

## Non-goal Guardrails
The following should not appear in v1 tests except as explicit “unsupported/not implemented” assertions:
- consumer group coordination
- offset commit APIs
- TLS/SASL handshakes
- compression codecs
- idempotent or transactional producer flows
- admin/create-topic APIs
- automatic background metadata refresh behavior

## Exit Criteria
This test spec is satisfied when:
1. Unit coverage exists for every implemented protocol module.
2. Integration tests pass reliably against a real Kafka broker from repo scripts.
3. The example smoke test passes.
4. Negative scope review confirms no accidental non-goal implementation.
