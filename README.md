# zig-kafka

`zig-kafka` is a from-scratch Kafka project in Zig with **two deliberate tracks**:

1. a **small real-broker client library** that can talk to Apache Kafka over the wire, and
2. an **educational single-broker Kafka-like server** used to learn the protocol, request handling, storage shape, and broker-side semantics step by step.

The repository is intentionally narrow. It focuses on the binary Kafka protocol, explicit topic/partition operations, and clean reusable protocol code rather than broad feature coverage.

## Project status

The repository has already moved beyond a blank client scaffold. It now contains a working protocol layer, a real Kafka client slice, an educational broker slice, examples, Docker-backed test harness scripts, and a fairly deep unit/integration test suite.

The client track and broker track **share protocol modules**, but they are **not yet feature-parity peers**. The client currently interoperates with a real Kafka broker via `ApiVersions`, `Metadata`, `Produce`, and `Fetch`. The educational broker currently implements `ApiVersions`, `DescribeTopicPartitions`, `Fetch`, and `Produce`, but **does not yet implement the `Metadata` API**, so the real-client roundtrip example targets Apache Kafka rather than the educational broker.

## What has been achieved so far

### Real Kafka client slice

The client side already supports the minimum useful bootstrap path for a real Kafka cluster: open a TCP connection, negotiate supported APIs, fetch cluster metadata, discover a partition leader, produce records to a chosen topic/partition, and fetch records back from an explicit offset.

The public surface is intentionally small:

```zig
const kafka = @import("zig-kafka");

var client = kafka.Client.init(.{
    .allocator = allocator,
    .bootstrap_brokers = &.{"127.0.0.1:9092"},
    .client_id = "zig-kafka-example",
});
defer client.deinit();

var producer = kafka.Producer.init(&client);
var consumer = kafka.Consumer.init(&client);
```

This slice already includes:

- Kafka binary framing over TCP
- request/response headers with correlation IDs
- `ApiVersions` negotiation
- `Metadata` lookup and leader resolution
- uncompressed record batch encode/decode
- `Produce` request/response handling
- `Fetch` request/response handling
- a metadata cache
- a real-broker roundtrip example

### Educational broker slice

The educational broker has also advanced significantly. It is no longer just a port-binding spike. It can now accept Kafka-framed requests, dispatch on API key, answer several APIs, serve multiple clients concurrently, process multiple requests on the same connection, shut down gracefully, emit deterministic unsupported-version responses for supported APIs, and close cleanly on malformed input.

The broker currently supports these API families:

| API | Role in repo | Current broker support |
| --- | --- | --- |
| `ApiVersions` | protocol bootstrap | implemented |
| `DescribeTopicPartitions` | static topic listing | implemented |
| `Fetch` | read path | implemented |
| `Produce` | write path | implemented |
| `Metadata` | real-broker client discovery | **not yet implemented on educational broker** |

### Test and harness work

The repository already includes broad protocol, broker, and interoperability coverage.

- unit tests cover primitives, headers, request parsing, codecs, storage, and handlers
- integration tests cover the real Kafka roundtrip and educational broker behavior
- repo scripts can start and stop a disposable Kafka container for real-broker validation
- build targets exist for unit tests, integration tests, the educational broker, and the roundtrip example

The build exposes these top-level steps:

```text
zig build test
zig build test-integration
zig build run-broker
zig build run-roundtrip
```

## Theory

### Kafka as a binary TCP protocol

Kafka is fundamentally a **size-prefixed binary request/response protocol over TCP**. Every request starts with a 4-byte frame length, followed by a request header and an API-specific body. Every response mirrors that shape and carries the caller's correlation ID back to the client.

That basic theory is the backbone of this repository. Both the client and the educational broker treat Kafka first as a framing and schema problem: parse bytes safely, preserve correlation IDs, encode valid responses, and only then layer higher-level behavior on top.

### Topics, partitions, leaders, and offsets

Kafka stores records in **partitioned append-only logs**. A producer writes to a topic partition. A consumer fetches from a topic partition starting at some offset. Offsets are logical positions in that log, and they give Kafka its rewind/replay model.

In a real cluster, the client must discover which broker leads a given partition before it can read or write that partition. That is why the client path in this repo first performs API negotiation and metadata discovery before attempting `Produce` or `Fetch`.

### Record batches matter

Kafka does not send each message as an isolated request-sized unit. Records are grouped into **record batches** with their own structure, timestamps, offset deltas, and CRC validation. Even in a small implementation, getting record-batch encode/decode correct is essential because both produce and fetch semantics depend on it.

This repo therefore treats record-batch handling as a first-class module rather than burying it in the client or broker code.

### Why the project is split into client and broker tracks

A real Kafka client and an educational broker are related, but not identical, problems. The client track proves interoperability with Apache Kafka as it exists today. The broker track isolates the concepts needed to learn and reimplement the protocol a piece at a time.

That split keeps the project honest. The client remains practically useful. The broker remains pedagogical and small enough to reason about. Shared protocol code allows lessons from one side to transfer to the other.

## Architecture

### High-level layout

```text
src/
  lib.zig
  errors.zig
  net/
    connection.zig
    transport.zig
  protocol/
    primitives.zig
    header.zig
    api_versions.zig
    metadata.zig
    describe_topic_partitions.zig
    produce.zig
    fetch.zig
    record_batch.zig
    error_codes.zig
  cluster/
    metadata_cache.zig
  client/
    config.zig
    client.zig
    producer.zig
    consumer.zig
  server/
    config.zig
    broker.zig
    catalog.zig
    storage.zig
    request.zig
    handlers/
      api_versions.zig
      describe_topic_partitions.zig
      fetch.zig
      produce.zig
examples/
  broker.zig
  roundtrip.zig
tests/
  unit/
  integration/
```

### Layering model

The repository follows a fairly clean layered shape:

```text
client API / broker runtime
        |
 protocol-specific request/response modules
        |
  shared headers + primitives + record batches
        |
             TCP
```

That shared middle layer is the most important architectural decision so far. It keeps Kafka wire knowledge in one place and prevents the broker and client from drifting into separate ad hoc encodings.

### Client architecture

The client path is centered around `src/client/client.zig`.

```text
Config
  -> Transport
  -> ApiVersions negotiation
  -> Metadata lookup
  -> MetadataCache leader resolution
  -> Produce / Fetch against resolved broker
```

Key responsibilities are split like this:

- `net/connection.zig` parses host/port text and opens TCP streams
- `net/transport.zig` owns correlation IDs, request framing, response framing, and correlation checks
- `protocol/*.zig` owns schema-specific encoding/decoding
- `cluster/metadata_cache.zig` stores the latest broker/topic/partition view
- `client/client.zig` orchestrates negotiation, metadata refresh, leader selection, and retries
- `client/producer.zig` and `client/consumer.zig` are intentionally thin wrappers

The current client is explicit by design. It does not try to hide the partition/offset model yet.

### Broker architecture

The educational broker is centered around `src/server/broker.zig`.

```text
TCP listener
  -> read Kafka frame
  -> parse request header
  -> dispatch on api_key
  -> handler builds response body
  -> broker wraps response header + frame
```

Important broker modules:

- `server/request.zig` reads frames and parses request headers
- `server/catalog.zig` defines a static topic catalog used by broker-side listing and validation
- `server/storage.zig` holds seeded and appended records in mutable in-memory state
- `server/handlers/*.zig` implement API-specific semantics
- `server/broker.zig` owns accept loop, per-connection processing, worker tracking, and shutdown

The broker currently processes requests **serially within a connection** but can serve **multiple clients concurrently** via per-connection worker threads.

## Implementation details so far

### Protocol primitives and headers

`src/protocol/primitives.zig` implements the byte-level foundation: big-endian integer encoding/decoding, strings, nullable strings, nullable bytes, compact strings, compact arrays, UUID helpers, tagged-field skipping, and varint/varlong helpers.

That module is the lowest-level guardrail in the repo. Many higher-level correctness properties depend on it rejecting negative lengths, truncated payloads, and malformed compact encodings cleanly.

`src/protocol/header.zig` then defines Kafka request/response headers, including support for both traditional and flexible-header variants. This is already important on the broker side because `DescribeTopicPartitions` uses the newer compact/tagged-field header shape.

### ApiVersions and version gating

`src/protocol/api_versions.zig` and `src/server/handlers/api_versions.zig` form the version-negotiation base. The client asks a broker what versions it supports. The educational broker advertises exactly what it can handle.

Today the educational broker advertises support for:

```text
ApiVersions               v2
DescribeTopicPartitions   v0
Fetch                     v4
Produce                   v7
```

On the client side, negotiated versions are stored once and then used to cap `Metadata`, `Produce`, and `Fetch` requests to the mutually supported range.

### Metadata and leader resolution

The real-client path uses `src/protocol/metadata.zig` plus `src/cluster/metadata_cache.zig` to discover brokers and resolve partition leaders. `Client.refreshMetadata()` loads topic metadata from a bootstrap broker, and `Client.leaderBroker()` uses the cache to find the host/port for a topic partition.

The current client also does a **limited explicit refresh-and-retry** when a produce or fetch call receives a leader-related error. This is not a background metadata subsystem; it is a narrow, on-demand recovery path.

### Record batch implementation

`src/protocol/record_batch.zig` is one of the most important modules in the repo.

It already implements:

- uncompressed record batch encoding
- base offset and timestamp handling
- offset and timestamp deltas
- varint and varlong field encoding
- CRC calculation and validation
- decoding back into owned Zig record structures

That module is what lets the same record representation flow through both the real client and the educational broker.

### Produce and fetch codecs

`src/protocol/produce.zig` and `src/protocol/fetch.zig` implement the current narrow request/response subsets needed by this project.

The repo does **not** try to cover every field or every modern Kafka feature. Instead it implements the subset required for:

- explicit topic+partition produces
- explicit topic+partition+offset fetches
- deterministic broker integration tests
- real-cluster roundtrip validation

This narrowness is intentional. It keeps the code small enough to understand while still proving real wire interoperability.

### Educational broker catalog and storage

`src/server/catalog.zig` defines a tiny static broker-side topic catalog:

```text
broker-topic-a (3 partitions)
broker-topic-b (1 partition)
```

`src/server/storage.zig` seeds that catalog with in-memory records and allows append-on-produce behavior for valid topic/partition combinations. Storage is mutex-protected, which is enough for the current concurrent-client milestone without introducing a heavy runtime framework.

This gives the broker both a deterministic read path and a mutable write path while staying intentionally educational.

### Broker behavior milestones already implemented

The educational broker already covers several runtime milestones that are easy to miss if you only skim the source tree:

1. **Bootstrap wire support** — listen on TCP, parse Kafka frames, preserve correlation IDs.
2. **Topic listing** — respond to `DescribeTopicPartitions` from a static catalog.
3. **Fetch** — return deterministic static or appended records.
4. **Produce** — append records into mutable in-memory storage.
5. **Concurrent clients** — multiple sockets can be served at once.
6. **Pipelined/sequential requests per connection** — one connection can issue multiple requests back to back.
7. **Graceful shutdown** — worker threads are tracked and joined.
8. **Error semantics** — supported APIs return protocol-valid unsupported-version responses.
9. **Malformed input handling** — truncated or malformed requests are closed cleanly.

### Current limitations

The current codebase is useful, but still intentionally incomplete.

- the educational broker is not a real Kafka broker and does not yet implement cluster metadata
- storage is in-memory only; there is no segment file, retention, recovery, or replication story yet
- the client surface is explicit and narrow; there are no consumer groups, commits, auth, compression, or admin APIs
- protocol coverage is selective rather than exhaustive
- the broker's semantics are deterministic and educational rather than production-complete

## Building

### Prerequisites

- **Zig 0.15.2**
- **Docker** if you want to run the real Kafka harness scripts

### Build the project

```bash
zig build
```

### List available build targets

```bash
zig build -l
```

## Running

### Run the educational broker

This starts the local educational broker, not Apache Kafka.

```bash
zig build run-broker -- --host 127.0.0.1 --port 9094
```

You can also add an artificial per-request delay, which is useful for concurrency and shutdown testing.

```bash
zig build run-broker -- --host 127.0.0.1 --port 9094 --request-delay-ms 200
```

### Run the real Kafka roundtrip example

First bring up a real Kafka broker for the example. The repo includes Docker-based helper scripts for that.

```bash
./scripts/kafka-test-up.sh
```

Then run the example against that broker:

```bash
export KAFKA_BROKER=127.0.0.1:9092
export KAFKA_TOPIC=zig-kafka-v1-test
export KAFKA_PARTITION=0

zig build run-roundtrip
```

When finished:

```bash
./scripts/kafka-test-down.sh
```

## Testing

### Unit tests

```bash
zig build test
```

### Integration tests

The integration suite always compiles, but the networked scenarios are gated by an environment variable. Set `ZIG_KAFKA_RUN_INTEGRATION=1` when you want the full broker and real-Kafka flows to execute.

```bash
export ZIG_KAFKA_RUN_INTEGRATION=1
zig build test-integration
```

### Real Kafka integration with managed Docker broker

The roundtrip integration test can optionally manage the Kafka container itself.

```bash
export ZIG_KAFKA_RUN_INTEGRATION=1
export ZIG_KAFKA_MANAGE_BROKER=1
zig build test-integration
```

Useful environment variables:

```bash
export KAFKA_TEST_CONTAINER_NAME=zig-kafka-test-broker
export KAFKA_TEST_BOOTSTRAP_SERVER=127.0.0.1:9092
export KAFKA_TEST_TOPIC=zig-kafka-v1-test
export KAFKA_TEST_PARTITION=0
export ZIG_KAFKA_BROKER_TEST_PORT=19094
```

### What the integration suite covers

The integration suite currently exercises both tracks:

- real Kafka client roundtrip against Apache Kafka
- educational broker `ApiVersions`
- educational broker `DescribeTopicPartitions`
- educational broker `Fetch`
- educational broker `Produce`
- concurrent clients
- multiple requests on one connection
- graceful shutdown
- unsupported-version responses
- malformed-request close behavior

## Usage notes

### Client roundtrip example

The current real-client happy path is roughly:

```text
bootstrap broker list
  -> ApiVersions
  -> Metadata
  -> leader resolution
  -> Produce(topic, partition)
  -> Fetch(topic, partition, offset)
```

### Educational broker request path

The broker-side request path is roughly:

```text
accept connection
  -> read frame length
  -> parse request header
  -> choose handler by api_key
  -> encode response body
  -> encode response header
  -> write framed response
```

Because responses are correlation-ID preserving and FIFO within a connection, the broker is already useful as a protocol-learning target even though it is intentionally small.

## Next major milestones

### 1. Close the protocol gap between client and educational broker

The biggest architectural gap right now is that the real client depends on `Metadata`, while the educational broker currently exposes `DescribeTopicPartitions` instead. Implementing `Metadata` on the broker side would let more of the shared client path target the educational broker directly and would reduce the split between “real broker mode” and “learning broker mode”.

### 2. Move the educational broker from seeded memory to log-shaped storage

The next major broker leap is durable storage: append-only segment files, startup recovery, offset indexes, and simple retention. That would align the implementation much more closely with Kafka's underlying log model and make fetch semantics more realistic.

### 3. Expand client ergonomics without losing the narrow core

Once the current protocol base is stable, the client can grow in carefully chosen ways: explicit batching helpers, cleaner consumer iteration APIs, better error surfaces, and more metadata refresh control. The key is to keep those additions layered on top of the current small protocol core rather than rewriting it.

### 4. Add modern protocol coverage deliberately

Flexible versions, more compact encodings, topic IDs, and additional request families should be added one bounded slice at a time. The current repo already has some compact/tagged-field support, but full modern Kafka protocol coverage requires deeper schema-driven work.

### 5. Decide how far the educational broker should go

There is a natural fork ahead: keep the broker intentionally small and pedagogical, or continue toward a much more realistic standalone Kafka-like implementation. That choice affects whether the next priorities should be durability, metadata, replication concepts, or consumer coordination.

## References

The following are the core resources that should be treated as required reading when implementing this project from scratch.

- **Apache Kafka Protocol Guide** — the canonical wire-format reference for framing, primitive types, headers, API keys, versioning, and record-batch structure.  
  https://kafka.apache.org/40/design/protocol/
- **Apache Kafka Message Definitions** — the schema source for request/response message shapes used by Kafka's generated protocol code. This is the best ground truth when hand-writing codecs.  
  https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message
- **KIP-482: The Kafka Protocol should Support Optional Tagged Fields** — required for understanding flexible versions, compact encodings, tagged fields, and newer request/response headers.  
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
- **Kafka papers and presentations (official Apache index)** — the best official hub for the foundational design material and historical papers.  
  https://cwiki.apache.org/confluence/display/KAFKA/Kafka+papers+and+presentations
- **Kafka: A Distributed Messaging System for Log Processing** — the foundational Kafka paper; essential for understanding the original log-centric model, pull-based consumption, offsets, batching, and broker simplicity.  
  https://www.microsoft.com/en-us/research/wp-content/uploads/2017/09/Kafka.pdf
- **The Log: What Every Software Engineer Should Know About Real-time Data's Unifying Abstraction** — useful for the theory behind append-only logs, replay, and why Kafka's data model feels different from traditional queues.  
  https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- **Zig language reference (local copy vendored in this repo)** — required for implementing robust binary parsing, ownership, error unions, and allocator-aware APIs in Zig.  
  `docs/zig-0.15.2-language-reference.txt`
- **Zig standard library reference (local copy vendored in this repo)** — required for networking, threading, allocators, hashing/CRC, process handling, and testing.  
  `docs/zig-0.15.2-stdlib-reference.txt`

