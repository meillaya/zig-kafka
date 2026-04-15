# Context Snapshot: zig-kafka-broker-shutdown

- Task statement: Plan the next broker milestone after concurrent clients and multi-request connections: graceful shutdown and worker tracking.
- Desired outcome: A concrete PRD and test spec for the next educational broker slice.
- Known facts/evidence:
  - The broker now supports concurrent clients and multiple ordered requests per connection.
  - The broker currently detaches worker threads and `requestStop()` only wakes the accept loop; it does not explicitly track worker completion.
  - The roadmap after the current milestones most naturally points to richer shutdown/error-handling semantics.
- Constraints:
  - Keep the milestone educational and bounded.
  - Preserve all prior broker and client milestones.
  - Avoid durability or protocol-surface expansion in this slice.
- Unknowns/open questions:
  - Whether shutdown should wait only for in-flight workers or also reject new connections explicitly.
  - Whether per-request cancellation is desirable (likely no for this educational step).
- Likely codebase touchpoints:
  - src/server/broker.zig
  - src/server/config.zig
  - tests/integration/**
