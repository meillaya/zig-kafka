# Context Snapshot: zig-kafka-broker-malformed-requests

- Task statement: Plan the next broker milestone after error semantics: malformed request handling.
- Desired outcome: A concrete PRD and test spec for deterministic malformed-request behavior.
- Known facts/evidence:
  - The broker now has deterministic unsupported-version responses for supported APIs and retains unknown-API close behavior.
  - Request/frame parsing currently treats truncated frames and some connection-reset cases as end-of-stream at the frame boundary.
  - There is not yet an explicit malformed-request milestone or test matrix for bad framing / incomplete payloads / malformed headers.
- Constraints:
  - Keep the milestone bounded to malformed-request handling only.
  - Preserve all existing broker/client milestones.
  - Do not widen protocol feature surface.
- Unknowns/open questions:
  - Which malformed cases should close immediately vs attempt a protocol-valid error.
  - How much malformed parsing can safely happen before a correlation-id-bearing response is possible.
- Likely codebase touchpoints:
  - src/server/request.zig
  - src/server/broker.zig
  - tests/integration/**
  - tests/unit/**
