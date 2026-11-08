# Serapendiya

**Make sense of every event.**

Serapendiya is an early-stage real-time data platform exploring a clearer way
to build, inspect, and understand event-driven systems-from the event stream
to future analytics workflows.

**Stage:** early development · **License:** MIT · **Built in:** Rust

[Product vision](#product-vision) · [Current status](#current-status) ·
[Roadmap](#roadmap)

## Product vision

![Serapendiya platform vision](<Serapendiya Real-Time Data Platform.png>)

*Concept illustration: capabilities shown include planned features and do not
represent the current product.*

## The opportunity

Teams building event-driven systems often assemble several disconnected tools
for ingestion, inspection, replay, processing, schema governance, and
analytics. When something goes wrong, the difficult question is not merely
“what is the lag?” but “where did this event go, what state saw it, and can I
replay it safely?”

Serapendiya’s product hypothesis is to make that developer workflow the center
of the platform: durable event history, clear state, safe inspection, and a
path from real-time events to useful analytical data.

## Product thesis

- Open-source streaming core for local development and self-hosting.
- Developer tools for inspecting event history and explaining state.
- Safe, explicit replay workflows rather than opaque operational actions.
- Eventual adapters for governed contracts and analytical destinations, only
  after the core workflow is proven.

## Current status

The open-source repository contains a G0 foundation and experimental G1 storage work:

- Rust workspace with `streamd` and `streamctl` entry points.
- Strict configuration and foundational domain types.
- Local append log with RecordBatch v2-shaped framing and CRC32C.
- Checkpoint and certified-prefix recovery prototypes.
- Fault-injection and process-level crash-recovery tests.
- Derived sparse indexes, bounded group commit, and Linux Docker CI.

G1 acceptance and durability guarantees remain under review. The project is not
yet a broker or production MVP. Kafka networking, real producers and consumers,
queues, replay UX, and the end-to-end developer workflow remain future gates.

## Roadmap

1. Finish and review G1 storage evidence.
2. Obtain explicit authorization for G2 Kafka compatibility work.
3. Implement a tested, deliberately narrow Kafka subset.
4. Add independent native broadcast and queue state.
5. Build the local developer workflow: publish, inspect, watch, diagnose, and
   safe replay preview.
6. Validate the workflow with real client and recovery tests before making
   production claims.

One planned development milestone is a “publish → inspect → restart → recover”
workflow. It is a future goal, not an existing feature claim.

## Development checks

```sh
cargo build --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/run-g1-tests.sh
```

The Docker command requires Docker Desktop or a compatible Docker runtime.

See [docs/roadmap.md](docs/roadmap.md) for delivery status.
