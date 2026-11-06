# Serapendiya

## A developer-first real-time data platform for understanding events from production to analytics

Serapendiya is an ideation-stage open-source project exploring a simpler way
to build, inspect, replay, and eventually materialise real-time event data.

> Event infrastructure should help developers understand what happened, why it
> happened, and what they can safely do next—not just move bytes quickly.

![Serapendiya platform vision](<Serapendiya Real-Time Data Platform.png>)

The image above is a **product vision and investor discussion graphic**, not a
claim that every illustrated capability is implemented. The current repository
contains the foundational local storage work only.

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
- Optional commercial capabilities in a separate private repository.
- Eventual adapters for governed contracts and analytical destinations, only
  after the core workflow is proven.

## Current status

The open-source repository has completed G0 and the approved G1 storage gate:

- Rust workspace with `streamd` and `streamctl` entry points.
- Strict configuration and foundational domain types.
- Local append log with RecordBatch v2-shaped framing and CRC32C.
- Durable checkpoints with certified-prefix recovery.
- Fault-injection and process-level crash-recovery tests.
- Derived sparse indexes, bounded group commit, and Linux Docker CI.

This is not yet a broker or production MVP. Kafka networking, real producers
and consumers, queues, replay UX, and the end-to-end developer workflow remain
future authorized gates.

## Roadmap to a demonstrable MVP

1. Finish and review G1 storage evidence.
2. Obtain explicit authorization for G2 Kafka compatibility work.
3. Implement a tested, deliberately narrow Kafka subset.
4. Add independent native broadcast and queue state.
5. Build the local developer workflow: publish, inspect, watch, diagnose, and
   safe replay preview.
6. Validate the workflow with real client and recovery tests before making
   production claims.

The current investor-demo hypothesis is a focused “publish → inspect → restart
→ recover” workflow. It is a target for the next authorized product gate, not
an existing feature claim.

## Open source and commercial tracks

This repository is the public open-source track. Paid or proprietary features
belong exclusively in the private
[Serapendiya-commercial repository](https://github.com/sumalperera/Serapendiya-commercial.git).
The repositories must remain separate in code, commits, release artifacts,
credentials, and licensing claims.

## Development checks

```sh
cargo build --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/run-g1-tests.sh
```

The Docker command requires Docker Desktop or a compatible Docker runtime.

See [docs/investor-pitch.md](docs/investor-pitch.md) for the ideation-stage
product narrative and [docs/roadmap.md](docs/roadmap.md) for delivery status.
