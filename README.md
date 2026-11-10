# Serapendiya

**Make sense of every event.**

Serapendiya is an early-stage open-source project exploring a real-time event
platform built in Rust. Its goal is to make event-driven systems easier to
build, inspect and understand, with a path toward safe replay and analytics.

Today the repository contains an **experimental local append-log foundation**.
It is not yet a broker or a production MVP. G1 storage acceptance, the on-disk
format and durability guarantees remain under review.

[What exists today](#what-exists-today) · [Getting started](#getting-started) ·
[Verification](#verification) · [Roadmap](#roadmap)

## What exists today

The current Rust implementation provides:

- A local append log with contiguous offsets, bounded group commits and an
  experimental container around a narrow Kafka RecordBatch v2-shaped record.
- Checksummed frames and checkpoints, with recovery that validates the
  certified prefix before discarding an uncertified tail.
- Exclusive advisory locking, fail-closed handling of ambiguous storage
  states, and an in-memory sparse index rebuilt on open.
- Fault-injection, process-crash and selected Linux filesystem replay tests.

The storage API accepts opaque values and builds one record per batch; it does
not accept arbitrary Kafka batches. A terminal append error can have an
**unknown outcome**: the record may be present after recovery, so retrying
without reconciliation can create a duplicate. The experimental on-disk format
is not stable, and there is no supported repair or migration tool yet.

Storage currently requires Unix; opens fail closed on non-Unix platforms. The
`streamd` and `streamctl` commands are early command-line entry points:
`streamd` can validate configuration, and `streamctl info` reports its version.
They do not run a broker or provide publish and consume commands.

## Getting started

Install Rust using the version in [rust-toolchain.toml](rust-toolchain.toml).
The declared minimum supported Rust version is 1.85; the pinned toolchain is
used for the project's regular formatting and lint checks.

```sh
cargo build --locked
cargo test --locked --workspace
cargo run --locked --bin streamd -- --help
cargo run --locked --bin streamctl -- info
```

These commands build and test the prototype and show the current CLI surface.
They do not start an event-streaming service.

## Verification

The project tests commit ordering, recovery and failure handling using injected
errors, process interruptions and independently generated fixtures for the
supported batch shape. Selected initialization and append boundaries have also
been replayed on disposable ext4 and XFS virtual filesystems using
`dm-log-writes`. Those tests model selected block-write outcomes; they are not
physical power-cut certification or an exhaustive durability proof.

The [G1 review](docs/verification/G1-review.md) summarizes the evidence and
remaining gaps. The [ADR reconciliation](docs/verification/G1-adr-reconciliation.md)
records open decisions about the container format, initialization and checkpoint
rollback. Detailed results are available for [initialization replay](docs/verification/G1-init-replay.md)
and [XFS replay](docs/verification/G1-xfs-replay.md).

For routine development checks:

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
./scripts/run-g1-tests.sh
```

The last command uses Docker. The separate block-device and VM replay scripts
are opt-in and destructive to their designated scratch disks; see their linked
verification documents before running them.

## Roadmap

The immediate focus is reviewing and completing the storage foundation. The
intended direction after that is a deliberately narrow Kafka-compatible client
boundary, followed by queue and broadcast state and a developer workflow for
publishing, inspecting and safely replaying events. These are planned
capabilities, not features available today. See [the roadmap](docs/roadmap.md)
for the current gates and dependencies.

## Product vision

![Serapendiya platform vision](<Serapendiya Real-Time Data Platform.png>)

*Concept illustration: the capabilities shown include planned features and do
not represent the current product.*

The longer-term hypothesis is an open-source streaming core with clear tools
for understanding event history and state, plus eventual integrations for
governed contracts and analytics.

## License

Serapendiya is licensed under the [MIT License](LICENSE).
