# Serapendiya

Serapendiya is an original Rust project. This repository implements the G0
foundation plus the first approved G1 storage slice: domain value types, strict
configuration validation, CLI entry points, and a crash-conscious local append
log.

## Local checks

```sh
cargo build --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run --bin streamd -- --version
cargo run --bin streamctl -- --help
```

The storage slice is not a broker, wire-protocol implementation, or production
durability claim. Its supported surface is documented in ADR-003 and ADR-004.

## Delivery tracks

This repository is the open-source Serapendiya track. Paid and commercial
features belong exclusively in the private [Serapendiya-commercial repository](https://github.com/sumalperera/Serapendiya-commercial.git)
and must not be implemented or copied into this repository.

The next open-source step is PR05: fault-injection and durability-failure
testing for the approved storage slice. See [docs/roadmap.md](docs/roadmap.md)
for the separation rules and sequencing.
