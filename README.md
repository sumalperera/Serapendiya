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
