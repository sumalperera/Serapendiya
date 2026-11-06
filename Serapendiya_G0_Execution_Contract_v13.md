# Serapendiya — G0 Execution Contract v13

**Status:** ONLY CURRENTLY AUTHORISED CODING WORK. **Parent specification:** `Serapendiya_AI_Implementation_Blueprint_v13.md`. **Product:** Serapendiya. **Working binaries:** `streamd`, `streamctl` (names provisional).

## 1. Non-negotiable scope

Implement **G0 only**, in PR01–PR03, one small PR at a time. Create an original, minimal Rust workspace, foundational domain types/error taxonomy, configuration parsing, CLI skeleton, CI, licence/provenance policy and architecture tests. Do not implement broker storage, Kafka wire protocol, durable log, queue, replay engine or any future capability.

**Absolute embargo:** Do not create files, crates, modules, traits, stubs, feature flags, configuration fields, tests or APIs for Schema Registry, schema evolution, Iceberg, DuckLake, MotherDuck, DuckDB materialisation, commercial packaging, licensing checks, entitlements, managed services, replication, tiering, WASM or MCP. Treat master blueprint Sections 17–19 as future product requirements only. Do not expand scope to be helpful.

## 2. Work and approval rules

1. Inspect the available Rust toolchain, registry metadata, licences and crate APIs before selecting and pinning dependencies. Do not invent versions or claim checks ran when they did not. Commit `Cargo.lock`.
2. Candidate crates, only if actually needed: `tokio`, `bytes`, `thiserror`, `tracing`, `tracing-subscriber`, `clap`, `serde`, `toml`, `tempfile`. Candidate names are **not** approval of any version. Do not add unused dependencies.
3. Draft ADR-001 (architecture/workspace), ADR-002 (Kafka scope and compatibility), ADR-003 (RecordBatch v2 storage), ADR-004 (durability and recovery). Label each `PROPOSED`; do not self-approve. ADR-001 and relevant workspace decisions must be accepted before G0 is merged. ADR-003 and ADR-004 must be accepted before any G1 storage implementation.
4. Separate ADR drafting from permission to implement. Stop and request founder approval for architectural decisions affecting persistent formats or compatibility.
5. Use original code; review provenance/licences before reusing upstream material. Prefer existing verified libraries where useful.
6. Prefer marking **UNVERIFIED**, a failing test or a documented blocker over invented behaviour. Stop and report when an acceptance criterion cannot be met.

## 3. PR sequence and deliverables

**PR01:** Minimal workspace, pinned toolchain and lockfile, basic binaries with `streamd --version` and `streamctl --help`, README, licence/provenance policy. Draft ADR-001.

**PR02:** Small, coherent domain primitives and error taxonomy, strict configuration loading and validation, unit tests. Draft ADR-002. Avoid speculative interfaces and runtime components.

**PR03:** CI with clean Linux locked build, formatting, lint, tests and architecture checks. Draft ADR-003 and ADR-004 with unresolved questions clearly marked. Collect reproducible evidence. Do not implement their storage designs.

## 4. Acceptance and verification

Run and capture exact output and exit codes for:

```sh
cargo build --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
streamd --version
streamctl --help
```

Use the actual binary paths or `cargo run --bin ... --` when appropriate, and record the command actually executed. On a clean Linux CI runner, repeat all applicable checks.

Every PR **must** include `docs/verification/PRxx.md` containing the exact command, exit code, raw relevant output or attached log path, commit SHA, toolchain version, dependency versions and lockfile hash, test coverage, deviations, unresolved questions and `UNVERIFIED` items. CI must reject missing gate evidence. No assertion of G0 completion without evidence.

## 5. Completion report and stop condition

At the end, provide: (a) changed files, (b) completed vs failed acceptance criteria, (c) exact verification references, (d) ADR-001–004 status and approval records, (e) dependency/licence provenance, (f) risks and blockers, and (g) the proposed first G1 PR **without implementing it**.

**Stop after G0.** G1 requires explicit founder approval **and** accepted ADR-003 and ADR-004. No completed milestone automatically authorises the next. S0 research must be approved before any schema S1–S5 work. ADR-013 must be accepted before any commercial packaging implementation.

## 6. Copy-ready agent handoff

> Implement only G0 from `Serapendiya_G0_Execution_Contract_v13.md`. Create the minimal original Rust workspace, domain types, config, CLI skeleton, CI and draft ADR-001 through ADR-004. Do not create any files, modules, traits, stubs, config fields or tests related to schema registry, Iceberg, DuckLake, MotherDuck, packaging, entitlements, replication, tiering, WASM or MCP. Treat Sections 17–19 of the master blueprint as future requirements only. Record exact verification commands, outputs and exit codes in `docs/verification/PRxx.md`. Prefer UNVERIFIED or a failed test over invented behaviour. Stop and report if any acceptance criterion cannot be met. Do not proceed to G1 without explicit founder approval and accepted ADR-003 and ADR-004.
