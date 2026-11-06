# Serapendiya — AI Implementation Blueprint v13

**Status:** Original-code implementation specification, not a claim of verified correctness or working software. **Date:** 7 November 2026. **Audience:** coding agents and human reviewers. **Precedence:** this document supersedes v1–v12 implementation instructions. Earlier documents are historical research only. **Product name:** Serapendiya. CLI and binary names `streamd` / `streamctl` remain working names until ADR approval.

## 0. Operating contract for the coding agent

You are building an **original Rust implementation**. Study public specifications and open-source projects (Krabka, Apache Kafka, InfinyOn/Fluvio) for behaviour, interoperability tests and developer experience, but **do not copy code, APIs, assets or text** without explicit approval and a documented licence/provenance review. Prefer established, permissively licensed libraries over reimplementing codecs, CRC, object storage or Parquet. Never claim a capability is implemented until its acceptance test runs.

**Current authorization: G0 only. The separate `Serapendiya_G0_Execution_Contract_v13.md` is the sole execution handoff for coding agents. Sections 17–19 are future product requirements, not current tasks. G0 does not authorize any schema, analytics, packaging, licensing, entitlements, replication, tiering, WASM or MCP code. G0-only instructions take precedence over any future design examples.

**G0 authorization remains unchanged:** G0 only. G1 may begin only after G0 evidence and ADR-003/004 are reviewed and explicitly approved. G2 onward is reference scope, not authorization.**

**Agent stance:** Implement only the currently authorized gate. Prefer failing a test or marking **UNVERIFIED** over inventing behaviour or expanding scope. Do not expand scope. Stop and report if any acceptance criterion cannot be met with current information.

**Hard rules:**
1. Work **one numbered milestone and one small PR at a time**. Run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and the milestone's external integration tests. Fix failures before continuing. Record exact commands and outputs in `docs/verification/`.
2. Never invent crate versions, upstream feature support, Kafka API versions, benchmark results, licences or production readiness. Before pinning dependencies, inspect the actual registry metadata, crate APIs and upstream release docs. Commit `Cargo.lock` and use `cargo build --locked` in CI. If internet or execution is unavailable, mark **UNVERIFIED** and stop at that dependency.
3. Make all state-changing operations explicit, authorized, auditable and idempotent where applicable. No destructive replay, ACK reset, DLQ redrive or object eviction as a side effect of a read-only command.
4. Preserve crash consistency and ordering ahead of performance. Any uncertain `fsync`/`fdatasync` failure puts the partition in a **fatal, unavailable** state; do not retry writes or infer durability from page cache. Recover only through a documented restart procedure.
5. Each PR includes: purpose, contracts/invariants, tests, documentation, failure modes, and benchmark impact. Do not proceed if any correctness test fails.
6. Never silently weaken an acceptance criterion. Record changes as an ADR and ask for review.
7. **Future-feature embargo (including schema registry):** Do not create files, crates, modules, traits, stubs, configuration fields, API endpoints, feature flags or tests for R3–R8 until the corresponding ADR is approved and all preceding gates are closed. Future sections are constraints for later design, not tasks.
8. **Architecture approval is separate from implementation approval:** Writing an ADR does not approve it. Any decision affecting disk layout, durability, Kafka compatibility or persistent state requires a human approval record before code lands.
9. Avoid speculative abstractions: clean boundaries on control paths; concrete types, preallocated buffers and static dispatch where useful on the hot path.

## 1. Product, differentiation and explicit scope

**Product:** Rust-native Kafka-compatible event platform with a shared append-only event log, **independent** broadcast and work-queue subscription state, first-class inspection and safe replay, ergonomic local CLI, eventual MCP, WASM processing, S3 tiering, a developer-first schema contract platform, and a multi-destination analytics materialisation platform spanning Apache Iceberg, DuckLake, MotherDuck and extensible adapters.

**Competitive truth:** Kafka share groups, Pulsar shared subscriptions, Redpanda tiering/Iceberg, and Fluvio WASM/CLI already cover parts of this vision. Rust is not unique (Krabka and Fluvio are Rust-based). The proposed wedge is a coherent, measurable developer workflow for inspecting, explaining, replaying and debugging streams and queues. Do not call any competitor limitation proven without pinned version, reproducible test and evidence.

**MVP (release R1):** single process, one node, durable local log, one partition per topic initially, documented Kafka `Produce`/`Fetch`/metadata subset, **manual partition assignment only** for external Kafka consumers, native broadcast cursor, native work queue with leases/ACK/retry/DLQ, read-only replay preview, CLI, basic metrics. No Raft, no KRaft, no Kafka consumer group coordinator, no share-group protocol, no transactions, no idempotent producer support, no tiering, no Iceberg, no WASM, no cloud control plane, no MCP mutation. A narrow read-only MCP adapter may follow after R1.

**Future releases:** R2 production hardening and client compatibility; R3 replication; R4 S3 tiering; R5 schema contract management; R6 analytics materialisation (Iceberg, DuckLake, MotherDuck); R7 WASM/connector SDK and advanced MCP; R8 managed cloud and enterprise operations. Each capability has independently authorised sub-gates; release labels are sequencing aids, not dates. These are dependency stages, **not promised dates**. Do not present future interfaces as working features.

## 2. Architecture and dependency rules

```text
External Kafka clients ---> kafka_wire adapter ----\
Native CLI/API -----------> http/grpc adapter -------+--> application services --> domain policies
MCP (later) --------------> read-only adapter ------/
                                                   |
                                        partition actor (data plane)
                                                   |
                                append-only log + indexes + sync coordinator
                                                   |
                                             local filesystem

Independent native subscription state --> queue lease/ACK journal --> local filesystem
Read-only replay --> log reader (never mutates live cursors)
Later adapters: Raft | object storage | schema service | Iceberg | DuckLake | MotherDuck | WASM | telemetry | managed control plane
```

**Dependency direction:** domain types and invariants know nothing of Tokio, sockets, filesystem or Kafka wire framing. Application ports define behaviour. Infrastructure implements ports. **Exception:** the partition actor and log I/O are an explicitly performance-oriented data plane: avoid trait-object calls and allocations per record; use concrete implementation behind coarse-grained boundaries. Do not make an `async_trait` for every method or pass domain events through unnecessary conversions.

**Target workspace (not a G0 creation checklist):**
```text
Cargo.toml / Cargo.lock
crates/domain/       event identity, offsets, topic policy, queue states, errors
crates/logstore/     append, segment scan, indexes, durability, fault injection
crates/broker/       partition actor, admission, group commit, queue engine
crates/kafka_wire/   protocol adapter and version negotiation
crates/admin_api/    native HTTP API, auth boundary
apps/streamd/        configuration, startup, graceful shutdown
apps/streamctl/      local CLI
integration-tests/  external Kafka client tests, crash harness
benchmarks/         throughput, tail latency, CPU, RSS
 docs/adr/ docs/verification/ docs/contracts/
```

For G0, create only the minimum crates needed to compile and demonstrate CLI/config/error boundaries; do not create empty placeholder crates. Later split out `replay`, `object_store`, `schema_registry`, `materialization_core`, `iceberg_sink`, `ducklake_sink`, `motherduck_sink`, `wasm_runtime`, `mcp_server`, `cluster`, `cloud_control_plane` only when real implementations justify it. Use Rust edition and MSRV supported by verified dependencies; pin in `rust-toolchain.toml`. Tokio for networking/coordination; use blocking storage workers or measured async filesystem integration, never block Tokio core workers on `fsync`.

## 3. Canonical data model and semantics

- **Topic/partition:** ordered sequence of immutable Kafka RecordBatch v2 batches with broker-assigned base offsets. One partition initially; a later extension must not reinterpret existing offsets.
- **Headers:** ordered `Vec<Header { key: String, value: Option<Bytes> }>`; preserve duplicates and null values. Validate Kafka protocol's byte representation at the boundary.
- **Payload:** opaque bytes; do not assume JSON. Key/value nullable. Timestamps and compression follow the selected Kafka record format.
- **Broadcast:** each independent native subscriber maintains its own durable cursor; read operations do not automatically commit the cursor. Explicit checkpoint with generation fencing.
- **Queue:** named queue subscription over a topic/partition. Each record transitions `available -> leased -> acknowledged` or `leased -> available` on timeout/retry; exhausted attempts -> DLQ via an atomic or recoverable journaled transition. Queue progress does **not** delete source records or advance unrelated broadcast cursors.
- **Retention:** source log retention is independent of queue ACK. Before deleting source records, check all configured retention pins, outstanding leases, replay jobs and tiering state. For R1, **disable automatic deletion** and impose configurable disk quota/backpressure; introduce retention only with a separate tested policy.
- **Delivery guarantees:** at-least-once for queue processing, duplicates possible on lease expiry or crash. ACK must carry `(subscription, partition, offset, lease_generation, token)`; reject stale ACKs. No claim of end-to-end exactly-once.
- **Replay:** read-only bounded `preview(topic, partition, from_offset, max_records, max_bytes)` with optional timestamp lookup (only after timestamp index exists). No cursor movement, queue re-enqueue, external sink invocation or transformation side effects. Later `redrive` is a separate authorized, idempotent job with destination, dedupe key, rate limit, audit log and explicit confirmation.

### Contract examples (illustrative, not prevalidated code)

```rust
pub struct Header { pub key: String, pub value: Option<bytes::Bytes> }
pub struct Record { pub key: Option<bytes::Bytes>, pub value: Option<bytes::Bytes>, pub headers: Vec<Header> }
pub struct LeaseId { pub generation: u64, pub nonce: u128 }
```

Use checked arithmetic for offsets and lengths. Explicitly reject oversized batches, malformed varints, negative lengths where forbidden, unsupported codecs, invalid CRCs and impossible timestamp/offset deltas.

## 4. Storage format, append, ACK and recovery: non-negotiable

### 4.1 Format

Prefer **Kafka RecordBatch v2 bytes on disk** rather than inventing a second event encoding. **ADR-003 must be reviewed and approved before any G1 storage code lands.** Its required decisions are: record-batch framing and segment header, broker-assigned base offsets, batch-length validation, CRC32C coverage (from `attributes` through the batch payload, excluding base offset), handling of multi-record batches and relative offset deltas, timestamp bounds, compression acceptance/rejection, producer metadata restrictions, and index rebuilding. The chosen protocol codec and test fixtures must be verified against the Kafka record format; do not assume that changing the base offset is sufficient validation. Implement a thin segment container and sparse offset/time indexes. Broker-assigned base offset changes require validation of the entire batch and CRC rules: Kafka's batch CRC covers fields beginning at `attributes`, not the base offset; nonetheless validate all invariants and never assume rewriting offsets alone makes arbitrary producer batches valid. For R1, accept only a **documented subset** (e.g., uncompressed and selected verified codecs; nontransactional, non-idempotent batches). Return correct Kafka errors for unsupported features. Document whether multiple records per batch are supported and how offsets are allocated.

### 4.2 Durability

Define separate `append_position`, `visible_offset`, and `durable_offset`. A successful *durable native write* means all referenced log bytes and required metadata are persisted under the documented sync protocol. Kafka `acks=all` by itself is **not** an fsync guarantee; in single-node R1 choose and document an explicit mapping, and do not advertise replicated durability.

**Group commit:** collect pending append requests; flush and sync in bounded groups; complete only the promises covered by the successful sync. Expose batch size, sync duration, queue depth and p99 ACK latency. Never acknowledge ahead of the configured durability barrier. Provide an explicit non-durable development mode only if visibly labelled.

**Durable boundary:** maintain a crash-safe checkpoint for the highest known fully synced byte position / batch offset. A standalone watermark file is insufficient: define an ordered write + sync + atomic checkpoint protocol, including parent-directory sync where needed, and a generation/checksum or dual-slot checkpoint to detect torn metadata. A crash can leave synced log bytes beyond the last checkpoint: recovery may conservatively treat these as unacknowledged, but **must not declare them durable** without proof. An invalid batch **within** the certified durable range is fatal corruption, not a truncatable tail. Invalid bytes strictly after that boundary may be truncated only under a documented, tested tail-recovery policy. Be precise about the acknowledgement/checkpoint ordering: never claim a successfully ACKed record is protected by a checkpoint that was not durably committed before ACK.

**fsync failure:** transition to terminal partition error; reject new writes and pending acknowledgements; operator intervention/restart with verification required. Never silently retry and report success.

### 4.3 Recovery and filesystem matrix

On startup: acquire exclusive data-dir lock; validate format version and checkpoint checksum/generation; scan segments and indexes; detect overlaps, gaps and CRC errors; reconstruct indexes; distinguish invalid certified bytes (hard fail) from invalid uncertified tail (recover according to policy); never silently lose acknowledged data. Test abrupt `SIGKILL`, partial writes, torn checkpoint, corrupt index, corrupt durable batch, disk-full, rename failure, directory sync failure and sync failure. Document OS/filesystem assumptions (initially Linux + ext4/XFS) and use a fault-injectable storage backend. Avoid implying portable crash guarantees across all filesystems.

## 5. Kafka wire compatibility: implement a tested subset, not a slogan

1. Pin a verified Kafka protocol library and client versions after inspecting actual available crates. The previous `kafka-protocol` 0.18.0 suggestion was **not verified**; do not use it blindly.
2. Implement framed requests, correlation IDs, request/response headers, error codes, and flexible versions only for explicitly selected API versions. **`ApiVersions` responses use the version-0 response header** even where flexible-version negotiation exists; validate with captured protocol fixtures.
3. Support `ApiVersions`, `Metadata`, `Produce`, `Fetch`, `ListOffsets` as needed for manual-assignment clients; determine whether `FindCoordinator` and other APIs are actually required by target clients, and return correct unsupported errors where possible. Advertise **only implemented API versions**.
4. Test with an actual JVM Kafka client and `librdkafka`/`rdkafka`, using **manual `assign()`**, `enable.idempotence=false`, no transactions, and no group coordination. Modern producers may enable idempotence by default; document explicit compatible settings.
5. For every API, add golden wire fixtures, malformed-frame fuzz/property tests, response-header checks, compression tests, and cross-client smoke tests. `kcat` or equivalent external tooling must be included **before R1 release**, not optional or deferred.
6. Do not claim Kafka consumer groups, Kafka share groups, transactions, exactly-once, or KRaft interoperability in R1.

## 6. Native subscription engine

**Design:** single-writer partition actor owns log offset assignment and write admission. Queue state uses an independent durable journal/checkpoint with deterministic replay on restart. No blocking filesystem calls on actor's Tokio executor. Bound in-flight requests, active leases, bytes and per-subscription work. Fair scheduling prevents a slow queue consumer from starving ordinary fetch.

**Queue lease invariant:** only one current lease generation for a `(subscription, partition, offset)`; timeout increments generation; late ACK rejected; ACK persisted before response; duplicate ACK of same token returns idempotent success if still in the dedupe horizon. Retries and DLQ transitions survive crashes. Reconcile log append and queue journal independently; do not expose a queue record before its source record satisfies visibility/durability contract.

**DLQ:** either append to a dedicated topic and persist the transition with recoverable idempotency, or defer DLQ to R1.1 if no atomicity strategy is proven. Never mark a message completed before its DLQ copy is durable. Include max attempts, exponential backoff, poison-message diagnostics and operator re-drive only later.

**Broadcast:** explicit checkpoint updates, no hidden auto-commit; on restart resume at committed cursor and tolerate duplicate delivery since last checkpoint. Replays use separate readers and cannot change checkpoint state.

## 7. Developer experience: the initial wedge

Proposed commands (do not pretend they already exist):

```bash
streamd dev start --data-dir ./data
streamctl topic create orders --partitions 1
streamctl publish orders --key order-1 --json '{"status":"new"}'
streamctl watch orders --from earliest --limit 10
streamctl subscription create workers --topic orders --mode queue --lease 30s
streamctl queue claim workers --max 1
streamctl queue ack workers --offset 0 --lease-token <token>
streamctl queue inspect workers
streamctl replay preview orders --from-offset 0 --limit 25 --max-bytes 1048576
streamctl diagnose topic orders
```

CLI should offer structured `--output json`, understandable errors, local one-command setup, no cloud login, command completion later, and safe read-only defaults. Diagnostics should explain why a record is unavailable (retention, uncommitted write, leased, ACKed, invalid offset) rather than simply print lag. Avoid echoing sensitive payloads by default in diagnostics; require an explicit reveal flag and authorization boundary. Benchmark time-to-first-event and time-to-diagnose against pinned competitor versions.

## 8. API, security and observability

Native admin HTTP API must bind to loopback by default. For R1, require explicit configuration to expose beyond loopback; add authentication/authorization before any remote multi-user deployment. Separate read metadata, read payload, produce, ACK, replay preview and admin capabilities. Sanitize logs; cap request sizes; set timeouts; bounded concurrency. TLS and multi-tenancy are **release gates** for networked production, not optional decorations.

Metrics: append/fetch rates, ACK latency histogram, sync duration, pending group commits, actor mailbox depth, disk space, recovery failures, queue ready/inflight/retry/DLQ counts, stale ACK rejections, replay scan bytes and CLI errors. OpenTelemetry-compatible exporter after stable metric definitions; New Relic via standard telemetry export rather than custom hot-path integration. Use structured `tracing` spans and redact payloads/tokens.

## 9. Later capabilities: design now, implement only after R1

### 9.1 Distributed replication (R3)

Choose and document a consistent metadata/leader-election design (Raft or other verified protocol), term fencing, quorum commit, leader epochs, log truncation, snapshot/compaction, partition movement, client metadata and failure modes. Do **not** equate local fsync with quorum durability. Kafka KRaft compatibility is a separate scope decision. Never bolt Raft into the R1 log without an ADR covering log format and durability invariants.

### 9.2 Tiered S3 storage (R4)

Treat remote segments as immutable, content-addressed or uniquely identified objects. Upload -> verify checksum/size -> durably publish manifest -> only then permit local eviction. Manifest versions need atomic compare-and-swap or equivalent metadata fencing; object listing is not a transaction. Recovery reconciles manifests and orphan uploads. Fetch must handle cache miss, range reads, object unavailability, remote corruption and retention. Cloud-primary is **not** synonymous with tiered storage and remains a separate future design. Never delete local data merely because an upload request returned success.

### 9.3 Apache Iceberg (R5)

Implement an **independent asynchronous sink**, not an alternate representation of the Kafka source log. Consume durable offsets -> convert/schema validate -> write Parquet -> commit Iceberg snapshot through verified REST/catalog APIs -> durably record checkpoint. Crash between catalog commit and checkpoint can duplicate data: define deterministic file IDs, commit reconciliation or downstream deduplication; do not promise exactly-once without proving the chosen strategy. Support schema evolution and partition spec changes only after compatibility tests. Sink failures must not block source append; bounded sink lag/backpressure and retention pins required. Verify current `iceberg`/`iceberg-catalog-rest`, Arrow and Parquet crate versions at implementation time.

### 9.4 WASM and connectors (R6)

Sandbox with Wasmtime or another verified runtime; enforce fuel, memory, wall-time, permissions and deterministic versioned transform artifacts. Distinguish stateless transforms from stateful processing with checkpointing/watermarks. Do not execute arbitrary WASM in the partition append critical path without isolation and performance tests. Connector SDK includes retry, dedupe, secrets and DLQ contracts.

### 9.5 MCP (R2+)

Start read-only: list topics, inspect metadata, sample/redact records, explain consumer/queue state, preview replay. Scoped tokens, tool-call auditing, payload redaction, rate limits, and clear server-side validation. Never expose ACK, replay redrive, delete or reset via MCP until explicit authorization and confirmation are designed and tested. MCP is an adapter, not a source of business logic.

## 10. Implementation sequence and PR-level acceptance gates

**G0 — Compiling skeleton (PR01–PR03; ONLY CURRENTLY AUTHORIZED GATE).** Initialize workspace, CI, toolchain, licence policy, architecture tests, error taxonomy, config parsing and basic CLI help. `cargo build --locked`, fmt, clippy and tests pass on clean Linux. `streamd --version` and `streamctl --help` run. No invented dependencies.

**G1 — Durable log (PR04–PR09; REQUIRES EXPLICIT AUTHORIZATION).** Segment/batch reader-writer, offsets, indexes, group commit, checkpoint protocol, startup recovery and fault-injection harness. Prove crash invariants under sustained write load and repeated fault injection; **no append-count or throughput target is an acceptance criterion**. Any workload volume is a configurable test-harness parameter, not a performance promise. Acceptance: persisted data survives kill/restart, valid uncertified tail handled per policy, corrupt certified data hard-fails, injected fsync error terminal, acknowledged offsets never disappear in supported filesystem test matrix.

**G2 — Kafka subset (PR10–PR14; NOT AUTHORIZED).** Network framing, ApiVersions, Metadata, Produce, Fetch, ListOffsets, fixture tests, external clients, `kcat` smoke tests. Acceptance: JVM and `librdkafka` clients can produce/fetch with documented manual assignment/idempotence disabled; unsupported APIs fail honestly; no false feature advertisement.

**G3 — Native subscriptions (PR15–PR19; NOT AUTHORIZED).** Broadcast checkpoints, queue durable journal, lease generation/fencing, ACK, retry/recovery, optional DLQ after proof. Acceptance: concurrent stream and queue readers see same source events independently; restart and timeout produce at-least-once but no permanently lost queue item; stale ACK rejected; bounded queue load sheds gracefully.

**G4 — Developer workflow and replay (PR20–PR24; NOT AUTHORIZED).** Native admin API, CLI, watch/inspect, bounded read-only replay, diagnostics, basic security/metrics and docs. Acceptance: fresh install -> publish -> watch -> queue claim/ACK -> preview replay without changing state, demonstrated by automated end-to-end script. No secret payload exposure by default. **This completes R1** after a release review.

**G5 — Hardening (PR25+; NOT AUTHORIZED).** Fuzz wire parser, stress and soak, benchmark reproducibility, TLS/auth, operator guide, recovery playbooks, performance profiles, security review. R2 MCP read-only may follow. R3–R8 are separately scoped epics, not automatically part of R1.

Each PR must create `docs/verification/PRxx.md` recording git commit, toolchain, dependency versions, commands, results, unresolved issues and deviations. CI must fail on missing verification for a gate closure. Each evidence file must contain the exact commands, exit codes, relevant raw outputs or attached log paths, commit SHA, toolchain and dependency lockfile hash; do not replace failures with prose summaries.

## 11. Required test matrix

| Test class | Required cases | Gate |
|---|---|---|
| Unit/property | batch roundtrip, CRC, varint bounds, offsets, header duplicates/nulls, integer overflow | G1/G2 |
| Fault injection | partial write, ENOSPC, EIO, fsync failure, checkpoint tear, directory sync error | G1 |
| Recovery | kill at each append/checkpoint boundary; corrupt certified batch hard fail; index rebuild | G1 |
| Kafka interop | JVM + librdkafka manual assign, disabled idempotence, unsupported API errors | G2 |
| Queue state | timeout, stale ACK, duplicate ACK, crash after claim/ACK, backoff, poison record | G3 |
| Replay | bounds, read-only state, missing offsets, retention policy, no side effects | G4 |
| Security | loopback default, auth boundaries, oversized request, payload redaction | G4/G5 |
| Performance | p50/p95/p99, throughput, CPU, RSS, fsync rate, disk bandwidth, batch sizes | G5 |
| S3 (later) | upload failure, manifest race, object loss, corruption, eviction safety | R4 |
| Iceberg (later) | crash before/after catalog commit, duplicate reconciliation, schema evolution | R5 |

**Benchmark fairness:** compare equal payload size, compression, partitions, client settings, replication factor, durability/fsync policy, hardware, disk, warmup and measurement duration. Publish commands and raw results; don't claim faster than Kafka/Krabka/Fluvio until reproduced.

## 12. Architecture decision records and approval schedule

**Approval rules:** ADRs start as `PROPOSED`; only an explicit human decision changes status to `ACCEPTED`. Record reviewer, date, alternatives, invariants, rollback/migration implications and source evidence. An agent must not self-approve.

**G0:** Draft ADR-001, ADR-002, ADR-003 and ADR-004 as part of early PRs. ADR-001 and workspace/toolchain decisions must be accepted before merging G0. ADR-002 through ADR-004 may remain proposed during G0, but **ADR-003 and ADR-004 must be accepted before any G1 storage implementation**. ADR-005 must be accepted before partition actor implementation. ADR-006 must be accepted before G3 queue code. ADR-007 before replay code.

**ADR-003 mandatory checklist:** Segment framing; Kafka RecordBatch v2 layout; CRC32C exact coverage; producer-supplied vs broker-assigned base offsets; multi-record batch and relative offset semantics; compressed-batch validation; indexes; recovery scans; fixture corpus; compatibility test against an independent decoder. Do not silently mutate CRC-covered fields.

**ADR-004 mandatory checklist:** Meaning of acknowledged/certified durable range; group-commit ordering; durable watermark/checkpoint format; dual-slot or equivalent atomicity; file and directory sync order; recovery handling of certified corruption vs valid uncertified tail; fatal sync errors; disk-full behaviour; platform/filesystem assumptions.

**ADR-006 mandatory checklist (before G3):** Concrete journal record layout with version, length, CRC, subscription identity, offset, lease generation, token, transition, timestamp and sequence; journal fsync-before-ACK; checkpoint generation/dual-slot or equivalent; crash ordering between source log and queue journal; replay/rebuild; stale ACK and retry fencing; DLQ atomicity or recoverable two-step transition; expired source records; poison-message handling. No implementation until these are approved.

## 12A. Required failure-state matrix

| Failure | Expected behaviour | Evidence gate |
|---|---|---|
| Torn append beyond certified boundary | Discard or preserve only per accepted recovery policy; never claim ACK | G1 |
| Invalid batch below certified boundary | Fail startup/partition; never silently truncate acknowledged data | G1 |
| `fsync` / `fdatasync` returns error | Terminal unavailable partition; no page-cache retry or success ACK | G1 |
| Checkpoint slot torn or invalid | Use last independently validated committed slot; fail if certification cannot be proven | G1 |
| Directory sync failure on new segment/rename | Do not certify new durable range; terminal error when durability uncertain | G1 |
| Disk full / partial write | Return error; maintain certified prefix and stop unsafe append path | G1 |
| Queue claim persisted but response lost | Lease may be redelivered after expiry; no permanent loss | G3 |
| ACK journal write/flush fails | Never report ACK success; stop affected subscription safely | G3 |
| Stale lease generation ACK | Reject; no state mutation | G3 |
| Crash during DLQ transition | Reconcile journaled transition without losing source event | G3 |
| Replay preview interrupted | No production cursor, lease or queue-state changes | G4 |

## 12B. Initial dependency policy

The **candidate allow-list** for G0/G1 is `tokio`, `bytes`, `thiserror`, `tracing`, `tracing-subscriber`, `clap`, `serde`, `toml`, `crc32c` (or an independently verified CRC32C implementation), and test utilities such as `tempfile` and `proptest` where needed. This is **not a pinned or preapproved version list**. For every selected crate, inspect current registry metadata, Rust/MSRV requirements, licence and APIs; record versions in `Cargo.lock` and the PR evidence. Prefer the smallest actual dependency set. Additional crates require justification and approval. Do not use unmaintained YAML parsers by default.

## 12C. Merge and gate checklist

A PR is not mergeable unless: required ADRs are accepted; no unauthorized scaffolding exists; `docs/verification/PRxx.md` contains reproducible evidence; clean locked build, formatting, lint and tests pass; failures are recorded rather than suppressed; and any acceptance-criteria changes have human approval.

## 12D. Architecture decision records required before coding

- ADR-001: Why original implementation vs fork, upstream evidence, licensing.
- ADR-002: Kafka API/version subset, client matrix and protocol codec.
- ADR-003: Segment format, base-offset rewrite, CRC and compression.
- ADR-004: Durability checkpoint protocol and filesystem assumptions.
- ADR-005: Single partition owner and concurrency/backpressure.
- ADR-006: Queue state journal, fencing, ACK and retry/DLQ semantics.
- ADR-007: Replay preview vs replay job vs redrive; security boundary.
- ADR-008: Retention, quota and pins (no automatic retention in R1).
- ADR-009 (later): replication, metadata and fencing.
- ADR-010 (later): S3 manifests and cache/eviction.
- ADR-011 (later): Iceberg catalog commit/checkpoint reconciliation.

## 13. Source-of-truth and dependency verification

Before coding each integration, read **current** upstream specs and record URLs, versions and commits: Apache Kafka protocol guide (`kafka.apache.org/protocol`), Kafka record format, Kafka client docs, Rust crate registry docs, Rust standard library file sync semantics, Apache Iceberg spec and Rust implementation, S3 object store semantics, Wasmtime documentation, MCP spec, Krabka active repositories, Fluvio docs. Do not rely on a stale description of their functionality. License checks must cover source, dependencies, NOTICE, assets and trademarks; inspiration is not authorization to copy code.

Create `docs/competitive-evidence.md` with feature, competitor, pinned release, tested workflow, command, observed result and conclusion. No untested negative claims.

## 14. Agent handoff prompt (copy into your coding agent)

> Implement **G0 only** from `Serapendiya_AI_Implementation_Blueprint_v13.md`. Create an original Rust workspace and small, coherent domain model. First inspect available Rust toolchain and crate versions; pin verified dependencies and generate `Cargo.lock`. Follow clean architecture at module boundaries but avoid per-record dynamic dispatch. Add CI, draft ADR-001 through ADR-004 (do not self-approve them), and a `docs/verification/G0.md` with the exact commands and results. Run fmt, clippy, tests and locked build. Do not implement broker storage, Kafka, queue, S3, Iceberg, WASM or MCP in G0. If any dependency or command cannot be verified, state the blocker rather than claiming completion. Finish with a list of changed files, evidence, remaining risks and the exact next PR proposal. Do not create any R3–R8 files, modules, traits or stubs, including schema registry, schema-aware ingestion or Iceberg schema mapping. Do not expand scope. Stop and report if any acceptance criterion cannot be met with current information. Do not proceed to G1 without explicit human approval and accepted ADR-003/004.

## 15. Definition of ready / definition of done

**Ready for implementation:** explicitly accepted ADR, concrete invariant, failure matrix, external reference/version, acceptance tests and scope. **Done:** code compiles in a clean checkout, automated tests and required external clients pass, fault-injection evidence exists where relevant, docs updated, security implications reviewed, benchmark methodology documented, and no critical unresolved issue is disguised as complete.

**Final note:** this document is a detailed, internally scoped instruction set, **not** a proof of correctness. The first meaningful validation is G0 compilation; the most important validation is G1 crash safety. Do not advertise production durability or Kafka compatibility until their executable tests pass.


## 16. Revision history

- **v8:** Incorporated external review: strict authorization, future-feature embargo, early ADR approvals, precise RecordBatch v2 decision requirements, queue journal design gate, failure matrix, starter crate candidates, removed synthetic append volume target, and stronger verification evidence.
- **v7:** Original staged agent blueprint.


## 17. Schema Registry and Data Contract Platform (research-backed, NOT AUTHORIZED)

**Status:** Future product specification and discovery hypothesis only. This section is not permission to create code, modules, schemas, API routes, database tables, crates or scaffolding. The G0-only rule and ADR approval gates remain authoritative. Schema Registry must never be placed on the R1 broker ingestion critical path.

### 17.1 Research method, evidence, and what developers actually asked for

This product direction is based on public vendor documentation, an independent tooling comparison and qualitative developer discussions, **not** customer interviews, a representative survey or measured market demand. Treat themes as hypotheses to validate in 10–15 interviews with platform engineers, application engineers and data engineers. Record evidence and interview notes before claiming a differentiator.

| Evidence / expressed pain | Existing capability and gap | Serapendiya response (hypothesis) |
|---|---|---|
| Breaking schema changes are operational migrations, not merely compatibility flags (Reddit ExperiencedDevs, April 2026) | Registries can reject incompatible changes, but cannot automatically coordinate producers, consumers and retained historical data | **Migration planner**: impact graph, dual-publish/dual-read checklist, historical replay impact and staged rollout guidance; never silently execute migrations |
| Developers repeatedly need to identify deserializers while inspecting topics (Factor House 2026 tooling comparison) | Registry ID lookup and serializer selection can require several tools | **Decode inspector**: identify known framing, resolve schema ID, display field-level decoded records and failures; explicit unknown-format state |
| Developers want compatibility feedback before merging (Factor House; Confluent compatibility API) | Confluent already supports pre-registration compatibility checks and verbose errors | **CI-first schema diff**: GitHub/GitLab checks, structured field-level change classification, actionable migration suggestions; do not claim preflight checking is unique |
| Governance, ownership and documentation are spread across tools (Apicurio supports broader artifacts; Confluent has data contracts) | Existing registries already support parts of contracts, tags, references and rules | **Discoverable contract portal**: owners, consumers, SLAs, examples, provenance, PII classifications and review history, with a clear evidence-backed scope |
| Evolving event schemas into Iceberg needs stable field identity (Iceberg specification) | Iceberg catalog has its own schema IDs and field IDs; registry schema IDs do not equal Iceberg field IDs | **Schema-to-table evolution preview** with persistent mapping, explicit approvals and unsupported-conversion diagnostics |
| Registry migrations risk breaking old messages when IDs change (registry migration literature) | Compatibility APIs do not guarantee ID preservation during import | **Migration dry-run**: compare source and destination subjects, schema IDs, contexts, references and wire framing; require explicit import-mode design |

**Competitor accuracy constraints:** Apache Kafka brokers do not ship a canonical built-in Schema Registry; Confluent Schema Registry is a separate product. Redpanda includes an integrated registry with Avro/Protobuf/JSON Schema, normalization, contexts and compatibility testing. Confluent already has verbose compatibility diagnostics, schema references, contexts, metadata and rule sets. Apicurio supports a wider artifact registry. Do not describe any of these as absent across the board. Benchmark actual UX and API behaviour before claiming Serapendiya is better.

### 17.2 Positioning and prioritized jobs to be done

**Positioning:** A schema registry that helps developers **understand the effect of a change before shipping it**, rather than merely storing schema versions. The moat, if validated, is an integrated contract-change workflow from producer through consumers, replay and Iceberg, with standard compatibility APIs.

Priority:
- **P0 (first schema milestone, after R1 and separate authorization):** Confluent-compatible registry *subset*, Avro schema validation and references, version lookup, compatibility preflight, schema diff with precise explanations, CLI + read-only event decoding; optional external registry adapter as a quicker first route. Support for Protobuf and JSON Schema is phased and must not be falsely advertised.
- **P1:** Protobuf and JSON Schema with pinned supported dialects; consumer/producer ownership and dependency graph; PR checks; structured impact assessment; schema migration import/export with ID-preservation checks; UI.
- **P2:** Iceberg schema mapping preview and approved evolution; lineage and contracts; staged migration planner; optional policies and redaction.
- **P3 (only if validated):** Schema-aware broker enforcement, automated transformation proposals, AI explanation assistance, advanced governance and multi-region registry. AI may explain or draft but cannot silently approve or mutate contracts.

### 17.3 Separation of responsibilities and trust boundaries

```text
Kafka clients / SDKs -- Confluent-compatible Schema Registry API (optional standalone service)
                              |
                       schema service
                   /       |       \
           compatibility  immutable   audit / governance
              engine      schemas
                              |
                    decoding / inspection service
                              |
                     replay preview (read only)
                              |
                     Iceberg mapping planner
                              |
                     Iceberg sink / REST catalog (later)
```

- Broker stores opaque Kafka payload bytes and remains available when schema service is down, unless a *separately approved* enforcement mode is configured. Do not require a registry round-trip on every Produce or Fetch.
- Registry storage is a separate durable metadata concern. Its backing store, transactions, ID allocator, restore semantics, namespace model and high availability need an ADR; **do not** casually use the broker's `_schemas` topic before its correctness is established.
- API compatibility is **not** equivalent to serialization-wire compatibility. Validate Confluent framing (`magic byte`, 4-byte schema ID) and format-specific Protobuf message-index framing against official client fixtures. Handle raw Avro and schemaless records explicitly.
- Distinguish schema **subject**, **version**, **registry ID**, **content fingerprint**, **namespace/context**, **topic association** and **Iceberg table schema ID**. None are interchangeable. IDs must remain stable for retained data and across restore/import as promised.
- Do not rewrite old message schema IDs during registry migration unless a separate, verified re-encoding job is explicitly requested.
- Schema compatibility is format-specific, not a universal property. JSON Schema `$ref` resolution, Protobuf reserved field numbers and Avro defaults must each have independent test fixtures. Semantic compatibility and runtime deployment safety are distinct.
- **Security:** registry authn/authz; separate read/register/configure/delete permissions; immutable audit trail; soft deletion and reference safety; quotas, input limits, recursion/depth limits, schema-reference cycle detection, protection against malicious regex and resource-exhaustion; redact sensitive example data.
- **Failure:** registry unavailable, partial metadata commit, ID allocator crash, stale caches, concurrent subject updates, invalid references, malformed payloads, migration conflicts and incompatible catalog changes must have explicit behaviour.

### 17.4 Developer workflows to outperform, not merely match

**Workflow A: Safe schema change**
1. Developer opens a PR changing an Avro/Protobuf/JSON schema.
2. CI resolves referenced schemas and compares against configured subject compatibility and retained history.
3. Output is a **field-by-field diff** with reason codes and reader/writer implications.
4. If incompatible, propose *non-destructive* alternatives and a migration plan; require human approval.
5. Display potentially impacted registered consumers/owners as **known**, **inferred** or **unknown**. Do not pretend all consumers are discoverable.
6. After approval, register atomically and emit an audit event.

**Workflow B: Inspect production data safely**
1. Developer selects topic, partition and bounded offset/time range.
2. Inspector detects only **known** framing; validates schema ID/context; fetches registry version from bounded cache.
3. Shows decoded payload, schema version, compatibility history and meaningful error messages.
4. Read-only by default. Unknown framing, unavailable registry or invalid schema yields a diagnostic, never fabricated decoded values.
5. PII masking and authorization enforced before returning decoded data; replay never advances production cursors.

**Workflow C: Schema-to-Iceberg**
1. User selects topic/subject and target table/catalog.
2. Planner resolves each writer schema, optional/default semantics, logical types and field mapping.
3. Maintain a **durable field identity mapping** across schema versions; renames preserve Iceberg field IDs, new fields receive fresh IDs, and dropped IDs are never reused.
4. Compare planned evolution to **actual Iceberg catalog schema** and supported type promotions; reject ambiguous or lossy conversions by default.
5. Human approves changes before catalog mutation; sink separately handles commit/checkpoint recovery and duplicate risk.
6. Distinguish registry compatibility from table evolution safety; neither implies end-to-end exactly-once delivery.

**Illustrative CLI UX, NOT implemented:**
```sh
serapendiya schema diff --subject orders-value --candidate ./orders.avsc
serapendiya schema check --subject orders-value --candidate ./orders.avsc --history all
serapendiya schema impact --subject orders-value --candidate ./orders.avsc
serapendiya inspect orders --from-offset 100 --limit 20 --decode --redact
serapendiya iceberg plan --topic orders --table analytics.orders --dry-run
```
CLI commands above are design examples; agents must not create them until the gate is authorized.

### 17.5 Required ADRs (future, not G0 tasks)

- **ADR-008: Registry compatibility, identity and persistence.** Supported Confluent REST endpoints and error codes, subject naming strategies, contexts, schema IDs, normalization, references, transactionality, durable ID allocation, backup/restore, migration/import, soft/hard delete, consistency, auth, cache and concurrency semantics.
- **ADR-009: Format-specific validation and schema evolution.** Avro reader/writer resolution, Protobuf descriptors/indexes/reserved fields, JSON Schema dialect/reference resolution, compatibility matrices, normalization, resource bounds and fuzz corpus.
- **ADR-010: Developer contract workflow.** Structured diffs, ownership claims, GitOps/CI, approvals, audit, migration plan generation, redaction and privacy.
- **ADR-011: Iceberg schema mapping and commit coupling.** Stable nested field-ID mapping, type promotion, nullability/defaults, catalog concurrency, schema change approvals, Parquet writing, checkpoint commit, duplicate recovery and retention pinning.
- **ADR-012: Optional broker-side schema enforcement.** Explicit opt-in, fail-open vs fail-closed policy, cache invalidation, impact on latency, availability and compatibility. No default enforcement in R1.

### 17.6 Future implementation gates (separate authorization)

| Gate | Deliverable | Acceptance evidence |
|---|---|---|
| S0 | Research + interviews + competitor UX reproduction | 10–15 interview summaries; reproducible baseline task timings; source/version matrix |
| S1 | External-registry adapter and read-only decode inspector | Known Confluent/Redpanda fixtures decode correctly; unknown formats fail safely; no broker changes |
| S2 | Minimal own registry with Avro and supported API subset | Official Confluent serializer interoperability; ID persistence after crash; concurrent registration; compatibility fixtures; backup/restore |
| S3 | Protobuf + JSON Schema + schema diff and CI | Format-specific golden fixtures; diff explanations; reference cycles; malformed schema fuzzing |
| S4 | Impact graph + governance + migration dry-run | No invented dependencies; permission/audit tests; source/destination ID mapping verified |
| S5 | Iceberg mapping and materialization | Field-ID evolution tests; catalog conflict and retry; restart/replay dedupe tests; Parquet interoperability |

**S0 is mandatory before S1–S5.** Complete the specified research, interviews and competitor baseline before requesting implementation authorization. **S1 is the preferred early developer-value experiment** and can precede building Serapendiya's own registry once R1 is complete. Do not assume users need a replacement registry to value superior inspection and CI.

### 17.7 Research and source register (checked November 2026)

1. Confluent Schema Registry API: https://docs.confluent.io/platform/current/schema-registry/develop/api.html
2. Confluent schema evolution: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
3. Redpanda registry overview and limitations: https://docs.redpanda.com/cloud-data-platform/manage/schema-reg/schema-reg-overview/
4. Apicurio registry overview: https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-intro-to-the-registry.html
5. Apache Iceberg specification: https://iceberg.apache.org/spec/
6. Developer discussion of production breaking changes (qualitative, not representative): https://www.reddit.com/r/ExperiencedDevs/comments/1srtld2/kafka_schema_evolution_breaking_changes_what_do/
7. Independent schema-registry tool comparison (vendor-produced; treat scores as opinion, not independent benchmarks): https://www.factorhouse.io/resources/kafka/tools/kafka-schema-registry-tools/

**Research gaps:** No primary customer interviews yet; no comparative usability experiment; no verified claim of a unique feature; no implementation or benchmark. Any new claims must cite dated product documentation or a reproducible test.


## 18. Complete product capability register and commercial packaging decisions (v12)

**Non-negotiable rule:** The product implementation scope and the commercial distribution model are **orthogonal**. Every capability in this register is in the intended product roadmap. Marking a capability `PAID`, `MANAGED`, or `ENTERPRISE` does **not** remove it from engineering scope, and marking it `OPEN SOURCE` does **not** waive implementation, security, quality or testing requirements. Conversely, inclusion in this roadmap does not authorize an agent to implement it ahead of its gate. There are no paid-only placeholders masquerading as implemented features.

### 18.1 Founder-approved packaging model

The founder is the decision-maker. **Founder decisions are recorded below; individual capability mappings are explicit.** Do not silently default features to paid, open source, cloud-only or enterprise. Record choices in `docs/product/feature-packaging.yaml` **only when the product/packaging ADR is approved and its work is authorized**; the table below is the planning source until then.

Choose one or more delivery/distribution modes **per capability**:
- `OSS`: published source and self-hostable build under a selected, legally reviewed license.
- `PAID_SELF_HOSTED`: commercial license for self-managed deployment; **always paired with `MANAGED`** as an available paid Serapendiya-operated offering. Both delivery modes must be implemented when the capability gate is authorized; can coexist with OSS core but requires a clearly defined boundary.
- `MANAGED`: Serapendiya-operated hosted implementation, with billing, provisioning, operations and service obligations.
- `ENTERPRISE`: commercial security/governance/support capability, deployable self-hosted or managed where technically feasible.

**Founder-wide invariant:** Every `PAID_SELF_HOSTED` feature is also available as a **paid managed service**. This is an availability requirement, not a claim that both offerings must launch simultaneously. Both implementations are on the roadmap, with independently verifiable deployment, isolation and recovery tests. `OSS` features remain open source and may also be used within the managed service without changing their source classification.

**Allowed combinations:** `OSS + MANAGED`, `OSS + ENTERPRISE`, `PAID_SELF_HOSTED + MANAGED`, `OSS + PAID_SELF_HOSTED + MANAGED` (only where license/feature boundaries are legally coherent). `MANAGED` means an actual operated service, not merely a feature flag. For each choice, specify deployment availability, source availability, license, entitlements, API compatibility, migration/export and dependencies. Never assume an OSS license automatically permits closing redistributed modifications; verify obligations with counsel. No deceptive degradation of the open build, hidden data lock-in or undocumented wire-format divergence.

### 18.2 Feature-by-feature decision matrix: founder-approved classifications

All 28 rows have **Build = YES**. The founder-approved categories have been mapped to every row; see notes on overlapping feature boundaries and the managed-cloud classification below. The `Phase` column is implementation sequencing, not exclusion. An OSS implementation may be operated by Serapendiya as a managed offering, and a paid feature may require a server component plus cloud operations.

| ID | Capability (required product roadmap) | Phase / dependency | Build | Packaging choice |
|---|---|---|---|---|
| F01 | Rust-native durable event log, Kafka RecordBatch v2 | R1 / G1 | YES | OSS |
| F02 | Kafka-compatible wire protocol and client interoperability | R1 / G2 | YES | OSS |
| F03 | Broadcast/fanout subscriptions and durable cursors | R1 / G3 | YES | OSS |
| F04 | Work queues, leases, ACK, retry and DLQ | R1 / G3 | YES | OSS |
| F05 | Safe replay, bounded preview and audited redrive | R1 preview G4; later redrive | YES | OSS |
| F06 | CLI, local developer tooling, web console | R1 CLI; later UI | YES | OSS |
| F07 | Metrics, tracing, diagnostics and performance tooling | R1 baseline; R2 expansion | YES | OSS |
| F08 | Kafka consumer groups, transactions, idempotence, compatibility expansion | R2+ | YES | OSS |
| F09 | Replication, leader election, partition placement, failover | R3 | YES | OSS |
| F10 | Per-topic RAM/NVMe/SSD/object-storage tiering and retention | R4 | YES | OSS |
| F11 | Schema Registry REST compatibility and durable schema IDs | R5 / S2 | YES | PAID_SELF_HOSTED + MANAGED |
| F12 | Avro, Protobuf, JSON Schema and schema references/evolution | R5 / S2–S3 | YES | PAID_SELF_HOSTED + MANAGED |
| F13 | Schema diff, CI checks, change impact graph, migration planner | R5 / S3–S4 | YES | PAID_SELF_HOSTED + MANAGED |
| F14 | Schema-aware inspection, historical decode and masking | R5 / S1+ | YES | PAID_SELF_HOSTED + MANAGED |
| F15 | Schema governance, ownership, approvals and audit | R5 / S4 | YES | PAID_SELF_HOSTED + MANAGED |
| F16 | Shared materialisation engine: schema mapping, batching, Parquet, checkpoints | R6 / M1 | YES | PAID_SELF_HOSTED + MANAGED |
| F17 | Apache Iceberg catalog and table materialisation | R6 / M2 | YES | PAID_SELF_HOSTED + MANAGED |
| F18 | DuckLake SQL-catalog and Parquet materialisation | R6 / M3 | YES | PAID_SELF_HOSTED + MANAGED |
| F19 | MotherDuck managed analytics destination | R6 / M4 | YES | PAID_SELF_HOSTED + MANAGED |
| F20 | Local DuckDB query/preview integration | R6 / M0 | YES | PAID_SELF_HOSTED + MANAGED |
| F21 | Pluggable source/sink connectors and SDK | R7 | YES | OSS |
| F22 | WASM transformations, validation and isolation | R7 | YES | OSS |
| F23 | MCP developer tools, read-only first, audited mutations later | R1+ / R7 | YES | OSS |
| F24 | Managed cloud provisioning, multi-tenancy, metering and billing | R8 | YES | PAID_SELF_HOSTED + MANAGED |
| F25 | Enterprise auth/RBAC/SSO, audit, governance and support tooling | R8, foundational security earlier | YES | OSS |
| F26 | Backup, restore, upgrades, DR, monitoring and operational controls | R2–R8 | YES | OSS |
| F27 | Optional schema enforcement at broker edge | R5+ / ADR-012 | YES | PAID_SELF_HOSTED + MANAGED |
| F28 | Destination framework for future Delta Lake, Snowflake, BigQuery etc. | R6+ / discovery gate | YES | PAID_SELF_HOSTED + MANAGED |

**F28 scope:** The adapter framework is committed, but individual unnamed future destinations require explicit product approval before implementation; do not claim every possible destination is already committed. **F08 scope:** specific Kafka protocol versions and transactional semantics require their own acceptance matrices. The registry and analytical integrations are real engineering deliverables even if ultimately monetized.

### 18.3 Decision workflow and acceptance

1. Founder classifications in Section 18.2 are the source of truth. Clarify only explicitly flagged ambiguities; pricing and licensing terms remain unselected and **must not block unrelated technical work**.
2. Create `ADR-013 Commercial Packaging and License Boundaries` for reviewed choices. Include copyright ownership, third-party license compatibility, self-hosted build reproducibility, entitlement boundary, managed deployment design, upgrade/migration path and support commitments.
3. Engineers implement a feature against the **same domain correctness and interoperability contracts** regardless of packaging. Deployment adapters and entitlements may differ, but core event/schema/table semantics must not fork silently.
4. Test every supported distribution: OSS source build where applicable; licensed self-hosted deployment where applicable; managed staging/provisioning, tenancy, metering, failure isolation, backup/restore and data export where applicable.
5. Packaging decisions may change later through an ADR and documented migration path. Do not silently withdraw previously promised source availability or customer data access.

**Founder approval:** The 12 category selections supplied on 6 November 2026 are authoritative. They are expanded to all 28 feature IDs in Section 18.2. These are product/distribution decisions, **not** an approved license text, pricing schedule, entitlement implementation or permission to begin work outside G0.

**Founder clarification resolved:** Every paid self-hosted capability must also be offered in Serapendiya-operated paid managed form. F24 therefore explicitly includes both commercially licensed customer-operated cloud deployment software and a Serapendiya-operated managed cloud service. This is a product obligation, not permission to start R8 during G0. Managed offerings require tenancy, security, operational SLOs, billing/metering, backups, support, data portability and failure-isolation acceptance tests before launch.

**Shared functionality boundary:** F14 (schema-aware decoding) is classified `PAID_SELF_HOSTED` because it belongs to the selected schema category, while F05/F06 (generic raw event replay and inspection) remain `OSS`. F20 (local DuckDB query/preview) is classified paid as part of the analytics integration family, not the generic OSS CLI. F25 (enterprise auth, RBAC, SSO and administration) is `OSS` as explicitly requested, even if this differs from common open-core monetisation. F27 (optional schema enforcement) is paid as part of schema management. F28 (destination framework) is paid, but F21 (general connector SDK) is OSS; the adapter boundary must be specified in ADR-013 before any packaging code. No artificial protocol incompatibility or duplication of core semantics is permitted.

## 19. Multi-destination analytics: Iceberg, DuckLake, MotherDuck and DuckDB

**Status:** Full planned product capability, future implementation only. This section **replaces any reading of Section 17 that implies Iceberg is the sole destination**. It extends the schema platform and does not weaken the current G0 authorization. **Table formats, catalogs, execution engines and hosted analytics services are different categories**:

| System | Correct role | Adapter responsibility |
|---|---|---|
| Apache Iceberg | Open table format, snapshots, schema and partition evolution, catalogs | Catalog-aware snapshot/table commits and schema mapping |
| DuckLake | Open lakehouse format with SQL catalog metadata and Parquet object data | Transactional SQL catalog commits, file registration, schema evolution and recovery |
| DuckDB | Embedded SQL analytical engine | Local inspection/query and test execution; not a substitute for a durable remote destination |
| MotherDuck | Managed DuckDB-based analytics platform, with evolving DuckLake capabilities | Supported MotherDuck connection/auth, destination-specific loading and validation; assess supported DuckLake deployment modes at implementation time |

**Do not assume MotherDuck is merely DuckLake with a different connection string.** Check current product/API/feature availability and supported write semantics before selecting the integration method. MotherDuck can be a distinct destination even where a DuckLake-compatible path exists.

### 19.1 Architecture: one materialisation core, separate commit adapters

```text
Serapendiya immutable source log (offsets + durable certified watermark)
  -> bounded source reader / retention pin
  -> optional schema resolution (registry IDs, writer schema, references)
  -> typed mapping + per-destination evolution plan (explicit errors)
  -> batches + optional Parquet writer + staging manifest
  -> durable materialisation job journal / checkpoint
  -> destination adapter:
       Iceberg: catalog snapshot commit + Parquet objects
       DuckLake: SQL metadata transaction + Parquet objects
       MotherDuck: supported ingestion/load API or verified DuckLake route
       DuckDB local: local query/preview or explicitly configured persisted output
  -> reconcile commit result and advance source checkpoint
```

**Core invariant:** Source offsets, schema version/fingerprint, destination table identity, materialisation job ID, staged file/object identities and commit status must be recoverable together. A destination commit and source checkpoint are **not** a distributed atomic transaction by default. Design for unknown-commit outcomes, idempotent retries or reconciliation, and document when at-least-once delivery can create duplicates. Never claim end-to-end exactly-once without a proven destination-specific protocol and failure-injection evidence.

**Separation:** S3 broker log tiering is not an Iceberg/DuckLake data file store; schema registry IDs are not Iceberg field IDs; a MotherDuck managed account is not a generic object-storage catalog. No connector may change Kafka log offsets or mutate queue ACK state as a side effect of query/preview.

### 19.2 Required contracts and tests

- **Common sink job:** stable `job_id`, source topic/partition/offset range, writer schema identity, mapping revision, destination identity, retry policy, staged file manifest, commit token/transaction marker where supported, status, and last reconciled source checkpoint.
- **Schema evolution:** compare source schema against actual destination table metadata; stable nested field identity; nullability, defaults, logical types, decimals, timestamps, deletes, renames, type promotion and unsupported conversions. Fail closed on lossy/ambiguous mappings unless explicitly approved.
- **Security:** destination-scoped credentials; short-lived tokens where supported; encryption, least privilege, secret redaction, object ownership, audit logs, tenant isolation, safe egress controls and explicit retention.
- **Observability:** per-destination lag, bytes/rows committed, staging backlog, commit retries, unknown outcomes, schema errors, catalog conflicts, cost estimates and storage footprint.
- **Recovery tests:** process killed before/after data upload, catalog commit, checkpoint fsync and response; destination timeout after successful commit; schema changed concurrently; expired credentials; object upload partial; duplicate replay; orphan cleanup.
- **Compatibility tests:** independent readers can query produced tables/files; verify field IDs and table metadata after schema evolution; no self-referential tests only.

### 19.3 Destination implementation gates (all committed, not yet authorized)

| Gate | Scope | Exit evidence |
|---|---|---|
| M0 | DuckDB local analytical preview | Bounded read-only queries over known records/Parquet, no production cursor changes |
| M1 | Shared durable materialisation engine | Crash/restart tests, staged manifest and checkpoint journal, unknown-commit simulation |
| M2 | Iceberg sink | Real catalog + independent reader, schema evolution, snapshot conflict and retry tests |
| M3 | DuckLake sink | Real SQL catalog + Parquet reader, transaction conflict and recovery tests |
| M4 | MotherDuck sink | Supported auth/connectivity, real managed destination, restart/duplicate/reconciliation tests; no assumption that DuckLake support is universal |
| M5 | Extensible connector SDK | External adapter conformance suite, resource quotas, versioned contracts and safe upgrades |

**Gate ordering:** M0 can be prototyped independently after R1 if approved; M1 precedes production M2–M4. M2 and M3 can be prioritized by customer evidence, but both remain committed. M4 requires current MotherDuck API verification. No gate creates future scaffolding in G0/G1.

### 19.4 Additional architecture decisions

- **ADR-014:** Shared materialisation job protocol, durability, offset retention pins, commit reconciliation and idempotency.
- **ADR-015:** DuckLake SQL catalog, storage layout, metadata transaction, schema evolution and recovery.
- **ADR-016:** MotherDuck connectivity, auth, API support, destination ingestion contract and operational limits.
- **ADR-017:** Destination adapter contract and capability negotiation; per-adapter compatibility matrix.
- **ADR-011 (existing):** Iceberg mapping and commit coupling; amend to align with ADR-014 without conflating catalog semantics.

### 19.5 Reference documents to verify before implementation

- Apache Iceberg specification: https://iceberg.apache.org/spec/
- DuckLake documentation and specification: https://ducklake.select/docs/stable/ducklake/overview
- DuckDB DuckLake extension: https://duckdb.org/docs/stable/core_extensions/ducklake/overview
- MotherDuck documentation: https://motherduck.com/docs/

All external capability claims, APIs, pricing and compatibility are subject to version-specific verification during the relevant gate. This plan is an architecture/design commitment, **not** a statement that integration has been implemented or tested.

## 20. Agent handoff and precedence for v13

- **Only G0 is currently authorized.** This remains true even though the complete feature set is now in scope for the product roadmap.
- Treat Sections 17–19 as product requirements for future gates, not implementation instructions for G0. Do not create any schema registry, analytics adapter, managed-service, licensing or billing code during G0.
- The founder selects commercial packaging; every paid self-hosted capability also has a paid managed delivery mode; agents must not choose OSS/paid/managed classifications or licenses on their behalf.
- Before starting each future capability, require approved ADRs, a versioned compatibility matrix, a small executable test plan, a human gate authorization and PR evidence.
- **Do not remove features because they may be commercial.** Implementation scope and distribution/monetization remain independent.

## 21. v10 revision record (historical)

- Added explicit founder-controlled OSS / paid self-hosted / managed / enterprise packaging matrix for 28 capabilities, all included in implementation scope.
- Added DuckDB, DuckLake and MotherDuck to Iceberg through a shared materialisation architecture with destination-specific recovery and schema evolution.
- Added ADR-013 through ADR-017 and destination gates M0–M5.
- Preserved strict G0-only authorization, source-of-truth verification and failure-first coding-agent rules.


## 22. v11 founder commercial decision record (6 November 2026)

**Classification groups, preserved exactly as selected:**

| Founder-selected category | Approved mode | Mapped IDs |
|---|---|---|
| Core broker, Kafka compatibility and subscriptions | Open source | F01, F02, F03, F04, F08 |
| CLI, console, replay and debugging | Open source | F05, F06, F07 |
| Schema Registry and schema evolution | Paid self-hosted + paid managed | F11, F12, F14, F27 |
| Schema governance and migration intelligence | Paid self-hosted + paid managed | F13, F15 |
| Apache Iceberg integration | Paid self-hosted + paid managed | F17 |
| DuckLake integration | Paid self-hosted + paid managed | F18 |
| MotherDuck integration | Paid self-hosted + paid managed | F19 |
| WASM transformations and connectors | Open source | F21, F22 |
| MCP and AI developer tools | Open source | F23 |
| Replication and object-storage tiering | Open source | F09, F10 |
| Enterprise security and administration | Open source | F25, F26 |
| Managed cloud platform | Paid self-hosted + paid managed (founder clarified) | F24 |
| Shared analytics engine and DuckDB preview (mapped by closest selected category) | Paid self-hosted + paid managed | F16, F20, F28 |

**No scope deletion:** F01–F28 are all retained, including the paid features and managed-cloud operations. **No execution scope expansion:** G0 is the only authorized coding gate. **Commercial boundary gate:** ADR-013 must be proposed and explicitly accepted by the founder before any entitlement logic, licensing checks, dual-distribution builds, managed-service scaffolding or SaaS packaging. Do not implement packaging in G0 or G1. Source publication also requires a separately approved licensing and provenance review. In particular, do not create a paid license gate during G0.

**Revision note:** v12 extends v11 and records the founder's 12 commercial classifications, maps them across all 28 roadmap features, distinguishes open-source enterprise security from paid schema/analytics, and flags the managed-cloud/self-hosted ambiguity without overriding the founder's choice.


## 23. v12 commercial delivery clarification (founder approved)

- All F01–F28 remain mandatory roadmap capabilities; only G0 is currently authorized for implementation.
- Every capability labeled `PAID_SELF_HOSTED + MANAGED` has two required commercial delivery forms: customer-operated licensed deployment and Serapendiya-operated paid service. No paid self-hosted-only exception.
- The paid managed form must expose equivalent documented core functionality and compatibility, allowing for operational differences; do not implement a separate, semantically divergent product.
- Open-source capabilities remain OSS and may be used in the paid managed platform without becoming proprietary.
- Licensing, exact price points, service-level commitments and launch sequencing are separate decisions under ADR-013.
- Verify self-hosted install/upgrade/backup/restore and managed provisioning/tenant isolation/metering/recovery as separate acceptance suites.


## 24. v13 review incorporation and execution-document precedence

**Founder scope freeze:** All F01–F28 capabilities remain committed to the product roadmap, with founder-approved commercial classifications and the rule that each paid self-hosted capability is also available as a paid managed offering. No future feature is authorised merely by being documented here.

**Execution separation:** Use `Serapendiya_G0_Execution_Contract_v13.md` as the only document supplied to a coding agent for G0. This master blueprint is for product and architecture reference. If the agent also has this blueprint, it must treat Sections 17–19, and all commercial material in Sections 18 and 20–23, as future requirements only. No schema, Iceberg, DuckLake, MotherDuck, packaging, entitlements, replication, tiering, WASM or MCP files, traits, stubs, configuration fields or tests may be created in G0.

**Schema discovery dependency:** S0 research is mandatory before any S1–S5 implementation, including interviews and reproducible competitor workflow comparisons. The founder must approve S0 findings and explicitly authorise each next gate.

**Commercial implementation dependency:** ADR-013 must be accepted before any licence checks, entitlements, dual distributions, managed-service scaffolding or packaging code. Founder packaging decisions alone do not authorise these changes.

**G0 closure:** The agent must submit per-PR `docs/verification/PRxx.md` files with exact commands, outputs, exit codes, commit SHA, toolchain and lockfile hash; a gate completion report; ADR-001 through ADR-004 drafts and approval states; and a list of blocked/unverified items. A completed G0 does not authorise G1. G1 requires a new founder decision and accepted ADR-003 and ADR-004.

**v13 changes:** Converted the G0 handoff into a short standalone contract, reinforced S0 and ADR-013 gates, clarified agent-document precedence, corrected historical handoff reference and retained all 28 product capabilities and founder packaging choices.
