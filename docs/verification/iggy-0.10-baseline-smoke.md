# Iggy 0.10 baseline smoke and initial source trace

Date: 2026-11-15. Scope: one synthetic clean-restart smoke test and a partial
source trace. This is not crash, power-loss, corruption, performance,
cross-platform, client-compatibility or Serapendiya durability evidence.

## Environment and source

- Apache Iggy `server-0.10.0`, annotated tag object
  `d7369a6b444db6955d16fd766006651922250c46`, target commit
  `91e0206367aad2d30259f7d45203832b08ae84e7`.
- macOS Darwin arm64, `rustc 1.99.1` and `cargo 1.99.1`; debug builds;
  `pkgconf 3.0.10` and `hwloc 2.15.0` installed to satisfy build prerequisites.
  Temporary source and data directories; one unpinned shard. TCP and other
  enabled listeners bound to localhost; HTTP disabled; no customer data.
- Test topic had one partition, no compression, message durability
  `persisted` and consumer-offset durability `persisted` explicitly selected.
  The test used development credentials only.

## Commands and observed results

The initial `cargo build --locked --offline -p server --bin iggy-server`
failed: the local Cargo index lacked `cucumber`. A sandboxed online retry could
not resolve crates.io and was interrupted. With network access, the first build
failed because `pkg-config`/`hwloc` were unavailable. After installing those
prerequisites, in the pinned checkout:

```text
cargo build --locked -p server --bin iggy-server   # exit 0
cargo build --locked -p iggy-cli --bin iggy        # exit 0
```

The server was started with `IGGY_PATH` pointing to a temporary directory,
`IGGY_HTTP_ENABLED=false`, `IGGY_TCP_ADDRESS=127.0.0.1:28090`,
`IGGY_SHARDING_CPU_ALLOCATION=1`, `IGGY_SHARDING_PIN_CORES=false`, and
`--with-default-root-credentials`. A prior default `numa:auto` startup produced
zero shards; an initial sandboxed attempt with one shard could not bind a
loopback socket. The successful local listener and CLI operations ran with the
necessary socket permissions. Neither failure establishes a server defect.

Using the matching CLI against that TCP address and the development account:

```text
iggy stream create feasibility                                      # exit 0
iggy topic create feasibility sample 1 none --durability persisted \
  --consumer-offset-durability persisted                           # exit 0
iggy message send feasibility sample baseline-synthetic-1           # exit 0
iggy message poll --offset 0 feasibility sample 0                   # exit 0
iggy consumer-offset set 0 feasibility sample 0 0                   # exit 0
```

The first poll returned `baseline-synthetic-1` at offset 0. After a clean
Ctrl-C shutdown (exit 0) and restart against the same data directory, the
message was again read at offset 0. The test then sent
`baseline-synthetic-2`, explicitly set consumer 0's stored offset to 1,
cleanly shut down (exit 0), and restarted again. Polling offset 1 returned the
second payload; `iggy consumer-offset get 0 feasibility sample 0` returned
stored offset 1. Both commands exited 0. The server was then cleanly stopped.
No write or offset failure was injected.

## Initial code trace, incomplete

- The existing Serapendiya append path in `src/storage.rs` calls segment sync
  before replacement-checkpoint publication. ADR-024's journal sequence is
  proposed, not accepted. Neither is verified against Iggy by this smoke test.
- Iggy's pinned [partition recovery source](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/segment_recovery.rs)
  describes log/index recovery, refusals and durable-segment evidence. Its
  [partition initialization](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/server/src/partition_helpers.rs)
  checks a frontier for an existing singleton. A separate
  [partition journal](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs)
  defines two frontier slots, while a distinct
  [superblock](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/superblock.rs)
  handles VSR state. These observations alone do not establish which structure
  certifies each acknowledged singleton message or how every crash cut resolves.
- The upstream [durability guide](https://iggy.apache.org/docs/server/durability/)
  describes the singleton `persisted` log/index sync and new-name sync before
  reply and says a singleton does not use the cluster prepare WAL. Trace the
  relationship to the singleton frontier checks in the actual code; do not
  assume they represent the same write path or resolve the question by naming.
- The pinned [sink runtime](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/sink.rs)
  configures `CommitPolicy::OnRead`; its consumption code describes the poll
  committing the batch before plugin processing. This is source inspection,
  not a reproduced connector crash test.

Remaining: full path-by-path acknowledgment authority trace; Iggy versus
Serapendiya failure-state comparison; deterministic injection, independent
review, process crash and separately authorized block/VM replay; connector
failure reproduction; and matched measurements. Do not infer safety or select
a foundation from this single clean-restart observation.
