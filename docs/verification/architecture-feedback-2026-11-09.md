# Review of 2026-11-09 architecture feedback

Status: historical engineering assessment, not an accepted ADR or permission
to implement future gates. The storage-container experiment was subsequently
merged; its format remains experimental and G1 acceptance remains open.

## Findings for current G1 work

- Kafka RecordBatch v2 CRC32C starts at `attributes`; base offset, batch length
  and partition leader epoch are outside it. Changing the base offset does not
  require recomputing the Kafka CRC, though any Serapendiya checksum covering
  the whole payload or header may need recomputation. Source:
  https://kafka.apache.org/44/implementation/message-format/
- The experimental container currently computes a CRC over the full Kafka
  batch and constructs/decodes one narrow record shape. It is not a verified
  opaque Kafka batch ingestion or zero-copy fetch path. Whether to retain the
  extra full-batch checksum, and which record validation requires decoding, must
  be measured and reviewed in ADR-003; do not claim Kafka compatibility.
- Strict mode must not expose a visible offset beyond its durable offset.
  The experimental implementation moves visibility only after the checkpoint
  succeeds; an injected failure test checks the invariant.
- SIGKILL/abort tests process recovery, not power loss: the host page cache can
  survive. Experimental tests include a small deterministic model of selected
  stable/volatile write and rename outcomes. It is not a full simulated
  filesystem, randomized reference model or proof. Extend it with reproducible
  seeds and a reference model. Validate simulator assumptions on
  real fault-injection facilities and on the supported ext4/XFS matrix. VOPR
  is a relevant example, not proof of our implementation:
  https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md
- Current successful group commit performs one segment sync, one checkpoint
  file sync, one rename and one parent-directory sync. No latency or device
  flush count has been benchmarked; do not equate syscall counts to physical
  flushes. Linux says file fsync does not itself ensure the directory entry is
  durable: https://man7.org/linux/man-pages/man2/fsync.2.html
- Compare the existing certified-checkpoint protocol against a fully specified
  WAL and recovery alternative. A checksum chain, a blank/garbage tail or lack
  of later valid entries cannot by themselves distinguish a torn unacknowledged
  append from corruption of an acknowledged final entry. Do not truncate an
  ambiguous tail or demote the checkpoint without a proven recovery/ACK rule,
  defined failure model, crash tests and an approved ADR-004 amendment.
- Successful Linux `fallocate` protects against ENOSPC for writes in the
  allocated range under its documented conditions. It does not eliminate EIO,
  metadata, checkpoint or directory failures; it does not itself establish
  that fdatasync is faster. Treat preallocation as a measured option:
  https://man7.org/linux/man-pages/man2/fallocate.2.html
- A generic filesystem backend is a reasonable performance/testing option;
  decide after API, compile-time and benchmark review. The existing dyn seam
  is used per I/O operation, not automatically per record, and has not been
  shown to be a bottleneck.

## Future proposals, not authorization

- ADR-006 should compare a replicated internal queue-state log with separate
  journals, including atomic ordering against the source log and recovery,
  replication, snapshotting and retention. Sharing a pipeline alone does not
  make writes across separate logs or partitions atomic.
- Evaluate volatile claims for at-least-once delivery. Define leadership/boot
  epochs, fencing across failover, retry counts, deadlines, concurrent owners,
  stale ACKs, and recovery before deciding claims never need persistence.
  Deriving tokens with an HMAC still requires durable key management, rotation,
  authentication and fencing; it is not a substitute for those properties.
- Specify ACK low watermark plus a bounded sparse set and its backpressure,
  persistence, replay and retention behavior. Evaluate dead state referencing
  a source offset; retention pins and disk-exhaustion policies still apply.
- Evaluate per-topic durability modes with explicit acknowledged-loss windows,
  visibility and recovery rules. Cross-partition batching does not collapse
  separate file syncs into one automatically. No weaker mode is authorized.
- Revisit keyed compaction, retention with bounded pins, and multi-partition
  ordering against product evidence and separate authorization.
- Kafka Java producer idempotence defaults on since 3.0 (subject to conflicting
  settings). Its PID/epoch/sequence, fencing and restart behavior need a design
  and tests; do not call it trivial. Manual assignment remains the planned
  narrow initial consumer scope. Source:
  https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
- Specify fetch wait/minimum bytes, session/version negotiation, nontransactional
  high-watermark/last-stable-offset semantics and advertised listeners in the
  G2 compatibility matrix, tested against selected real client versions:
  https://kafka.apache.org/44/design/protocol/
- Evaluate registry adapters and DuckDB-backed materialization before later
  implementation, without removing committed roadmap capabilities or adding
  code now. Verify current APIs, license, recovery and data correctness first.

## Document reconciliation

The authoritative authorization table is in Section 0 of the local v13
blueprint. ADR-008–010 collisions in Section 12D were resolved by allocating
retention, replication and S3 to ADR-020–022; registry keeps ADR-008–010.
Both references to ADR-011 concern Iceberg and should remain one coordinated
decision. Release labels follow Section 1: R5 schema, R6 analytics, R7 WASM.
Kafka `kcat` smoke coverage belongs to G2 and must pass before an R1 release.
Historical v13-only authorization statements are historical, not current.

## Second review: concurrency, I/O and future replication

- Tokio `LocalSet` confines its local tasks to one thread but does not by
  itself pin that thread to a CPU. ADR-005 should compare partition ownership,
  dedicated shard threads and a work-stealing baseline, including CPU affinity,
  overload, blocking workers, rebalancing and replayable tests. No runtime or
  pinning scheme is selected. Source:
  https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html
- Compare buffered I/O, optional preallocation and direct I/O with representative
  workloads before selecting a mechanism. A particular approach does not
  guarantee better tail latency without measurements.
- A stored Kafka batch may permit reduced conversion on fetch, but interleaved
  container headers, request framing, offset rewriting, filtering, compression
  and TLS affect whether a particular path can use sendfile. The experimental
  code currently constructs and decodes one record and copies batches. It does
  not implement zero-copy. Preserve the option where practical; do not promise
  it without an end-to-end implementation and measurements.
- ADR-003 and a separate replication ADR must analyze leader epochs, fencing,
  divergent suffix truncation, snapshots, checksums and format evolution
  together. Merely reserving a field does not provide safe replication or
  eliminate future migrations. No replication code is authorized.
- Design segment rolling, active/sealed/compacted lifecycle, timestamp/offset
  indexes and retention together. Compaction can legitimately leave offset gaps;
  the current contiguous scan is valid only for the present noncompacted slice.
  Do not change gap acceptance before a format and recovery design is accepted.
- Internal queue and group logs can share storage infrastructure, but they
  still need explicit atomicity, ordering, retention, fencing and replay rules.
  Kafka documents `__share_group_state`. Source:
  https://kafka.apache.org/42/getting-started/upgrade/
- Specify bounded admission before allocation: global and per-shard byte and
  request budgets, large-frame rejection, ownership across asynchronous I/O,
  cancellation, fairness and recovery. Avoid promising a particular pool or
  hardware topology before measuring and proving lifecycle correctness.
- Online format upgrades, mixed-version reads, rolling restarts and rollback
  require explicit compatibility and migration tests; version fields alone do
  not provide them.
- Claims that Rust is inherently faster or memory-safe everywhere, or that our
  failure guarantees or performance exceed other systems, are unverified. Rust
  code may use unsafe blocks, have logical errors, allocate without bound or
  deadlock. Current code
  includes an unsafe OS lock call. Any performance claim requires comparable
  benchmark conditions and raw results.

## Experimental Kafka fixture

On 2026-11-09, kafka-python 3.0.13 independently generated the single-record
fixture exercised by `matches_kafka_python_3_0_13_batch_fixture` using:

```sh
PYTHONPATH=/tmp/serapendiya-kafka-check python3 -c 'from kafka.record.default_records import DefaultRecordBatchBuilder; b=DefaultRecordBatchBuilder(2,0,False,-1,-1,-1,1024); print(b.append(0,0,None,b"one",[])); print(bytes(b.build()).hex())'
```

Exit 0. The generated hex fixture is embedded in the test. A second fixture uses
an 80-byte value and multibyte lengths. They check a narrow uncompressed batch
shape only; they do not demonstrate general Kafka protocol
compatibility, client interoperability, or all record field handling.
