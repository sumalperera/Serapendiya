# ADR-024: segment-count and metadata alternatives

Status: **JOURNAL DIRECTION SELECTED FOR DESIGN; no journal format, retention
policy or implementation accepted.**
Requested 2026-11-13 after PR #33. This compares the candidate in
[the segmentation review](ADR-024-segmentation-review.md) with alternatives;
its crash guarantees and performance have not been demonstrated by an
implementation. Preserve sync-before-ACK, absolute offsets, immutable sealed
segments, full certified-payload verification on startup, explicit offline
migration and refusal/preservation of ambiguous artifacts. Automatic deletion,
deferred verification and coordinator changes remain separate decisions.

## Reference context

Inspected 2026-11-13. These are narrow design studies, not upstream audits or
authorization to copy code. [Kafka LogSegment at 0e675429](https://github.com/apache/kafka/blob/0e675429a2d15605322ac6a8e3077433cbe15732/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java)
separates segment data and derived indexes; its recovery/replication assumptions
do not establish a single-node certified-range contract. [Redpanda segment
appender at 4254992f](https://github.com/redpanda-data/redpanda/blob/4254992f6115e92c1dbd86f90bb0aee0b869f15e/src/v/storage/segment_appender.cc)
is relevant to buffered data ownership; [Redpanda 26.1 topic defaults](https://docs.redpanda.com/streaming/26.1/manage/kubernetes/k-manage-topics/)
document a 128 MiB segment target and time-based cleanup. They do not prove
that its metadata protocol is equivalent to this proposal. [ReductStore block
manager at 8cb50097](https://github.com/reductstore/reductstore/blob/8cb50097dd09c5a89dcc1d1f8508ace6c2bbb4d0/reductstore/src/storage/block_manager.rs)
separates lifecycle and metadata but uses a different workload; [Liftbridge
segment at af2a6626](https://github.com/liftbridge-io/liftbridge/blob/af2a662695ce67c099e56d24444afb15bd8df232/server/commitlog/segment.go)
offers segment lifecycle examples but not this single-node ACK guarantee.
[RocksDB's MANIFEST documentation](https://github.com/facebook/rocksdb/wiki/MANIFEST)
(wiki revision dated 2022-01-27) describes a log of state edits, snapshots and a
CURRENT pointer. Its design motivates studying a journal; it does not establish
the ACK, rollover or recovery safety of a journal integrated here. No new
dependency or upstream code reuse is proposed.

## Options

All capacities below are approximate encoded bytes at the target, per partition,
not hard byte limits: whole groups can overshoot, an active segment can be
empty, framing uses space and changing a target affects future rolls only. None
of the proposals bounds disk use or full-history verification without a separate
retention/compaction protocol. The table omits filesystem write amplification.

| Option | Nominal scale and per-commit authority write | Correctness and operational trade-off |
| --- | --- | --- |
| **A: 4,096 entries, 128 MiB** | ~512 GiB; up to 98,356 bytes | One bounded replacement record and one authority to validate. Highest familiarity with the existing checkpoint protocol; cap eventually blocks a needed roll. Full startup verification remains O(history). Cost is unmeasured. |
| **B: 16,384 entries, 128 MiB** | ~2 TiB; up to 393,268 bytes | Same shape, four times the maximum metadata, file-count and parsing/index budget. Needs new exact count/length bounds, oversized and malformed fixtures, memory and matched sync-latency tests. Still stops at a finite cap. No measured basis for calling it better. |
| **C: 4,096 entries, 1 GiB** | ~4 TiB; up to 98,356 bytes | Same manifest cap and protocol, fewer entries per nominal history. Larger segments change rollover timing, recovery and deletion granularity; one group can overshoot. Full verification and disk growth remain. 1 GiB is already the proposed maximum selectable target, but the default is a separate choice. No current workload evidence favors it. |
| **D: immutable topology plus small active checkpoint** | Per-commit checkpoint can be O(1); topology publication still grows with segments unless changed again | Checkpoint must bind to a durably published topology identity. Specify every old/new checkpoint-topology pair, rollover publication order, missing files, partial rename, orphan handling and version lifetime. The cross-file consistency proof and recovery matrix do not yet exist; a checksum or generation alone cannot provide freshness. |
| **E: append-only metadata journal and snapshot** | Incremental commit records could avoid O(N) metadata rewriting; replay/snapshot costs depend on history | Define record framing, bounded parsing, certified journal tail, torn writes, data-before-metadata order, fsync/ACK semantics, journal rotation/snapshot and atomic authority selection. A second journal/checkpoint can reintroduce cross-file atomicity; no unbounded replay or automatic cleanup may be assumed. RocksDB is inspiration, not a drop-in protocol. |
| **F: safe retention** | Keeps an explicitly bounded working history when policies and workload allow | Directly addresses persistent history and segment turnover, but deletes acknowledged data by policy. Requires separately accepted retention semantics, subscriber/queue/replay and replication pins, stale readers, crash-safe metadata publication/deletion and backpressure when pinned. Not authorized by segmentation. |
| **G: infer segments from filenames** | No complete segment-list rewrite | Reject for this stage. Names do not prove certification, and a synced orphan must not be silently adopted. A separate durable authority would still be required to distinguish acknowledged data and detect missing certified files. |

Changing the cap is a persistent-format bounds decision, even if the entry bytes
do not change. It needs an explicit reader/writer compatibility and migration
decision; a new reader cannot assume an old reader recognizes the expanded
format. A 128 MiB default is based on Redpanda's documented development
default, not a Serapendiya throughput or retention requirement. The target range
and cap have not been validated against a customer workload.

## Recommendation and decision boundary

On 2026-11-13 the founder selected **option E, an append-only metadata journal**
as the direction to develop, with an explicit decision point to revisit it later.
This supersedes the earlier recommendation to develop option A as the first
candidate. It is a planning choice, not evidence that the journal is faster or
acceptance of any persistent bytes, journal protocol, recovery rules or
implementation. The manifest proposal remains a comparison baseline, not the
selected format. Its 4,096-entry bound is not automatically a journal bound;
the journal needs explicit independent limits and resource budgets.

Prepare a journal decision packet with exact records, framing, checksum
coverage, authority selection, data/metadata sync and ACK order, bounded replay,
journal rollover and snapshot publication, initialization, corruption and
orphan handling, legacy compatibility and offline migration. Preserve full
startup payload verification and fail-closed certified-range handling. Obtain
explicit human acceptance of the concrete persistent format and recovery rules
before implementing them. Neither this directional choice nor merging the
proposal accepts retention, a coordinator, deferred verification or G1.
The [journal decision packet](ADR-024-metadata-journal-decision-packet.md) is
in preparation; its rollover, cleanup, migration and independent fixture
verification remain unresolved.

**Revisit decision:** after an authorized implementation and matched-durability
measurements, compare journal versus the single-manifest and/or small-checkpoint
baseline at representative segment counts. Evaluate p50/p99/p99.9, throughput,
sync counts, bytes written, RSS, full-scan restart and journal rotation tails,
alongside deterministic, process-crash and block/VM replay results. Reopen the
choice if recovery complexity, bounded replay, memory, latency or throughput is
unacceptable; the founder has not specified numerical thresholds, which remain
to be decided with workload evidence. Do not call the journal fastest before
matched results. For sustained-use pilots, safe retention remains a separately
authorized decision; no metadata authority alone prevents disk exhaustion.

### Additional journal and checkpoint research

Inspected 2026-11-13. [RocksDB's WAL tracking design](https://github.com/facebook/rocksdb/wiki/Track-WAL-in-MANIFEST)
explicitly considered recording every synced WAL boundary in MANIFEST and warned
that doing so can slow writes and create many records; its documented choice
tracks other WAL state with different coverage for the active WAL. Its WAL and
MANIFEST serve distinct purposes, and the documented tradeoff must not be
assumed to preserve this project's fail-closed certified active range. The
[LevelDB implementation notes](https://github.com/google/leveldb/blob/main/doc/impl.md#manifest)
describe append-form metadata changes and a CURRENT pointer, but its table/WAL
durability contract is not our partition ACK contract. [Kafka 4.0 operational
guidance](https://kafka.apache.org/40/operations/hardware-and-os/) describes a
different default durability approach relying on replicas and OS flushing; its
timings cannot be compared directly to a single-node sync-before-ACK design.
The previously pinned Redpanda segment appender study demonstrates flush and
in-flight write ownership, not a documented equivalent per-commit metadata
authority or measured comparison. ReductStore and Liftbridge studies above
likewise do not establish a matched comparison of these two options.

The earlier suggestion that a journal is necessarily the fastest was too
strong. A journal may avoid rewriting O(segment count) bytes and repeated
directory publication in steady state, but a design preserving this contract
may still require both segment-data and authority syncs; log rotation, replay,
tail integrity and snapshot publication add costs. A small checkpoint may also
be competitive, but safely binding it to the correct durable topology remains
unspecified. Even the relative rank is unknown until measured with equivalent
durability; neither reducing metadata bytes nor success in another system is a
latency result. Preserve full payload verification and unknown outcomes for all
comparisons. The journal direction is selected for design, but no concrete
journal format or implementation is accepted here.

Kafka client compatibility is an independently scoped wire-protocol and
behavioral requirement. Either internal authority could in principle support a
compatible broker; neither format alone supplies the Kafka API or makes the
current repository compatible. Any future lakehouse connector similarly needs
its own cursor, schema and destination commit/retry contract.

Before accepting any choice, the founder must explicitly accept or revise its
unmeasured operating envelope and its unavailable-at-cap behavior. Acceptance
of this comparison or a PR merge does not accept a format or authorize coding.

## Validation if a candidate is authorized

- Before coding: exact authority bytes and bounds; complete initialization,
  commit, roll, interrupted-publication and orphan tables; independent fixtures;
  compatibility and offline migration; review the executable classification
  oracle. Any multi-file or journal alternative needs its own atomicity proof.
- After implementation: deterministic syscall faults and process-crash tests,
  plus independently reported ext4 and XFS block/VM power-loss replay. No ACK
  may be lost, no uncertified bytes returned and ambiguous evidence is preserved.
- Compare under matched sync-before-ACK durability, filesystem, hardware,
  compiler, payloads and group sizes at 1, 256, 4,096 and (if applicable) 16,384
  segments. Record raw samples, sync counts, manifest bytes and time, throughput,
  p50/p99/p99.9 where sample counts support them, CPU, RSS, rollover and full
  startup verification. Report infeasible runs and bounds rather than
  extrapolating. No candidate measurements exist today.
