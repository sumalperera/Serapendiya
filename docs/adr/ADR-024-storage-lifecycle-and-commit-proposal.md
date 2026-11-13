# ADR-024 proposal: storage lifecycle and commit optimization

Status: **PROPOSED; persistent-format, verification-policy and coordinator
decisions require human acceptance before implementation.** The founder requested
all findings from the 2026-11-11 storage review be addressed. This packet makes
the remaining architectural changes reviewable; it does not self-approve them.

## Existing contracts and immediate fixes

Retain certified-range integrity, terminal write failures, unknown outcomes at
the commit boundary and explicit operator recovery. Fix detected-corruption
health handling, temporary encoding buffers, duplicate parsing and sparse-index
read amplification without changing stored bytes. Use the already reviewed CRC
library and rustix lock wrapper. These repairs do not require this new format.

## Reference assessment and selection

Research from the project conversation on 2026-11-11 remains applicable.
Sources below are mutable upstream branches unless a version is stated; exact
commit pins and byte layouts are required in the implementation review before
adopting a format. These are design studies, not exhaustive upstream audits or
authorization to copy code.

| Reference | Inspected source and mechanism | Selection for this project |
| --- | --- | --- |
| Kafka 4.1 | [FileRecords](https://github.com/apache/kafka/blob/4.1/clients/src/main/java/org/apache/kafka/common/record/FileRecords.java), plus [OffsetIndex](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java): file ranges and segment-local sparse indexes. | Separate navigation, verified payload access and segment lifecycle. Keep indexes derived and validate identity before reuse. Do not import replication-based durability assumptions into a single-node sync contract. |
| Redpanda dev | [segment appender](https://github.com/redpanda-data/redpanda/blob/dev/src/v/storage/segment_appender.cc) and [batch model](https://github.com/redpanda-data/redpanda/blob/dev/src/v/model/record.h): bounded chunks, ownership during in-flight writes, header and batch checksums. | Use bounded buffer ownership and deliberate checksum coverage. Start with buffered I/O; direct I/O adds alignment/cache obligations without current evidence of need. |
| ReductStore main | [block manager](https://github.com/reductstore/reductstore/blob/main/reductstore/src/storage/block_manager.rs) and [persistence](https://github.com/reductstore/reductstore/blob/main/reductstore/src/storage/block_manager/persistence.rs): lifecycle, index and cache responsibilities. | Keep these responsibilities separate; do not transplant its timestamp identity or infer broker ACK semantics from a block metadata flush. |
| Liftbridge docs v1.9.0 | [concepts](https://liftbridge.io/docs/concepts.html) and [cursors](https://liftbridge.io/docs/cursors.html): partition logs and internal compacted cursor streams. | Reuse the storage engine for future durable state. Queue/cursor work is outside the current storage optimization; cross-log atomicity remains explicit. |

## Decision A: segmentation and derived metadata

Proposed first implementation: configurable size-based rolling with a 128 MiB
default target, at batch boundaries. An oversized supported batch remains whole;
no group member can be split through a frame. Offset identity remains absolute
and partition-local. Sealed segments are immutable; the active segment appends.
Automatic deletion and compaction remain separate sub-gates with retention pins.

The detailed candidate and its unaccepted capacity, metadata-cost, rollover,
recovery and migration assumptions are developed in the
[segmentation review](ADR-024-segmentation-review.md). Acceptance of Decision A
must explicitly consider those assumptions; this proposal does not accept them.

Specify a new independently versioned directory identity and durable manifest
before coding. The manifest must identify the log, ordered segments, their
offset ranges, certified lengths, active segment and checkpoint generation.
Exact bytes, count/length bounds and checksums need fixtures. A recognized old
single-segment log stays readable with the original protocol; conversion is an
explicit offline copy to a new directory, never in-place or implicit. Unknown
or ambiguous layouts continue to refuse without deleting evidence.

Rollover protocol proposal: finish the prior commit; create and sync the next
segment; sync its directory entry; install a synced replacement manifest using
rename plus directory sync; only then accept writes into the new active segment.
No ACK may depend on a segment absent from durable metadata. A crash before
publication can leave an orphan: preserve and classify it under a reviewed
recovery state table, not by assuming any matching filename is disposable.
Multi-segment groups must certify every affected file or split at a group
boundary before encoding; the initial implementation should choose the latter.

Indexes remain caches only while reconstructible from authoritative segments.
Persisted indexes must bind to log/segment identity, format version, certified
length and offset bounds; reject/rebuild stale or invalid indexes. Sealing,
index publication and interrupted rebuild need crash tests.

Segmentation alone does not remove O(history) payload verification. Proposed
bounded-open mode loads validated metadata/indexes, verifies active recovery,
and verifies every fetched sealed batch before returning it, with a bounded
background scrub. It changes when corruption is discovered and must not claim
that unopened payloads were verified at startup. Retain full-scan mode as the
initial default until that trade-off and scrub progress semantics are accepted.
No verified-bit cache may silently stand in for validating changed file bytes.

## Decision B: checksum coverage

Retain current CRC coverage until ADR-003's exact alternative is accepted.
Recommended candidate: protect envelope and uncovered batch prefix with a header
checksum, verify Kafka CRC for its covered body, and consider a strong digest
when sealing a segment. Define the precise chain, if any, reset, byte order,
field coverage and version transition with golden vectors. CRCs detect
accidental faults; neither CRC nor an unprotected digest authenticates a writer.

Removing a full-payload pass changes the storage format and corruption model.
Require one-bit and malformed-header cases, stale/misplaced entries, truncated
headers/payloads, restart and migration tests, and matched end-to-end benchmarks.
Do not change checksums as a shortcut for direct-buffer encoding improvements.

## Decision C: bounded independent-request group commit

Proposed coordinator: one log owner, a bounded request queue and a configurable
maximum coalescing delay (candidate 1 ms), group byte limit and request limit.
Reserve both bytes and request slots before retaining accepted work. Account for
queued, active, completion and client-held buffers; count bounds alone are not
a global memory budget. Queue saturation returns a defined admission result.

Serialize offset assignment; collect requests up to size or deadline; execute
the existing segment-sync/checkpoint-sync/rename/directory-sync protocol once
per group. Every successful response contains only offsets covered by that
group's completed barrier. A timed-out/cancelled accepted request may still
commit. Preserve a request-to-offset-range map and distinguish admission rejection
from uncertain execution. On terminal failure latch unavailable, resolve affected
requests honestly and reject queued work that has not begun.

Shutdown must stop admission, drain or explicitly reject pending work, resolve
waiters and join the owner without deadlock. An independently failing reader
must communicate certified corruption to that same owner before another write
is admitted. Do not equate async submission with persistence or assume one
file sync flushes all partitions. Cross-partition coordination and async I/O
remain later decisions; a first coordinator can use a dedicated blocking owner.

## Acceptance and remaining decisions

- Human acceptance of A/B/C and exact format/recovery additions before their
  implementation; persistent indexes also require the ADR-003 amendment.
- Rollover/create/publish/commit interruption matrix and refusal/orphan rules;
  golden migration fixtures and old-reader/new-writer behavior.
- Deterministic queue saturation, cancellation, shutdown and failure tests;
  independent requests share a measured sync while retaining correct outcomes.
- Full-scan versus deferred sealed verification decision, with stale-index,
  payload-corruption and scrub tests; no silent loss of acknowledged offsets.
- Measure current versus candidate latency, syncs, allocations/copies, RSS,
  startup and many-segment behavior under identical durability contracts.

The immediate repair PR keeps existing stored bytes and full verification.
This proposal resolves the architectural next steps into explicit decisions;
it does not claim segmentation, reduced checksum coverage or a coordinator is
implemented by that PR.
