# Technology reuse and integration plan

Status: planning priorities updated at the founder's request on 2026-11-11.
Candidates below are not accepted ADRs, pinned dependencies or authorization
for later-gate code. See [the roadmap](../roadmap.md) for scope and sequencing.

## Reuse policy

Start with established mechanisms and maintained implementations. A widely used
library is evidence for its component, not proof of our integration or workload.
Review exact versions, upstream maintenance, advisories, MSRV, platform support,
transitive dependencies and licenses when selecting a dependency, as required by
[PROVENANCE.md](../../PROVENANCE.md). Record source revisions for adapted material;
preserve required notices. No upstream code is imported by this plan.

Prefer dependencies and specifications over porting another broker's internals.
Keep original product behavior in ownership, durability, admission, subscription
semantics, authorization, diagnosis and replay. Do not build custom cryptography,
cloud request signing, a consensus algorithm or a general query engine merely
to support those features.

## Established approaches to evaluate

| Mechanism | Intended application | Integration obligations |
| --- | --- | --- |
| Segmented append logs with sparse offset/time indexes | Bound file lifecycle work; seek into retained history. | Define roll/seal and durable manifest transitions; indexes are disposable only when retained authoritative data can reconstruct them. Persisted indexes need identity/version checks. |
| Immutable encoded batches and bounded group commit | Amortize framing, validation and sync; avoid repeated record materialization. | Validate the supported batch semantics; bound bytes and requests; preserve ownership through I/O completion; each affected file needs its own required sync. |
| Explicit shard ownership and message passing | Single writer per partition; local state and budget accounting. | Specify queue saturation, fairness, cancellation and fenced handoff. Begin with one shard; measure affinity and runtime choices before committing. |
| Durable internal logs plus materialized state | Reuse append/recovery for cursors and queue transitions. | Independent subscription state, deterministic replay and bounded snapshots; shared machinery does not provide atomic updates across logs. |
| Time/metadata indexes and capacity policies | Inspect event history within bounded disk use. | Timestamp semantics, metadata cardinality, authorization, retention pins and observable expiry; offset order remains partition-local. |
| Idle-partition resource suspension | Keep many low-traffic partitions inexpensive. | Reconstruct volatile state safely on activation; do not pause consensus obligations or discard durable subscriptions. Measure memory, timers and file descriptors per partition. |
| Consensus state machine with snapshots | Later replicated partitions and metadata. | Durable term/vote/log ordering, membership, fencing, truncation, committed versus local offsets, snapshot installation and restart tests. |
| Immutable remote objects plus versioned manifests | Later tiered segments and separately managed artifacts. | Upload verification, conditional publication, unknown outcomes, safe eviction, credential handling and bounded garbage collection. |

Queue design must decide which claims, attempt counts and transitions survive a
crash. Volatile leases can reduce sync work only if boot fencing, retry limits
and stale ACK behavior remain correct. Bound ACK holes with a window and define
what a stuck record does to progress. A DLQ source reference is valid only while
the source is pinned; copying to another partition needs its own deduplication
and recovery protocol. These are ADR-006 questions, not selected implementations.

## Reusable component candidates

The linked upstream documentation was inspected on 2026-11-11. Links describe
capabilities, not version pins or a completed dependency/security review.

| Component | Candidate and verified role | Selection gate and local proof |
| --- | --- | --- |
| CRC32C | [crc32c](https://docs.rs/crc32c/latest/crc32c/) provides Castagnoli CRC with software and SSE4.2 paths. | G1 follow-up candidate: verify identical checksum bytes with independent fixtures and differential tests, fallback behavior and actual ARM64/x86 support. Benchmark before changing checksum coverage. |
| Shared byte buffers | [bytes](https://docs.rs/bytes/latest/bytes/) provides reference-counted storage and shared slices. | G2 ownership design: add reservations/pooling only as needed; charge retained allocation capacity, not just visible slice length. The crate alone is not a bounded pool. |
| Descriptor-relative filesystem operations | Existing `rustix` dependency. | Continue using the established seam; retain directory, symlink, hard-link, lock and failure-boundary tests. No need for a new syscall wrapper. |
| Client interoperability | [rdkafka](https://docs.rs/rdkafka/latest/rdkafka/) wraps librdkafka; use alongside JVM clients and independent fixtures. | G2 test tooling, not a broker implementation. Pin client settings, test supported APIs and unsupported responses; client defaults are not proof of server support. |
| TLS | [rustls](https://docs.rs/rustls/latest/rustls/) implements client/server TLS. | Exposed network gate: review crypto provider/platform/MSRV and certificate validation. TLS does not supply tenant mapping, authorization or revocation policy. |
| Consensus | [raft](https://docs.rs/raft/latest/raft/) exposes a Raft state machine, a storage interface and `Ready` processing. | R3 candidate: review alternatives and exact persistence/completion ordering. Transport, durable storage and snapshot integration remain our responsibility; no library adoption yet. |
| Remote storage | [object_store](https://docs.rs/object_store/latest/object_store/) provides object APIs, conditional operations and multipart support. | Artifact/tiering gate: verify provider semantics and conditional writes; the library does not make log append and object publication atomic. |
| Property testing | [proptest](https://docs.rs/proptest/latest/proptest/) provides generation, assertions and a test runner. | Test-only candidate within the authorized gate: model acknowledged prefixes, crashes and recovery; retain seeds and minimized counterexamples. It does not model a physical device by itself. |

For future protocol codecs, compression, telemetry, Parquet and query execution,
evaluate maintained libraries or external engines at the relevant gate rather
than implementing these algorithms in the broker. Verify supported versions,
bounded decoding and compatibility then; no speculative dependencies now.

## First optimization experiment

Implementation follow-up: the existing wrapper now uses pinned `crc32c` 0.6.9.
See [compatibility verification](../verification/G1-crc-library.md) and
[local storage measurements](../benchmarks/crc-library-comparison.md).
This selects the checksum implementation, not a new coverage or container format.

Separate algorithm implementation from persistent-format changes. Evaluate a
library CRC against the current bitwise CRC on exactly the same bytes first.
Check the empty input, independent batch fixtures, incremental versus one-shot
semantics, varied lengths and both hardware/fallback paths where supported.
Equal outputs should preserve existing format bytes; changing coverage or chain
construction still requires ADR-003 approval.

The [existing checksum measurement](../benchmarks/checksum-model-comparison.md)
is a synthetic CPU comparison. It does not establish end-to-end append, fetch,
recovery or hardware-accelerated performance. Follow with the existing log API
under the same durability policy; record checksum, allocation, copy, queue and
sync costs separately where instrumentation permits. No new performance claim
is made by this plan.

## Reuse acceptance checklist

Before adopting a component, record:

1. The exact upstream revision/version, required features, license/provenance
   review and maintenance/MSRV/platform assessment.
2. The local contract it replaces, preserved bytes and behavior, error mapping,
   memory bounds and cancellation/ownership rules.
3. Differential fixtures, malformed inputs and relevant crash or adversarial
   tests, including failures at the boundary between the component and our code.
4. Measured costs under matched workloads and durability settings, including
   low load, saturation and small deployments where applicable.
5. Upgrade/rollback behavior, operational diagnostics and unresolved limits.

Keep the storage failure harness and independent client fixtures even after
adopting a dependency. Reuse reduces implementation work; it does not transfer
our end-to-end correctness obligations to the dependency.
