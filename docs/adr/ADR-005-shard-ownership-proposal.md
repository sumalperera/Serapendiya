# ADR-005 proposal: partition ownership and bounded data plane

Status: **PROPOSED. No runtime, actor or replication implementation is
authorized by this document.** This records a candidate architecture and the
questions that must be resolved before partition actor work.

## Candidate ownership model

Assign each partition exactly one owner among a configurable fixed number of
shard threads. Partition log position, admission, index and future queue state
are mutated through that owner. Requests can arrive on a separate ingress shard
and cross bounded message queues. One shard is a first-class configuration.
CPU pinning is optional and must respect available CPU sets and small hosts.
An explicit partition-to-shard mapping must be versioned or reconstructible;
changing its size requires a safe ownership handoff and must not change data
offsets. An illustrative hash modulo shard count is not a migration protocol.

Open questions: ownership of connections touching multiple partitions; buffer
transfer and budget accounting across shards; fairness, overload and cancellation;
handoff fencing and outstanding writes; failure and shutdown; and avoiding
deadlock while reservations and queues are held.

## Buffers, I/O and durability

Evaluate immutable validated encoded Kafka batches as an internal data-plane
unit. Validate bounds and supported semantics before accepting a batch. Charge
memory reservations before receiving or allocating a body, and release only
after all holders and in-flight operations finish. Bound both request counts
and bytes for ingress, writes, responses and caches. Backpressure must propagate
to clients. No zero-copy claim is made: page cache, TLS, protocol framing and
cross-thread transfer can require copies.

Start research from buffered I/O and a replaceable, fault-injectable backend.
An asynchronous API must specify buffer lifetime until completion, cancellation,
partial writes and ordering; merely making the current synchronous trait generic
does not do that. Compare io_uring and direct I/O against the baseline only after
the required durability and fault models are established. Monoio is a runtime
candidate, not an approved dependency or a measured choice:
https://docs.rs/monoio/0.2.4/monoio/

An epoch coordinator could collect bounded requests by size and delay while
another epoch is in flight. The owner must advance each partition's durable
boundary only when that partition's write and checkpoint protocol succeeds, and
acknowledge only covered requests. One file sync does not automatically make
other files durable. Compare a shared physical log against per-file sync with a
recovery, isolation, indexing, retention and latency analysis. A failed append
can have an unknown outcome; idempotency and reconciliation need explicit design.

Linux documents that file sync does not necessarily persist its directory entry
and that fdatasync includes metadata needed to retrieve the file:
https://man7.org/linux/man-pages/man2/fsync.2.html. Actual filesystem and hardware
assumptions must be tested; asynchronous completion does not itself prove durable
ordering. See also https://man7.org/linux/man-pages/man2/io_uring_enter.2.html.

## Acceptance evidence before selection

- State-machine tests for single ownership, cross-shard backpressure, cancellation,
  ownership transfer and memory accounting, including a one-shard deployment.
- A reproducible buffered-I/O baseline with build, machine, filesystem, sync
  policy, batch sizes, concurrency and raw results. Measure throughput, CPU,
  allocations/copies, RSS, restart time and p50/p99/p99.9 ACK and sync latency.
  Include low load, saturation, cold/warm reads and many partitions.
- Deterministic failures and power-loss simulation, followed by ext4/XFS and
  block-level fault testing. Record seeds, commit and filesystem assumptions.
- Only after baseline evidence: compare runtime, affinity, preallocation,
  io_uring, direct I/O and cache alternatives. Do not assert Rust, a particular
  runtime, or an I/O mode is faster without matched workloads.

Replication, shared logs, queue state, compaction and tiering remain future
decisions with separate authorization. A similar data-plane interface alone does
not establish replicated commit, fencing, atomicity or retention semantics.
