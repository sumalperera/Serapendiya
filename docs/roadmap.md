# Serapendiya delivery roadmap

## Current stage: ideation plus technical foundation

Serapendiya is not presented as production-ready software. The project has a
working Rust foundation and an approved local-storage implementation, while the
customer-facing streaming workflow is still ahead.

### Open-source work to date

- G0: workspace, configuration, domain primitives, CLI skeleton, CI, ADRs, and
  provenance policy.
- G1 prototype: local append storage, CRC validation, checkpoints, recovery,
  derived indexes, bounded group commit, fault injection, crash harness, and
  Docker/Linux tests. G1 acceptance and crash-safety guarantees remain under review.

### Current decisions and implementation authority

Planning update: 2026-11-11. The founder requested the product and technology
plan update below. It selects planning priorities, not a stable storage format,
accepted ADR, completed gate or authorization to implement later gates.

| Area | Current state | Next evidence or decision |
| --- | --- | --- |
| G0 foundation | Present; historical evidence remains in the verification records. | Preserve provenance and supported-toolchain checks. |
| G1 storage | Implementation authorized; acceptance remains open. | Review ADR-003/004, recovery assumptions and operator guidance; obtain independent review. |
| Checksum comparison | Synthetic coverage experiment and compatible library integration measured locally. | Review [storage measurements](benchmarks/crc-library-comparison.md); Linux CI and broader target/performance coverage remain outstanding. |
| G2 and later | Planned; implementation requires separate authorization. | Accept applicable ADRs and define executable acceptance criteria first. |

This table is the public planning status; it does not supersede recorded ADR
acceptance decisions. The local blueprint mirrors this status. Historical
G0-only handoffs and proposed designs do not grant new implementation authority.

The selected direction is a versioned Serapendiya storage container and
Kafka-compatible behavior at the client boundary. The concrete format and
migration are proposed in
[the ADR-003 amendment](adr/ADR-003-hybrid-storage-proposal.md); an experimental
implementation is in progress and must be reviewed with the
[ADR-004 initialization proposal](adr/ADR-004-initialization-proposal.md).
The [selected G1 recovery scope](adr/ADR-004-recovery-state-proposal.md) retains
fail-closed handling of ambiguous initialization and documents unknown append
outcomes. Externally restored valid checkpoint rollback detection is deferred;
the full recovery protocol and storage format still require review.
The competing checksum and versioning options are assessed in the
[storage recovery review](adr/ADR-003-checksum-and-recovery-review.md).
The [ADR-005 proposal](adr/ADR-005-shard-ownership-proposal.md) records future
shard ownership, backpressure and benchmarking questions without authorizing
partition actors, a runtime change or replication.
The [agent trust-model proposal](adr/ADR-023-agent-trust-model-proposal.md)
records future identity, authorization and uncertain-publish requirements;
it does not authorize agent-facing features in G1.

G2 Kafka compatibility requires explicit authorization. It must remain a narrow,
tested subset with honest unsupported errors and real client evidence. No Kafka
wire implementation exists in this repository yet.

### Delivery outcomes and dependencies

| Stage | Planned outcome | Required evidence |
| --- | --- | --- |
| Finish G1 | Reviewed local storage/recovery contract and a measured buffered-I/O baseline. | Exact format and failure model; independent review; golden fixtures and supported-filesystem evidence; append/read/restart measurements. |
| G2 | Narrow Kafka client boundary and bounded batch processing; proposed partition ownership work requires an explicit scope decision. | JVM and librdkafka clients, documented settings and API versions, malformed input and overload tests, measured memory limits. |
| G3 | Independent broadcast cursors and queue subscription state. | Restart recovery, stale-ACK fencing, bounded ACK holes and attempts, idempotent transitions, source-retention rules. |
| G4 | Publish, inspect, diagnose and read-only replay preview, with scoped access on exposed interfaces. | End-to-end workflow, permission checks across adapters, structured errors, documented uncertain-write reconciliation. |
| G5 | Hardening of the explicitly supported deployment scope. | Fuzz/stress results, failure playbooks, upgrade/refusal behavior, resource limits and security evidence. |
| Later releases | Replication, compaction, tiering and richer history queries according to demonstrated needs. | Separate ADRs, failure models and workload evidence; no implied full Kafka parity. |

These stages are sequencing gates, not delivery-date promises.

Segmentation and safe retention are prerequisites for sustained-use pilots.
They need an explicit implementation sub-gate before such a pilot, even if an
earlier prototype only applies disk-quota backpressure. Design the segment
lifecycle during storage review; do not add deletion to G1 by implication.
Replication, queue state, replay pins and idempotency evidence must constrain
retention. Exhausted storage must cause defined backpressure or explicitly
authorized expiry, never silent deletion of pinned data.

Authentication and server-side authorization are prerequisites for exposed
network deployments, including Kafka listeners. If needed before G4/G5, move
that security dependency forward explicitly; do not treat its later gate label
as permission to expose an insecure service. Local protocol experiments remain
explicitly isolated. Hardened transport and tenant isolation are production
requirements. Security design is detailed in
[ADR-023](adr/ADR-023-agent-trust-model-proposal.md).

## Planned end-to-end workflow

A future development milestone is:

```text
publish event → inspect event → restart process → recover event
```

That workflow should be built and tested as a vertical slice only after the
relevant gates are authorized. It must be presented as a prototype until client
compatibility, recovery, security, and operational claims are independently
verified.

The next product extension is:

```text
resolve publish outcome → resume subscription → preview replay → authorize mutation
```

Native publish outcomes should distinguish committed, rejected before acceptance
and unknown. Status queries have authorization, availability and retention
limits; not every unknown outcome can later be resolved. Kafka responses retain
their protocol semantics. Durable idempotency requires a separately reviewed
contract; a bounded cache alone does not make retries safe after restart or expiry.

Replay preview cannot alter live cursors, ACKs or external systems. Executing a
replay is a separate future operation with authorization and destination
idempotency/reconciliation. The platform cannot guarantee exactly-once payment,
email or tool execution merely by retaining an event.

## Technology and reuse priorities

Every feature or material redesign follows the standing engineering evaluation
process in [the reuse plan](architecture/technology-reuse-plan.md). Before coding, review applicable established
approaches and reusable components, record the chosen approach and deliberate
differences, and define the correctness/performance evidence. This rule applies
across all gates without granting implementation authority for future gates.


Use [the technology reuse plan](architecture/technology-reuse-plan.md) as the
decision checklist before building a subsystem. Reuse maintained libraries for
commodity mechanisms; keep the broker's ownership, durability and authorization
contracts explicit and test their integration. No listed library is a preapproved
dependency. MIT remains the project's license.

1. Preserve encoded batches through the ordinary append/fetch path, with bounded
   validation and memory reservations. Decode for inspection, supported semantic
   validation and compaction where needed.
2. Develop ADR-005 into a decision-ready ownership and buffer-lifetime contract.
   One shard is a first-class deployment; affinity is configurable. Benchmark
   buffered I/O before selecting asynchronous I/O or a custom cache.
3. Evaluate internal logs for durable cursor and queue transitions using the
   same storage engine. Independent subscription semantics do not require a
   separate journal implementation. Cross-partition atomicity is not automatic.
4. Specify segment roll/seal/replacement, rebuildable indexes and retention
   pins before sustained-use pilots. Compaction needs its own gap, tombstone,
   producer-state and crash-safe replacement rules.
5. Keep consensus replication distinct from asynchronous selective forwarding.
   A local sync, a quorum commit and an upstream delivery receipt are different
   outcomes with different failure models.

## History, agents and artifacts

Keep offsets authoritative for ordering within a partition. Future time-range
and selected-metadata indexes are query aids, not a new global ordering promise.
Distinguish producer timestamps from broker-observed time and specify late data,
clock skew and duplicate timestamps. Correlation and causation fields supplied
by applications are untrusted; separate them from server-attested provenance.
Metadata searches and discovery must enforce the same permissions as fetch.

For large documents, images and recordings, initially evaluate external object
storage plus event references, hashes and metadata. Specify completed upload
before reference publication, uncertain publish outcomes, authorization and
orphan cleanup. Never put credentials in event references. This is distinct
from tiering sealed log segments and does not authorize a second object store.

MCP remains scoped and read-only first, R2 or later. Mutation follows explicit
authorization and idempotency/adversarial tests. Prefer an external schema
registry adapter when contracts become necessary; defer a custom registry,
multiple analytics destinations and embedded execution until user evidence
justifies each subsystem. Existing future research remains available without
making every item a prerequisite for the first useful release.

## Next reviewable work

- Close the ADR-003/004 decision and independent-review gaps without weakening
  certified-range integrity or treating synthetic results as disk benchmarks.
- Record a baseline for current append/read/restart behavior, sync duration,
  throughput, CPU, RSS and p50/p99/p99.9 latency. Report machine, filesystem,
  compiler, batch size, workload and raw results; distinguish unsupported
  many-partition workloads from measurements possible today.
- Prepare the ADR-005 decision packet and G2 scope with explicit ownership,
  admission, cancellation and client contracts before implementing the bounded
  single-shard workflow.
