# ADR-024: journal authority and quarantine comparison

Status: **COMPARISON FOR REVIEW; no format, recovery policy, cleanup or
implementation accepted.** This compares the direction selected by the founder
with the [J1 journal packet](ADR-024-metadata-journal-decision-packet.md),
[retirement candidate](ADR-024-journal-rotation-candidate.md) and the earlier
single-manifest baseline. The comparison does not approve a change to existing
certified-range integrity, sync-before-ACK, full startup payload verification,
legacy compatibility, or the preservation of ambiguous crash artifacts.

## Failure model and what a checksum proves

Assume one cooperating writer under the existing locks, successful syncs
honored by the documented filesystem/device stack, and arbitrary interruption
between operations. Separately consider detected accidental corruption,
misdirected/truncated files and external coherent rollback. The ordinary
power-loss assumption does not claim that hardware always honors flushes or
that later media corruption is impossible. CRC-32C detects many accidental
changes, not all changes, and is not an authenticity check.

On restart, the same malformed final journal bytes can result from (A) an
interrupted write whose operation was never acknowledged or (B) damage to a
previously synced and acknowledged record. CRC and framing can identify the
invalid record; they cannot distinguish A from B. Refusing both preserves the
evidence and is compatible with a strict fail-closed policy for *observed*
corruption, though availability suffers. Truncating to the last valid record
would silently lose acknowledged data in B. A completely missing final record
may leave no malformed tail to observe; distinguish loss/truncation of previously
synced bytes from a normal crash under the stated honest-sync model. Without an
independent certified boundary, those two histories can look identical on
disk. Coherent external rollback is already outside local detection, but the
scope and handling of lost or damaged acknowledged bytes must be explicitly
decided, not hidden in the phrase “CRC-valid tail.”

[RocksDB's WAL-tracking design](https://github.com/facebook/rocksdb/wiki/Track-WAL-in-MANIFEST)
(inspected 2026-11-13) identifies the same ambiguity for a corrupted WAL record
and discusses tracking synced size. Its choices have their own durability
scope; they do not prove this project's ACK contract. [Kafka's 4.0 operational
guidance](https://kafka.apache.org/40/operations/hardware-and-os/) describes a
replication/OS-flush durability approach that is not a matched single-node
sync-before-ACK baseline. The pinned Kafka, Redpanda, ReductStore and
Liftbridge source studies and [RocksDB/LevelDB assessment](ADR-024-segment-cap-alternatives-review.md)
remain applicable; none establishes that the proposed two-sync protocol meets
our contract. No upstream code or dependency reuse is proposed.

## Authority alternatives

Sync counts below describe a non-roll group on an already established local
filesystem and the candidate ordering, not measured latency. They exclude
device-specific flush behavior and rare initialization or rollover work.
Batching multiple requests changes the cost per request but needs its own
coordinator, cancellation and unknown-outcome decision.

| Candidate | Normal per-group sequence | Trade-off |
| --- | --- | --- |
| Existing single segment / proposed whole-manifest baseline | Segment sync; checkpoint or manifest temp sync; rename; directory sync; ACK | Three explicit sync operations per group. The new whole-manifest candidate rewrites O(segment count) bytes; current legacy checkpoint is fixed-size. The certified boundary allows corruption inside it to fail closed. |
| J1: journal plus certified checkpoint | Segment sync; journal sync; checkpoint temp sync; rename; directory sync; ACK | Four explicit sync operations and fixed-size per-commit checkpoint, with two durable files linked by authority. Actual latency ranking is unmeasured; rotation and cleanup add states. |
| J2: journal as authority; optional lazy snapshot for replay | Segment sync; append complete framed journal record; journal sync; ACK | Potentially two explicit file syncs in steady state. Must define authority identity, durable creation and rotation, replay bounds, how valid unacknowledged records are handled, and a strict invalid-tail policy. A lazy snapshot is an optimization only unless its certified relationship to later records is specified. No shown mechanism detects a fully missing acknowledged tail while also distinguishing ordinary uncommitted absence. |
| Batched authority publication | Share one barrier across multiple accepted requests, ACK each only after coverage | Can amortize J1/manifest syncs, but admission, bounded memory, cancellation, offset assignment, shutdown and uncertain responses are Decision C work; no batching is authorized by this comparison. |

J2 does **not** follow merely from a configurable strict-refuse switch. An
invalid suffix can be refused, but a strict policy needs a complete definition
of partial records, missing files, recovery availability and loss of previously
acknowledged bytes, not just a parser setting. A complete valid record whose
caller did not receive an ACK may legitimately appear after restart; the caller
still has an unknown outcome. Recovering it does not establish exactly-once
delivery. No algorithm here can identify whether an ACK reached the client.

### Checkpoint cadence as a design axis

The external review suggests acknowledging after each journal sync while
publishing a checkpoint every N committed groups. This is a useful family of
designs to compare, but cadence alone is not a complete recovery protocol.
For finite N, a bound on acknowledged-but-not-checkpoint-certified groups exists
only if admission and concurrency enforce it: checkpoint failure or delay must
stop new acknowledgments before that bound is exceeded. Define whether the
Nth group is acknowledged before or after checkpoint publication, in-flight
requests, cancellation, checkpoint failure and restart. A time-only cadence
without an admission limit has no fixed group-count bound. Journal rotation,
snapshot bounds and disk exhaustion also remain explicit.

| Design point | Acknowledgment and expected normal sync work | Certified-gap implication |
| --- | --- | --- |
| J1, checkpoint every group **before ACK** | Segment sync, journal sync, checkpoint-file sync, rename and directory sync before response: four explicit syncs. | The completed checkpoint covers every successful ACK in the stated filesystem model. |
| N-group cadence, ACK after journal sync | Segment and journal sync for each group; checkpoint-file and directory sync on checkpoint publication. In an ideal serialized steady state, N groups per successful checkpoint gives 2 + 2/N explicit sync calls per group on average; it is not a latency or throughput result. | Up to N ACKed groups after the prior successful checkpoint can be outside its certified range **only if** the admission rule enforces that bound. State how a slow or failed checkpoint stops ACKs and how partial progress is reported. |
| No per-commit checkpoint, ACK after journal sync | Two explicit syncs per group; snapshot/rotation work is additional. | No checkpoint-certified per-commit bound on the tail. Finite storage and replay still require a reviewed rotation and snapshot protocol. |

`N = 1` is equivalent to J1 only if the checkpoint's complete publication
barrier precedes that group's ACK. If the group is ACKed first and its
checkpoint is written later, even `N = 1` has an acknowledged-but-uncertified
interval. Durability of a complete synced record does not reveal whether its
caller received the ACK. A fully valid post-checkpoint record can be considered
for replay only after identity, sequence, topology, referenced segment bytes
and data-before-journal ordering validate; it may represent an operation with
an unknown client outcome. Record CRC alone does not prove all those properties.

| Gap disposition | Consequence and unresolved choice |
| --- | --- |
| Refuse on an observed invalid/torn record, preserving it | Avoids silently truncating *observed* ambiguous damage, at an availability cost. Requires an operator path or explicitly accepted unavailability. It does not detect an acknowledged final record that is wholly missing and leaves no observable trace. |
| Truncate/discard to the last valid record | Can silently lose an acknowledged record if the damaged bytes were previously acknowledged; this changes the corruption model and would need explicit founder acceptance. |
| J1-style checkpoint before each ACK | Keeps an explicit certified boundary for each successful group, subject to honest sync assumptions; adds authority sync/publication work. External coherent rollback of the checkpoint and data remains undetectable. |

These are distinct failure models rather than three proven ways to preserve the
same guarantee. In the ordinary honest-sync power-loss model, a successful
journal sync is assumed to persist its bytes; later data loss, truncation,
misdirected writes or a device that falsely reports a successful sync must be
assessed separately. The existing checkpoint can detect some missing or damaged
journal bytes when the checkpoint still certifies them, not coherent rollback
of all authority and data. Before any cadence change, explicitly decide which
failure classes remain detectable and how availability changes.

Ext4/XFS block/VM replay can test selected ordering and crash cut points, but a
zero-failure run does not establish a zero failure rate or cover later media
corruption, all devices, all cache behavior or external rollback. The RocksDB
WAL-tracking design describes a particular choice with active-WAL limits and
several sync paths; it should not be reduced to a fixed N-groups cadence or
treated as proof of this protocol. Independent tests of this integration and
explicit human acceptance of any changed failure model remain required.

## Restart observations: J1 versus J2

The checkpoint in the table is the last authority actually observed after
restart, not a guess based on whether a pre-crash syscall returned an error.
For J2, “valid” requires identity, sequence, framing, checksum, referenced
segment bytes and offset continuity, not merely a record CRC.

| Observed case | J1: certified checkpoint | J2: sole-authority journal candidate |
| --- | --- | --- |
| Complete, validated record beyond last checkpoint | Do not promote it. After full certified-history verification and artifact classification, the current candidate may truncate the uncertified suffix; preserve/refuse if a temporary or other ambiguity prevents safe action. | If the full record and referenced data validate, candidate may retain it as committed even without a client ACK. Its acceptance and any earlier/later completeness must follow a specified journal authority and replay protocol, which does not yet exist. |
| Partial/CRC-invalid record beyond last checkpoint | After all certified ranges validate and other artifacts classify safely, candidate may truncate only the uncertified journal/data suffix; fail closed on I/O errors. | Strict candidate refuses and preserves. A last-valid-record truncation policy can discard an acknowledged record if the damaged record had been acknowledged. |
| Invalid, truncated or missing record within checkpoint-certified length | Refuse; never truncate or roll back that certified range. | Without an equivalent boundary, a parser can refuse observed invalid bytes but cannot infer from framing alone that a completely missing record once existed. |
| Segment data present but no corresponding valid journal record | If outside certified data, handle as uncertified suffix only after full validation; other ambiguity refuses. | Do not serve it solely because segment bytes exist. A partial/invalid journal record is refused under strict policy; an entirely absent record may be indistinguishable from no committed update. |
| Complete valid checkpoint/new journal after publication but client saw error | Validate the observed authority; retain data it certifies. Client outcome remains unknown. | Validate the observed journal authority; a durable valid record can survive an unacknowledged operation. Client outcome remains unknown. |
| Coherent old authority and matching old data restored externally | May open as an earlier consistent state; local generation cannot establish freshness. | Same limitation unless independently durable freshness/fencing authority is designed. |

This comparison deliberately separates process crash, later corruption, torn
write and external rollback. Testing must inject each within its stated model;
passing process-kill tests cannot establish behavior under corrupted storage or
power loss. A more permissive J2 tail policy or altered corruption guarantee
would be a new ADR-level decision requiring explicit human acceptance.

## Retired-journal quarantine versus bound cleanup

Both candidates first require a completely validated new snapshot and a
durably published checkpoint naming the new authority. Moving or removing an
old metadata journal never authorizes deletion of event segments. Under the
working [retirement-binding candidate](ADR-024-journal-rotation-candidate.md),
the checkpoint identifies one retired journal and its expected content; if
separately accepted, the implementation validates that binding before unlink
and directory sync. Unexpected or mismatched files cause refusal.

An alternative is an explicitly identified no-overwrite move from the live
directory to a dedicated `retired/` directory on the **same filesystem**, with
directory-relative no-follow operations and cooperative locks. `rename`
provides runtime namespace atomicity subject to filesystem semantics, but a
cross-directory crash transition cannot be assumed durable merely because
rename returned success. Specify and test creation of `retired/`, no-replace
capability, source and destination directory syncs, same-filesystem/EXDEV
handling and failures of each; see [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)
and [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html). A
cross-filesystem copy-and-delete is a different protocol and is not implied.

| State observed after restart | Conservative rule to evaluate |
| --- | --- |
| Old authority plus new journal or temporary authority | Refuse and preserve; the new journal is not authority and a name alone does not justify cleanup. |
| New authority validated; specifically bound old journal remains in live directory | Preserve or perform an explicitly accepted, identity-checked move only after making the new authority and relevant directory entries durable. |
| New authority validated; specifically bound old journal in `retired/`, absent in live directory | Preserve it; treat it as non-authoritative only after validating its binding and the new certified state. Directory sync failures and external changes do not prove which prior syscall completed. |
| Both locations contain conflicting entries, wrong file types/identities, destination collision, unbound extra file or unreadable metadata | Refuse and preserve; never overwrite or select by generation. |
| Neither location contains the expected retired file | Do not assert it was safely moved or deleted. Whether the new authority may open with an absent bound retired file is a distinct explicit decision; coherent rollback and uncooperative deletion are not detected. |

“The file is always in exactly one of two directories after a crash” is not a
guarantee established here. Even a non-destructive move needs crash-state and
directory-durability evidence on supported filesystems. Retired metadata
accumulates unless a separately reviewed bounded archive or cleanup policy is
chosen; an eventually full filesystem still needs backpressure. Preserving an
artifact forever is not a disk bound. Neither automatic move nor unlink is
authorized by this comparison.

If a `retired/` directory is later accepted, the new-layout directory classifier
must explicitly validate that directory and its permitted contents without
following unsafe links. The existing old reader's strict whitelist must remain
unchanged; extend old-reader refusal fixtures to cover both empty and populated
`retired/` directories. This is future validation work, not permission to add
the directory or modify the current reader's recovery behavior.

The review proposes an idempotent automatic move: if the specifically bound
retired journal remains live, retry a same-filesystem no-replace move; if it is
already in `retired/`, validate and treat it as non-authoritative. This could
reduce routine intervention, but it is not yet proven safe. The design must
specify lock/identity checks, behavior if the destination name already exists,
creation and durability of the retirement directory, both directory syncs,
EXDEV/unsupported errors, and collision or unexpected-entry refusal. Runtime
rename atomicity and a claimed ext4/XFS journal transaction do not by themselves
establish that every post-crash state contains exactly one durable copy. Test
the concrete filesystems and mount configurations with power-loss replay; on
both-present, neither-present, mismatched or unreadable evidence, do not infer
success or erase artifacts. Allowing a validated move during recovery is a
separate mutation decision. Even if moving is approved, the archive is not
bounded until its cleanup and disk-exhaustion policies are reviewed.

## Decision and validation required

The journal direction is selected for design, not a choice of J1 or J2, its
tail policy, a reduced corruption guarantee, or automatic retirement. Do not
implement either new persistent protocol before its exact bytes, bounds,
initialization, rollover, recovery and migration are accepted by the founder.
The user-facing decision must state the availability consequences and whether
any acknowledged-data corruption can become an undetectable tail loss.

Before coding, independently review a complete state machine, evidence
preservation rules, operator procedure or accepted unavailability rationale,
and complete golden fixtures with a separate verifier. For an authorized
implementation, run deterministic failures before/after every write, file
sync, rename, directory sync, truncate and quarantine move; include malformed
and wholly missing tails, failed syncs, collisions and process crashes. Then run
separately reported ext4 and XFS block/VM power-loss replay. Test accidental
post-ACK corruption separately from ordinary power-loss assumptions.

Compare candidates with identical local durability, payloads, group sizes,
hardware, filesystem, compiler and target segment counts; capture raw p50,
p99/p99.9 where supported by sample count, throughput, sync counts, bytes,
CPU/RSS, journal rotation and full-history startup. Until then no candidate is
established as fastest. A retained-file/quarantine limit, time-based retention,
wire protocol and lakehouse integration remain separate design decisions.
