# ADR-003/004 review: split checksums, versions and recovery

Status: **PROPOSED alternatives; no decision or stable format.** The
implementation observations below were recorded before the directory-descriptor
hardening in PR #15; see [the ADR reconciliation review](../verification/G1-adr-reconciliation.md)
for current behavior and its limits. The founder
requested an engineering review of the 2026-11-09 proposal; the container
merged in PR #13 is experimental and uses the earlier full-payload CRC.
The safety changes in PR #12 are merged independently.

## Refined split-checksum candidate

Threat model: detect accidental corruption, stale and misplaced writes, not
malicious tampering. CRC32C is not a MAC. The proposed chain is an additional
layer alongside segment identity, known allocation state and offset validation;
it does not guarantee detection. A 2^-32 estimate assumes a suitable random
error model; it is not a bound for correlated or adversarial faults.

Candidate construction, pending a complete byte layout: initial chain state is
the standard one-shot CRC32C of `segment_uuid || segment_base_offset ||
segment_header_crc`. Each entry's header checksum is standard one-shot CRC32C
of `previous_chain_state_le32 || entry_index_le64 || byte_position || offset ||
envelope_fields || kafka_batch_prefix[0..21]`; update the chain state to that
checksum. The exact widths, endianness and field order for UUID, offsets,
position and envelope must be specified before golden vectors can be generated.
This proposal does not rely on a library-specific seeded CRC API. Uncompacted
entries must also satisfy the accepted offset-continuity rules. Compacted
segments require different, explicitly specified validation.

Candidate serving policies: strict verification of container metadata and
Kafka CRC before serving; verified-sealed reads supported by a specified scrub
and footer policy; and a visibly labelled development-only mode. Strict is a
candidate default, not an implemented or approved policy. A verification cache
requires an invalidation model for file replacement, external modification,
reopen, and page-cache eviction; a once-per-process bit alone cannot establish
that later bytes are intact. A timestamp of a previous scrub is evidence of that
scrub, not proof against later corruption. Define partition health and explicit
corruption errors, and benchmark both latency and integrity trade-offs.

## Proposed initialization and classification

A candidate initialization sequence writes a versioned `FORMAT` marker by
temporary-file sync, rename and directory sync before creating segments. A
valid marker, identity, supported version and valid segment headers are needed
to classify existing data as current. Unknown, future, legacy-suspect, damaged
or ambiguous data fails closed. Restarting an interrupted initialization with
only `FORMAT.tmp` or a lock target is safe only if the protocol can prove those
files were created by this initialization and cannot be an unrelated preexisting
file or evidence of later data loss. Do not automatically delete them merely by
name.

Evaluate non-mutating exclusive directory-descriptor `flock` on supported local
filesystems, relative descriptor-based file operations, no-follow path handling,
classification under lock, and repeated concurrent opens. An open directory
descriptor alone does not prevent other programs from modifying files; all
cooperating actors must follow the locking protocol, and directory replacement,
rename and symlink races need explicit treatment. The current implementation
uses a `.lock` file and does not yet meet the proposed no-mutation-on-refusal
guarantee. Filesystem, symlink, permission and crash tests are required before
selecting or claiming the stronger protocol.

## Kafka coverage and proposed alternative

The Kafka RecordBatch v2 CRC32C covers bytes beginning at `attributes`; base
offset, length, leader epoch, magic and the CRC field itself are outside that
coverage. A broker can stamp base offset and leader epoch without recomputing
the Kafka CRC, assuming it does not change CRC-covered bytes. Source:
https://kafka.apache.org/44/implementation/message-format/

Candidate A keeps the prototype container's separate full-batch CRC. It
adds a complete pass and covers the Kafka prefix and payload, in addition to
Kafka's own CRC. Candidate B checksums the internal entry envelope plus Kafka's
21-byte prefix, and verifies Kafka's CRC for the remaining bytes. A chain over
headers and a per-segment identity may improve detection of accidental stale or
reordered entries. The CRC seed mapping, exact serialized fields, byte order,
chain reset, length bounds, and handling after a format upgrade must be fully
specified before use. A segment UUID is larger than a CRC32C seed: it must be
incorporated by a defined algorithm, not treated as a directly interchangeable
integer. CRC32C is not cryptographic authentication and does not guarantee
collision-free splice detection or reject every stale write. Test the exact
model, and do not claim absolute detection.

Candidate B may avoid re-reading payload bytes solely for a second CRC, but
there is no performance result for this project. Benchmark end-to-end append,
recovery and fetch with representative batch sizes, I/O and hardware. Do not
claim a fixed byte saving or faster operation before defining the actual layout
and measuring it. The current prototype's bitwise CRC32C is not evidence of
hardware-accelerated throughput.

## Verification and integrity policy

Evaluate a strong per-segment digest for sealed immutable segments separately
from Kafka CRC and the header checksum. Specify algorithm, cryptographic versus
accidental-corruption goal, expected overhead, when the digest is computed and
verified, and authenticity/trust of the stored footer. Neither a footer nor
an unkeyed digest establishes that a malicious party has not modified both
the content and digest.

Header-only recovery or serving a batch without validating its payload can
return corrupted data before a background scrub finds it. Those are explicit
availability, latency and integrity trade-offs, not verified-safe defaults.
ADR-004 must specify what is verified before serving, on which crash model,
and the consequences of corruption after a successful fsync. External Kafka
clients cannot be assumed always to verify payloads. Preserve fail-closed
handling of invalid bytes in the certified range unless a reviewed alternative
proves acknowledged data is not discarded.

## Versioning and legacy

Explore independent versions for the directory identity/format manifest,
segment container, Kafka batch magic, rebuildable index and durability
checkpoint. An upgrade/finalization protocol must define which old readers and
writers can safely read and append before a new version is written; allowing
new-version segments before finalization does not automatically ensure that
an old binary can roll back. Unknown critical flags and future versions fail
closed. Mixed-version behavior, golden fixtures, downgrade, interrupted
finalization and segment rolling require tests before promising upgrades.

The old prototype has no unique segment magic: it begins with a Kafka-shaped
base-offset field. It cannot always be positively distinguished from arbitrary
or damaged bytes by a signature alone. Treat non-current, damaged, future and
ambiguous input as non-writable unless positively classified by a documented
procedure. Detect-and-refuse without a migrator is a candidate policy; whether
the prototype contains valuable data is unknown. No existing data may be
discarded or migrated without a separate explicit operator decision.

The experimental `Log::open` rejects unknown directory entries and a nonempty
directory without `.lock` before creating the lock; a valid checkpoint no longer
causes a missing segment to be created. Tests cover an unknown file and a data
directory symlink. Path-based inspection and later opens are not
descriptor-relative and have check/use races. It therefore **does not meet an
absolute no-writes-on-refusal guarantee** under concurrent directory replacement
or non-cooperating processes. A new protocol must specify locking and
classification without mutating an ambiguous directory. Test full filesystem
snapshots and concurrent openers; do not claim this guarantee is implemented.

## Replication and compaction dependencies

The proposed local envelope and logical replication boundary must be designed
together with terms, fencing, truncation, membership and snapshots. A locally
durable follower suffix can later be truncated if not committed; the initial
single-node ACK guarantee must not be reinterpreted as quorum commitment.
Reserving a field is not an implementation. Segment identities and footer
hashes can be replica-local or logical; tier-upload ownership, file identity and
replica verification follow from that choice and remain undecided.

Kafka compaction can leave offset gaps and preserves record offsets. Its
documentation also describes empty batches retained for producer state:
https://kafka.apache.org/44/design/design/ and
https://kafka.apache.org/44/implementation/message-format/. A future
active/sealed/compacted lifecycle, checkpoint identity, safe segment swap,
cleaner progress and offset lookup must be specified together, with crash
tests for each replacement boundary. Record and batch offset semantics cannot
be reduced to an unqualified first-offset-greater-than rule. None of these
future capabilities is authorized or implemented here.

## Decision and acceptance work

Before finalizing ADR-003/004: compare checksum candidates under an explicit
corruption model; define exact byte layouts and verification on each path;
design read-only classification and locking; specify initialization, rollback,
and safe handling of legacy data; describe replication/compaction constraints;
and validate with golden Kafka fixtures, malformed inputs, deterministic
power-loss simulation and supported-filesystem tests. Independent review and
reproducible performance measurements are required for any comparative claim.
