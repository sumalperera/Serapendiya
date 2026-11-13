# ADR-024: metadata journal decision packet

Status: **WORKING DRAFT; direction selected, concrete format and protocol not
accepted. No implementation authority.** On 2026-11-13 the founder selected a
metadata journal as the direction to develop, to be revisited after matched
benchmarks and recovery testing. This packet refines that direction; it does
not grant acceptance to the byte layout, recovery, migration or maintenance
procedures below. See [the alternatives review](ADR-024-segment-cap-alternatives-review.md)
for reference research, alternatives and the revisit condition.

## Existing boundary and hard question

The current single-segment implementation syncs the segment, then installs a
36-byte replacement checkpoint with a synced temporary file, rename and
directory sync before acknowledging. Startup verifies every certified frame
before truncating the uncertified suffix. Absolute offsets, sync-before-ACK,
full startup payload verification, immutable sealed segments and refusal on
ambiguous artifacts remain required. A terminal error may have an unknown
outcome; a coherent external rollback of all locally consistent authority and
data remains undetectable without an independent authority.

A journal containing checksummed commit records does **not by itself identify
which byte boundary was acknowledged**. After a crash, a damaged final record
could be an uncommitted torn write or corruption inside an acknowledged range.
Silently discarding it could lose acknowledged data; refusing every such tail
preserves evidence but reduces availability. A valid record could also have
been persisted without the caller receiving an ACK. Choosing the last valid
record as authority alone is not yet shown to preserve the existing contract.

### Authority candidates

| Candidate | Commit authority and trade-off |
| --- | --- |
| **J1: bounded journal plus fixed replacement checkpoint** | Checkpoint binds journal identity and certified journal byte length, plus the final offset. Data is synced before journal record; journal is synced before checkpoint replacement and directory sync; ACK follows the final barrier. Existing certified-range reasoning extends to the journal, but two file syncs plus checkpoint sync and directory sync remain. The per-commit authority write is fixed size, not proof of a latency improvement. |
| **J2: journal as sole authority** | Append and sync the journal after segment data, then ACK. Needs a separately justified durable mechanism to distinguish corrupted certified records from uncommitted partial tails, including torn writes, before it can meet fail-closed certified-range handling. A CRC, increasing generation or filename alone is insufficient. Do not silently redefine the corruption contract to select this option. |
| **J3: transactional metadata database** | A maintained transactional engine might provide the authority, but integration must prove its transactions cover the separately synced segment files, directory creation and recovery. No dependency is selected; license, maintenance, Rust 1.85, platform, transitive dependencies and crash behavior need review. |

**Working candidate for further review: J1**, because it makes the certification
boundary explicit and resembles the existing checkpoint ordering. This is not
an accepted design and does not prove multi-file atomicity. A journal may reduce
metadata bytes but J1 still does a replacement and directory sync on every
commit; whether it improves p99 latency is unknown. If that cost defeats the
goal, evaluate J2/J3 or the small topology checkpoint alternative with their
own full correctness evidence. Do not assume upstream success proves integration.
Compared with the existing per-group segment-sync, checkpoint-file-sync and
directory-sync sequence, J1 adds a journal-file sync on the ordinary commit
path. This extra barrier is a concrete concern, not a proof that every measured
latency must be higher: write size, filesystem, scheduling and workload also
matter. Pre-register J1 as a candidate that may fail the performance revisit
even if correct; compare barrier counts and matched latency/throughput before
choosing it long-term. Batching or deferring checkpoint publication needs a
separately specified ACK and certified-tail protocol; J1 does not authorize it.
The [96-byte explicit-retirement candidate](ADR-024-journal-rotation-candidate.md)
extends J1 for review; the 64-byte checkpoint below is its earlier sketch, not
the checkpoint for that rotation candidate. Neither is accepted.

## Candidate v1 bytes for review, not implementation

All integers are unsigned big-endian; lengths and arithmetic are checked before
allocation or mutation. CRC means CRC-32C, encoded big-endian. The following
numbers are candidate bounds for making the tradeoffs concrete, **not accepted
workload guarantees**: at most 4,096 live segment entries; 16 MiB maximum
certified journal length; 128 MiB default segment roll target; existing
group admission limits unchanged. A snapshot and any single record must fit
inside the journal bound. These bounds require workload review and fixtures;
neither the old manifest's cap nor a future retention policy is implied.

Candidate journal file `journal-<20-digit-epoch>.log`: a fixed 40-byte header:

| Bytes | Field |
| --- | --- |
| 0..4 | ASCII `SRJ1` |
| 4 | version 1 |
| 5..8 | zero reserved |
| 8..24 | nonzero 128-bit log identity |
| 24..32 | journal epoch |
| 32..36 | header length, 40 |
| 36..40 | CRC-32C of bytes 0..36 |

Each record has a 32-byte header: ASCII `SRJR` at 0..4; version 1 at 4;
type at 5; zero reserved at 6..8; total length u32 at 8..12; sequence u64 at
12..20; payload length u32 at 20..24; payload CRC at 24..28; header CRC over
0..28 at 28..32. Total length must equal 32 plus payload length. Check the
header and bounded lengths before reading a payload; CRC covers all payload
bytes. Record type 1 is a snapshot; type 2 is an append commit; type 3 is a
roll. Unknown types/versions, zero-length required payloads, sequence gaps,
overflow, bad checksums and trailing bytes within the certified length refuse.
Sequence is a checked diagnostic/order value, not a freshness proof.

Snapshot payload: next offset u64, entry count u32, four zero reserved bytes,
then exactly `count` entries of `(base_offset: u64, end_exclusive: u64,
certified_length: u64)`. Count is 1..=4,096, entry ranges begin at zero and
are contiguous, last end equals next offset; all sealed segments are nonempty,
and only the final active segment may be empty. Maximum snapshot payload is
98,320 bytes. Snapshot is the first and only type-1 record in an epoch.

Append payload: active base offset u64, previous and new certified byte lengths
u64, previous and new exclusive offsets u64; exactly 40 bytes. The previous
values must match reconstructed state; new values must increase and match
verified whole frames with continuous offsets. A roll payload is exactly 16
bytes: former active base u64, new active base u64. It is valid only when the
former active is nonempty and the new base equals its end; it seals the former
entry and appends an empty active entry. No group spans a roll. Segment file
names use their base offset as 20 zero-padded decimal digits. All segment frame
bytes and checksum coverage remain the existing format.

Candidate checkpoint `journal-checkpoint`, exactly 64 bytes: ASCII `SRJC`
0..4; version 1 at 4; zero reserved at 5..8; log identity 8..24; journal epoch
24..32; certified journal byte length 32..40; last certified record sequence
40..48; derived next offset 48..56; zero reserved 56..60; CRC-32C of 0..60 at
60..64. The identity/epoch must equal the journal header, certified length must
be within the physical file and 16 MiB bound, sequence and next offset must
match reconstructed state, and checkpoint is the sole named authority.
`journal-checkpoint.tmp` is never an authority. Values at or beyond offset
exhaustion are handled consistently with the legacy signed-offset bound.

For an illustrative empty initial state, use identity
`00112233445566778899aabbccddeeff`, epoch zero, snapshot sequence zero and one
empty active entry. The following bytes were calculated with a standalone
bitwise CRC implementation checked against `123456789` = `e3069283`; they
are preliminary vectors, not complete independently reviewed golden fixtures:

```text
journal (112 bytes): 53524a310100000000112233445566778899aabbccddeeff0000000000000000000000286b9ac0e353524a520101000000000048000000000000000000000028ae847cf80e26b8a600000000000000000000000100000000000000000000000000000000000000000000000000000000
checkpoint (64 bytes): 53524a430100000000112233445566778899aabbccddeeff00000000000000000000000000000070000000000000000000000000000000000000000019bcee15
```

The initial empty segment is a zero-byte file named
`segment-00000000000000000000.log`; the directory also requires its lock file.
Full multi-segment, migration and malformed golden fixtures and independent
verification have not been produced. The proposed layouts are not proof of
correctness and must not be implemented until independently reviewed and accepted.

The separate [Rust review test](../../tests/adr024_journal_vectors_review.rs)
reads these documented bytes and checks field sizes and CRCs using the existing
CRC library. It was authored alongside the proposal and is not an independent
review of the protocol or a test of journal recovery.

## Candidate protocol and state table

All operations use the existing directory/lock-file cooperative locking model,
directory-relative no-follow opens and regular singly linked storage files.
An existing old log remains readable via its old protocol. New layout names
must cause actual old binaries to refuse without modification. A clean new
directory initializes an empty segment, journal header and empty snapshot;
sync each file and the relevant directory entries, then install a synced
checkpoint with rename and directory sync before returning a usable log.
Incomplete initialization with artifacts refuses and preserves evidence.

For a non-roll append: validate bounds and resources before mutation; append
whole group to active segment and sync it; append one commit record to journal
and sync it; write and sync a new checkpoint temporary, rename over checkpoint,
and sync directory; publish offsets and ACK only after all barriers. Any I/O
failure after mutation starts latches terminal. A successful ACK requires both
the data and its certified journal record to be durable; an error around
publication can have an unknown outcome. No CRC-valid newer journal tail or
temporary checkpoint is promoted merely because it appears complete.

| Observation on recovery | Candidate result |
| --- | --- |
| Missing, invalid, unsupported or inconsistent checkpoint or journal header | Refuse and preserve all artifacts; never manufacture a checkpoint. |
| `journal-checkpoint.tmp`, extra segment or unexpected file | Refuse and preserve; no generation or filename authorizes promotion/deletion. |
| Journal shorter than checkpoint's certified length, record corruption within that range, sequence/state mismatch or missing certified segment | Refuse; never truncate certified data or silently roll back. |
| Fully validated certified journal and all certified segment frames; no extra artifacts | Reconstruct state; open at the certified next offset. Validate every historical payload, including sealed segments. |
| Journal suffix after certified length and/or active segment suffix after certified length | Only after all certified journal and segment bytes validate and all artifacts classify safely, truncate both uncertified suffixes and sync each changed file; failure of either operation refuses and may leave partial mutation. Define and test the order and restart after each cut before acceptance. |
| Valid older checkpoint and internally coherent older files externally restored | May appear valid; local generation/identity does not detect rollback. |

For the suffix row, the working ordering is: validate the complete certified
journal and every certified segment first; truncate the journal to the
checkpoint's certified length and sync it; then truncate the active segment to
its reconstructed certified length and sync it. Skip a truncation when the
file has no suffix. If either truncate or sync fails, latch/refuse and do not
perform the next mutation; on restart revalidate from the surviving checkpoint
and observed files. A failed truncate or sync may already have changed bytes,
so preservation claims apply to certified ranges and separately saved evidence,
not necessarily to uncertified suffix bytes after mutation has begun. This
ordering is a candidate requiring failure tests, not a proven optimization.

## Rollover and bounded replay: unresolved decisions

For a segment roll, the prospective new empty segment must be exclusively
created, synced and its directory entry synced before a roll record can be
certified. Publish the roll record through the journal/checkpoint barrier before
writing the next group. A crash before publication may leave an orphan; preserve
it and refuse. A crash after checkpoint rename or directory-sync error can
expose old or new authority; validate the actually observed state and preserve
ambiguity. No ACK may depend on topology absent from durable authority.

The 16 MiB journal requires rotation to continue accepting commits. One
candidate is to write a new journal header and complete snapshot of the
certified state to an exclusively created epoch file, sync file and directory,
then atomically publish a checkpoint bound to the new epoch; only then append
new commits. The new snapshot record could use the previous certified sequence
plus one, with checked overflow; the checkpoint must certify exactly the
snapshot. This is a candidate, not proof that the checkpoint/snapshot pair
establishes the required continuity. The prior journal must not be removed
before the new checkpoint's directory sync succeeds.

The central unresolved issue is that an old journal still exists after
publication. The ordinary recovery rule currently refuses unexpected files;
therefore a rotation that simply leaves the old file in place would make the
partition refuse on its next open. Conversely, treating a filename that looks
like an older epoch as disposable could erase evidence of a different crash
state or an operator error. Neither automatic acceptance nor deletion of that
file follows from its epoch alone.

### Established approaches and the decision they leave us

Inspected 2026-11-13. [RocksDB's MANIFEST design](https://github.com/facebook/rocksdb/wiki/MANIFEST)
describes writing a new metadata snapshot, updating and syncing the `CURRENT`
pointer, and purging redundant manifests after successful publication.
[LevelDB's implementation notes](https://github.com/google/leveldb/blob/main/doc/impl.md#recovery)
describe recovery through `CURRENT` and cleanup of files considered obsolete.
These are metadata lifecycle examples, not evidence that their exact cleanup or
WAL durability assumptions satisfy Serapendiya's acknowledged certified-range
contract. In particular, deleting a metadata journal is distinct from retention
deletion of acknowledged event segments, but an incorrectly classified journal
could still erase evidence needed to recover those events.

The practical design question is whether a replacement checkpoint should
**explicitly identify one retired journal** as part of the published transition.
For example, it could bind the prior journal's identity, certified length and
content fingerprint to the new snapshot and record a retirement state. Recovery
could then distinguish an explicitly named, validated retired artifact from an
arbitrary earlier-looking file; it must continue to refuse unknown or mismatched
files. Such binding would require a different checkpoint layout from the 64-byte
candidate above, exact fingerprint/identity and collision assumptions, and
rules for whether a missing retired file is permitted after interrupted cleanup.
Publication would have to make the new snapshot and its directory entry durable
before naming the new state; cleanup, if authorized, must occur only after that
authority is durable, with directory sync and tested interruption behavior.
Do not infer that a prior file is safe to delete from its epoch or name alone.

This is an approach to evaluate, not an accepted retirement or deletion
protocol. The packet must still decide whether to keep an explicitly bound old
file, move it to external evidence, or automatically remove it, and must define
every old/new checkpoint and journal combination before seeking acceptance.
Under the current rules, automatic cleanup remains unapproved and ambiguous
artifacts are preserved. No event-retention decision is implied.

| Observed after interruption | Current safe classification; unresolved action |
| --- | --- |
| Old checkpoint, no new journal, no temporary checkpoint | Validate and open the old certified state; retry only under a future reviewed rotation protocol. |
| Old checkpoint and new journal, with or without temporary checkpoint | Refuse and preserve both epochs and any temporary; the new journal is not authority. |
| New checkpoint and matching new journal, old journal also present | Refuse under today's unexpected-file rule. A new classification and operator/cleanup protocol is required to regain availability. |
| New checkpoint and matching new journal, no old journal | Validate the complete new snapshot and all certified segment data. Its absence cannot be treated as proof that cleanup was safely performed or that no coherent rollback occurred. |
| Missing, invalid or mismatched checkpoint/new journal, multiple unexpected epochs, unknown names or failed sync | Refuse and preserve; no sequence or filename proves which transition completed. |

Compare two follow-up approaches before selecting one: (1) explicitly identify
retired journal epochs in durable metadata, preserve them, and refuse before a
reviewed bounded cleanup when the retirement limit is reached; or (2) publish
the new checkpoint then move/copy the old journal to durable external evidence
under an explicitly reviewed operator action, with restart rules for each
interruption. Both need an exact identity/binding scheme, old/new checkpoint
and epoch matrix, no-overwrite operations, bounded disk use, platform-specific
sync assumptions and independent failure tests. Neither approach authorizes
deleting segment data. There is not yet a demonstrated safe automatic or
operator procedure, so this is a **blocking design gap** for a usable journal,
not an implementation-ready decision.

The [explicit-retirement candidate](ADR-024-journal-rotation-candidate.md)
develops one possible checkpoint layout and crash-state table for review. Its
automatic metadata cleanup, CRC binding and platform behavior are not accepted
or proven; this packet's earlier 64-byte checkpoint remains a historical working
candidate rather than the retirement layout.

The provisional 16 MiB bound is operationally significant: starting with the
112-byte empty journal and using 72-byte append records only, it holds at most
233,015 append groups before another record no longer fits. Roll records and
larger snapshots reduce this count. At 1,000 committed groups per second that
is roughly 233 seconds; this is arithmetic, not a measured workload. Until a
safe rotation protocol is accepted and implemented, reaching the limit must
reject the next group before segment or journal mutation. A temporary 16 MiB
stop is not presented as a practical steady-state deployment policy.

Offline migration from the legacy layout must preserve the source, validate
every frame and its old checkpoint, create a distinct destination and compare
all offsets and bytes before explicit operator cutover. Group boundaries from
old appends are not persisted. Journal snapshot/commit construction, destination
capacity preflight, interrupted migration and old-binary refusal need exact
fixtures and failure tests. A migration-shaped single-manifest fixture is not
evidence for this new journal format.

## Validation and decisions still required

- Independently review the candidate authority, exact bytes, length/offset
  arithmetic, old/new format classification, directory safety and sync ordering.
  Generate complete golden fixtures independently and verify with a second
  parser; test unknown types, length overflow, torn headers/payloads and
  corruption before, at and beyond the certified boundary.
- Specify deterministic cases for every file create, write, sync, rename,
  directory sync, roll and both truncations, including short writes and injected
  errors. Test successful ACK recovery and honest unknown outcomes. Test actual
  old-reader refusal, source-preserving migration and concurrent lock behavior.
- Separately run process-crash tests and ext4/XFS block/VM power-loss replay
  against an authorized implementation; simulated I/O faults do not establish
  physical durability. Prior replay applies to the previous format only.
- Matched durability benchmarks must compare this journal with the current
  single-segment baseline and the manifest/checkpoint alternatives at increasing
  segment and journal sizes, including commit sync counts, bytes, p50/p99/p99.9,
  throughput, CPU, RSS, rotation pauses and full-scan startup. Record hardware,
  filesystem, compiler, sample counts and raw results. No candidate benchmark
  exists. Revisit the founder's direction based on these results.
- Human acceptance is still needed for the exact format, rollover, recovery and
  migration before implementation. Retention, operator mutation and the
  independent-request coordinator remain separate decisions. G1 acceptance is
  separate from this selection and from passing tests.
