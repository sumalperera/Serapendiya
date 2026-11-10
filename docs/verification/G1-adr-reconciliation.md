# ADR-003/004 implementation reconciliation

Reviewed on 2026-11-10 against merged commit
`9e02d40` (PR #21). This is an implementation and evidence review, not an
independent external audit, ADR acceptance, or G1 acceptance.

## Findings and decisions still needed

| Priority | Finding | Consequence / next action |
| --- | --- | --- |
| High | The original accepted ADR-003 describes Kafka-shaped frames with opaque record bytes. The current log writes a 32-byte `SRB1` container, a full-batch container CRC and a narrow encoded single-record Kafka-shaped batch; the amendment is proposed. | Reconcile and explicitly decide the persistent format, checksum coverage, future versions and migration before calling the format stable. Do not treat the older accepted text as approval of the experimental layout. |
| High | The proposed ADR-004 initialization steps previously allowed an existing empty segment with no checkpoint to be treated as new. The implementation refuses an existing `.lock` or segment without a checkpoint, including an empty one. | The current behavior is deliberately fail-closed. Keep it until identity, interruption, refusal and operator recovery semantics are designed and reviewed; the proposal now identifies the mismatch. |
| High | The current protocol cannot detect a valid older checkpoint being restored or rolled back together with a matching old certified prefix. Its generation field is stored and incremented but has no independent authority to detect rollback or fence an owner. | Specify the failure/threat model and whether checkpoint rollback is in scope. A self-contained generation alone is not fencing; do not claim it detects stale valid metadata or prevents truncation of an acknowledged suffix in that scenario. |
| Medium | A stale `checkpoint.tmp` beside a valid checkpoint permits recovery/read of the certified prefix but blocks subsequent appends. A failed initialization without a checkpoint also refuses restart. | Provide reviewed operator inspection and recovery guidance without silently deleting ambiguous files. For a terminal write error, the append outcome can be unknown and retries can duplicate data. |
| Medium | The pinned `replay-log` tool's replay mode exits successfully on reaching the end of the log without finding an end mark. Earlier VM runs did not separately check mark existence; some observed classifications and acknowledged-record assertions provide evidence, but the captured output does not independently prove every requested mark existed. | The harness now checks each mark with `--find --end-mark`. Guarded reruns passed on ext4 and XFS; a nonexistent-mark check returned an error. The earlier raw output remains historical. |
| Medium | `G1.md` described the original G1 work as complete despite unverified commit metadata and the later experimental format; older ADR review text describes path operations before PR #15. | Those records are now labelled historical and linked to current status; do not infer G1 acceptance from the old closeout. |

## What the code does today

- A Unix-only local log uses an open directory descriptor, directory and `.lock`
  advisory locks, no-follow child opens and singly linked regular files. The
  data directory's parent must exist. These measures assume cooperative actors
  and do not prevent a privileged or non-cooperating process from changing an
  open file or an ancestor during initial path lookup.
- New initialization syncs an empty segment and installs a version-2 empty
  checkpoint before returning a usable log. Existing ambiguous directories fail
  closed; a valid checkpoint and segment are required to recover existing data.
- For each nonempty group, the implementation writes and syncs the segment,
  writes and syncs an exclusively created checkpoint temporary file, renames it,
  then syncs the held directory. It advances the in-memory certified, visible
  and durable positions only after success. I/O errors during the commit put
  writes into a terminal state; an error around rename/directory sync can have
  an unknown persisted outcome. There is no replicated or quorum commitment.
- Recovery validates the certified prefix, including container header and
  payload CRCs, Kafka batch CRC, supported record shape and contiguous offsets,
  before truncating an uncertified tail. A CRC is accidental-error detection,
  not authentication or protection against a consistent rollback. An index is
  rebuilt from the complete certified prefix.
- The current encoder accepts opaque values, not arbitrary Kafka batches. It
  produces one uncompressed, nontransactional record per batch; kafka-python
  fixtures verify two encoded examples. Kafka wire compatibility, broad batch
  support, format migration, retention and replication are not implemented.

## Evidence and limits

The original storage review remains in [G1-review.md](G1-review.md). The
[initialization replay](G1-init-replay.md), [ext4 comparison](G1-ext4-mount-comparison.md)
and [XFS replay](G1-xfs-replay.md) cover selected boundaries on virtual disks;
they are not actual power cuts or an exhaustive torn-write, ordering, device or
filesystem matrix. Prior runs used the earlier harness without an independent
mark-existence check, as noted above. Guarded reruns on 2026-11-10 used Ubuntu
24.04 ARM64, kernel `6.8.0-147-generic`, Rust 1.85.0 and replay-log commit
`7b70d8a6863c5de30933d42a7672d35d01d2dc6c`. Ext4 passed 44 replay checks;
XFS passed 22. Asking find mode for `definitely-not-a-real-mark` returned
nonzero. Raw output is in [ext4](G1-guarded-replay-ext4-output.txt) and
[XFS](G1-guarded-replay-xfs-output.txt); the original output is retained.
The physical-ENOSPC and block-error run is in [G1-block-device-tests.md](G1-block-device-tests.md).

Before acceptance: decide ADR-003 format and migration, ADR-004 initialization
and stale-checkpoint assumptions, operator recovery and error semantics; expand initialization
syscall, crash and recovery coverage as required by the chosen contract; and
obtain independent review. This document does not authorize G2.
