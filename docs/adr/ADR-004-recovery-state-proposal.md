# ADR-004: initialization and recovery state proposal

Status: **G1 recovery scope selected; full protocol and format remain
PROPOSED.** The founder instructed the project to proceed with the scope below
in the project conversation. This selects the G1 boundary; it does not accept
G1, approve a stable format, or authorize an automatic recovery mechanism.
This document distinguishes current behavior from future decisions. It
supplements [ADR-004](ADR-004-durability-recovery.md), the
[initialization proposal](ADR-004-initialization-proposal.md), and the
[implementation reconciliation](../verification/G1-adr-reconciliation.md).
The table describes the code at merged commit `1622fda` under its supported
cooperative-locking assumptions; it is not a claim that arbitrary untrusted
processes cannot change an open directory.

## Current opening states

Classification and recovery occur with the directory descriptor and advisory
locks held. A valid checkpoint means a supported, checksum-valid checkpoint
whose certified prefix and next offset also validate against the segment.
`checkpoint.tmp` in the table is a recognized, singly linked regular file;
other entry types or unknown entries cause refusal. The parent of a newly
created data directory must already exist.

| Observed state | Current result | Mutation and limit |
| --- | --- | --- |
| Missing data directory with existing accessible parent, or empty directory | Initialize and install an empty version-2 checkpoint before returning a usable log. | Creates the directory if missing and creates the lock, segment and checkpoint; initialization failures can leave files requiring inspection. |
| Nonempty directory with no `.lock` | Refuse. | No new lock is created. The state could be foreign, damaged or incomplete. |
| Existing `.lock` and no checkpoint, with or without a segment or temporary checkpoint | Refuse. | Does not treat even an empty segment as proof of a new log; preserve the existing files. |
| Valid checkpoint and segment, no temporary checkpoint | Validate the certified prefix, then open. | An uncertified suffix is truncated and the segment synced after prefix validation. Opening is not read-only. |
| Valid checkpoint and segment, recognized `checkpoint.tmp` | Validate and open as above; appends return an error while the temporary exists. | Recovery may truncate an uncertified suffix. The temporary is not automatically deleted or overwritten. |
| Checkpoint exists but is unsupported, malformed or fails checksum | Refuse. | It cannot authorize truncation. No migration or repair is automatic. |
| Valid checkpoint but missing, too-short or invalid certified segment | Refuse. | Do not truncate or recreate the certified segment; preserve evidence. Ordinary I/O failures are not by themselves proof of corruption. |
| Unknown entry, symlink, unsupported file type or multiply linked opened storage file | Refuse. | Do not follow a final symlink or overwrite the unsafe entry. The protocol does not defend against a privileged or non-cooperating actor. |
| Another cooperating owner holds the lock, or locking is unsupported | Refuse. | The caller must not delete or replace `.lock` to bypass ownership. |

A valid checkpoint can be older than the real last acknowledged checkpoint.
The current implementation cannot distinguish a consistent rollback of that
checkpoint from a legitimate older certified boundary. If the segment still
has later bytes, opening may truncate them as an uncertified suffix. In
particular, a successful checksum or incrementing `generation` does not prove
freshness. Preserve a copy before opening when stale metadata is suspected.

## Selected G1 recovery scope

1. **Ordinary crash recovery:** on a documented local filesystem and storage
   stack that correctly honors successful sync and rename operations, a
   successful append must remain recoverable after the modeled interruption.
   Once a commit may have begun, a failed or timed-out append has an unknown
   outcome; callers reconcile before retrying. A rejection known to occur
   before any write is distinguishable. An acknowledgement is local, not
   replicated.
2. **Ambiguous initialization:** retain the current refusal until a reviewed
   identity and initialization protocol can distinguish interrupted creation
   from lost certification metadata. A filename, empty segment, stale lock,
   empty directory or `FORMAT` marker alone does not prove no acknowledged
   data existed. No automatic deletion or reset is authorized by this decision.
3. **Rollback outside the basic crash model:** restoring an older valid
   checkpoint, coherent disk snapshot rollback, external deletion or a storage
   stack that falsely reports successful sync can defeat local freshness checks.
   The current implementation cannot detect these cases. Detection of
   externally restored or rolled-back valid checkpoints is deferred beyond G1.
   If it is needed later, design an independently durable authority and its own
   recovery, availability and fencing rules; the local generation alone is not
   a fence. This boundary is not a claim about all physical devices.
4. **Operator action:** preserve and inspect an offline copy first. The
   [draft procedure](../operations/G1-storage-inspection-proposal.md)
   does not authorize truncation, migration, deletion or manufacture of a new
   checkpoint. A new empty log can be created at a separate path only when the
   operator explicitly accepts that it is a new identity and reconciles any
   potentially acknowledged data.
5. **Future format:** decide whether a versioned identity marker, initialization
   phases and a recovery tool are warranted. Define their exact bytes, sync
   order, handling of prior data, upgrades, lock rules and failure states before
   implementing them. A marker alone cannot establish freshness or rule out
   loss of later records.

## Review and evidence needed before acceptance

- Review the selected crash, storage, rollback and adversary assumptions and
  the table against the implementation and supported filesystems. The selected
  scope does not waive verification of in-scope crash behavior.
- Specify each initialization and directory operation's failure states,
  including the interval before the first checkpoint, and test uninstrumented
  syscall boundaries and concurrent openers.
- Define and test a practical operator workflow with real damaged and
  checkpoint-less copies; retain originals and record every authorized action.
- Revisit the unstable container and checksum proposal with ADR-003; specify
  migration and downgrade behavior before stable-format claims.
- Obtain independent review. Existing ext4/XFS replay and injected failures
  cover selected cases, not every physical power loss, torn write or rollback.
