# Proposed amendment to ADR-004: initialize the durable boundary

Status: **PROPOSED — experimental empty-checkpoint initialization merged in
PR #13; FORMAT marker and non-mutating classification are not implemented.**

The 2026-11-09 founder instruction authorized proceeding with the storage work.
The implementation initializes an empty checkpoint before accepting writes;
the full protocol below remains unaccepted. Selected simulated and ext4/XFS
replay evidence is now available; it is not a complete power-loss matrix. The
proposed FORMAT marker remains unimplemented. Directory-descriptor locking was
added later, but does not implement the full proposed classification protocol;
see `ADR-003-checksum-and-recovery-review.md`.
The prototype writes experimental checkpoint version 2, distinct from
the legacy version 1, before accepting writes. A valid empty version-2 checkpoint
allows an uncertified partial first frame to be truncated; a version-1 checkpoint
is rejected without truncating the segment. Regression tests cover a one-byte
partial first append and legacy-checkpoint refusal. This is not the proposed
FORMAT-marker or no-mutation-on-refusal protocol, and it has not been verified
against every power-loss outcome or accepted as a stable format.
If initialization fails with `checkpoint.tmp` present but no durable
checkpoint, the prototype refuses to reopen instead of overwriting the
ambiguous temporary file. An operator procedure and an identity-based safe
restart protocol remain to be designed.

An append returning a terminal I/O error has an **unknown outcome**: after a
checkpoint rename, even a directory-sync failure can leave the batch visible
upon reopening. Callers must reconcile or implement an idempotent operation
protocol before retrying. An error does not mean the write is absent.

## Problem

A nonempty segment with no checkpoint is ambiguous: it might be the tail of an
interrupted first append, or it might contain previously acknowledged records
whose checkpoint is missing. Automatically truncating it can destroy acknowledged
data. The prototype fails closed if the checkpoint is missing. Its initialized
version-2 checkpoint permits recovery from tested first-append interruptions,
but initialization failures with ambiguous temporary files still fail closed.

## Proposed protocol

1. Acquire the exclusive data-directory lock before examining or changing log
   files. Preserve the lock file while any process may have the log open.
2. Only when the segment is absent or empty and no checkpoint exists, establish
   a new empty segment and a checkpoint with certified length zero,
   next offset zero and generation zero. Sync the segment, sync the checkpoint
   temporary file, rename it, then sync the containing directory before making
   the log available for append. Durably create any new parent directory entries
   using a reviewed directory-sync order.
3. If the checkpoint is missing and the segment is nonempty, fail closed and
   preserve every byte. Do not infer prior acknowledgement or truncate. Provide
   an explicit operator recovery procedure separately; never silently initialize.
4. After a valid empty checkpoint is durably committed, use the existing append
   protocol. An interrupted first append may leave a tail beyond the checkpoint;
   validate the certified prefix and truncate only the uncertified tail.
5. Legacy version-1 checkpoints and segments are refused without truncation;
   they require a separately approved migration or operator procedure. An
   existing empty segment with no checkpoint is a proposed restart case, not
   current behavior. The current code refuses any nonempty directory lacking a
   checkpoint unless it just created the lock as part of this initialization.
   The proposal needs an identity proof before safe automatic restart can be
   accepted; a nonempty segment without a checkpoint remains ambiguous.

The experimental checkpoint version changes from 1 to 2, and the segment format
changes. If all durable metadata and data disappear together,
an empty directory cannot prove that the log was previously initialized; the
protocol does not claim detection of complete external deletion or storage loss.

## Before acceptance

- Review the state transitions and directory sync ordering for the supported
  Linux ext4/XFS configurations, including initialization after interruption.
- Add failure injection at every initialization write, sync and rename; verify
  no append succeeds before initialization is durable.
- Test first-append interruption at each boundary, a missing checkpoint after
  an acknowledged append, existing empty logs and legacy valid checkpoints.
- Test process interruption and storage/power-loss behavior separately; a
  process abort alone does not prove filesystem crash consistency.
- Decide and document operator recovery for ambiguous and externally damaged
  directories, and whether automatic ancestor creation is supported.

The complete initialization and classification protocol remains proposed; its
format and acceptance requirements require review before they are treated as a
stable durability contract.

## Implementation reconciliation (2026-11-10)

This remains a proposal. The current code holds a directory descriptor and
advisory locks, requires an existing parent when creating the log directory,
and installs a version-2 empty checkpoint before returning from a new open.
It refuses an existing lock or segment without a checkpoint, even if the
segment is empty; it preserves an existing `checkpoint.tmp` and blocks new
appends until inspection. These are stricter than step 5 above. The directory
lock and no-follow operations are not protection against privileged or
non-cooperating modification, and the FORMAT-marker policy is not implemented.
See [the ADR reconciliation review](../verification/G1-adr-reconciliation.md)
for evidence and outstanding decisions.
