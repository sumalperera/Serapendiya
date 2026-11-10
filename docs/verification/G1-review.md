# G1 storage and recovery review

Review date: 2026-11-08. Reviewed repository commit
`0d89708a8eb804432c004d4a67c4a4c22bc74860` against blueprint sections 4 and 8,
ADR-003 and ADR-004. Verdict: **G1 acceptance is not established**. The issues
below require resolution and review before durability claims or G1 acceptance.

## Current evidence and remaining work (2026-11-10)

The original review below is historical: its file references and descriptions
refer to the reviewed 2026-11-08 commit, not the current tree. As of merged
commit `77f76ea`, subsequent work added descriptor-relative directory handling,
lifetime locking, a durable empty checkpoint before the first append, bounded
groups and signed offset checks. The Rust 1.85 minimum is tested in [CI for 77f76ea](https://github.com/sumalperera/Serapendiya/actions/runs/42758511229), and
independent kafka-python fixtures cover the current narrow batch shape.

The evidence now includes actual ENOSPC and block-error tests on disposable
ext4, plus `dm-log-writes` replay of initialization, first append and later
append boundaries. Both normal ext4 journal recovery and the separately
replayed fsck path preserved the records asserted by the harness. See
[G1-init-replay.md](G1-init-replay.md) and
[G1-ext4-mount-comparison.md](G1-ext4-mount-comparison.md).

A subsequent [XFS replay](G1-xfs-replay.md) exercised the same 22 marks
through normal XFS journal recovery; it does not add an explicit pre-mount
repair comparison.

G1 acceptance remains open. The replay covers selected boundaries and one
virtual-disk storage model; it does not cover every initialization syscall,
write reordering, torn sector, physical device or sustained failure. Explicit
operator recovery for ambiguous initialization and stale temporary checkpoints,
format and initialization ADR review, and independent review remain open.
The current storage is Unix-only. A format-stability or production durability
claim must not be inferred from these tests.

## Historical remediation recorded on 2026-11-08

The working tree adds an advisory exclusive lock, rejects a missing checkpoint
beside a nonempty segment, syncs directory ancestors on open, enforces limits
of 1,024 batches and 16 MiB per group, and rejects offsets outside the signed
64-bit frame range. Regression tests cover concurrent handles and processes,
missing checkpoints, group limits and offsets. The lock file must not be
unlinked while a log is open. Non-Unix targets fail closed because this lock
implementation is Unix-only.

On 2026-11-08, `cargo test --workspace --locked`, `cargo fmt --all --check`,
and `cargo clippy --workspace --all-targets --locked -- -D warnings` each
exited 0 locally. `docker build --progress plain -f Dockerfile.g1-test -t
serapendiya-g1-review-fix .` exited 0 on Linux aarch64 and ran the locked
build, fmt, Clippy and workspace tests. This verified uncommitted changes based
on the review commit above; no commit SHA or CI run exists for those changes.
The temporary image was removed after the run.

Failing closed on a nonempty segment without a checkpoint also means a crash
during the first append, before the first checkpoint is committed, requires
operator intervention. A reviewed initialization protocol is needed to
distinguish that case from lost certification metadata. Ancestor sync ordering
and the new locking behavior still require review and crash testing on the
supported filesystems. The acceptance evidence gaps below remain open.

## Original findings

1. **Critical: no exclusive ownership of a data directory.** `Log::open_with_io`
   opens the segment without a directory lock. Two handles can start with the
   same offset and certified length, then overwrite each other's data and
   checkpoint. A local macOS reproduction opened two handles, appended
   `first` and `other`, and both calls successfully returned offset zero;
   reopening returned `other` at offset zero. Thus an acknowledged append was
   lost. Add an exclusive lock held for the complete lifetime of the log and
   test independent processes as well as multiple handles. See
   `src/storage.rs` lines 127–147 and 226–245.
2. **Critical: missing checkpoint is treated as a new log.** When the
   checkpoint is absent, recovery assumes a zero-length certified range and
   truncates an existing nonempty segment. In a local macOS reproduction,
   removing the checkpoint after a successful append caused reopening to
   succeed and truncate the segment to zero bytes. A missing checkpoint can
   also be observed if metadata is lost or removed; the code cannot safely
   infer that all existing bytes were unacknowledged. Distinguish provably new
   initialization from missing certification metadata, fail closed when the
   distinction cannot be proven, and test both cases and crash boundaries.
   See `src/storage.rs` lines 130–157.
3. **High: new data-directory durability is not established.** The code may
   create a directory with `create_dir_all`, but only syncs the log directory
   after checkpoint rename. It does not sync the parent directories in which
   newly created directory entries reside. A successful append therefore does
   not establish that a newly created data directory survives power loss under
   the stated model. Define and review initialization and parent-directory
   sync ordering, or require a pre-existing durably created directory, then
   test the supported procedure. See `src/storage.rs` line 129 and
   `write_checkpoint`.
4. **Medium: group commits are not bounded.** `append_group` accepts an
   arbitrary number of batches, encodes all frames and copies them into a
   second buffer without a group byte or count limit. This can exhaust memory
   and does not substantiate the bounded-group claim in `G1.md`. Establish
   explicit limits and tests. See `src/storage.rs` lines 184–220.
5. **Medium: signed offset range is not enforced.** Domain offsets are `u64`,
   but `encode_batch` casts them to `i64` while recovery rejects negative base
   offsets. An offset above `i64::MAX` would be written as negative and could
   not be recovered. Reject overflow before writing and test the boundary. See
   `src/storage.rs` lines 203–212, 336–338 and 411–417.

## Historical acceptance evidence gaps

- The expanded process-abort test covers six append/checkpoint boundaries, but
  does not exercise sustained repeated failures or
  establish behavior under actual power loss. The fault tests inject I/O
  errors, but do not show explicit ENOSPC tests or the full recovery matrix.
- The CI Docker job passes, but does not establish an ext4 and XFS matrix or
  identify and verify the underlying filesystem. Process termination is not a
  substitute for persistence testing under the documented crash model.
- The batch payload is opaque, with record count set to one. A Kafka-compatible
  record body and independent decoder fixture are not established. ADR-003
  expressly leaves independent fixtures for follow-up; do not claim Kafka
  record compatibility on the current evidence.
- `G1.md` does not identify its tested commit or link a specific CI run, and
  calls the group commit bounded despite the absence of an enforced bound.

## Existing evidence

The [CI run for commit 0d89708](https://github.com/sumalperera/Serapendiya/actions/runs/42463039542)
passed the G0 job and Linux Docker G1 job. Existing tests cover certified-prefix
corruption, tail truncation, checkpoint corruption, injected I/O failures,
index rebuild and one process abort/restart scenario. They are useful evidence,
but do not cover the findings or prove the complete acceptance criteria.

The two data-loss reproductions above used a temporary standalone Rust program
linked to this checkout, run with `cargo run --offline` on macOS. Both calls
returned success; their reported values were `Offset(0)`, `Offset(0)`, recovered
bytes `[111, 116, 104, 101, 114]`, and, after checkpoint deletion, certified
bytes `0` and segment bytes `0`. No persistent format or production code was
changed during this review.
