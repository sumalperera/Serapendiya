# Proposed G1 storage inspection procedure

**Status: draft for review.** The selected G1 scope calls for preserving and
inspecting an offline copy first; this procedure still requires review and
operational testing. There is no supported repair or migration tool for these
states. The procedure preserves evidence; it is not permission to
repair, truncate, delete files or manufacture a checkpoint. A terminal append
error can mean the append was persisted, so avoid automatic retries.

1. Stop all processes that may access the affected log and prevent them from
   restarting. Do not remove `.lock`: advisory ownership ends when the owner
   exits, and replacing the lock file can defeat coordination.
2. Record the exact path, error, software version, relevant timestamps and
   whether any append may have returned success or an unknown outcome. Avoid
   recording payloads or secrets in shared incident notes.
3. Make a verified, offline copy using an operator-approved backup process that
   preserves file names, contents and relevant metadata. Keep the original and
   at least one untouched copy. Verify the copy against the source while the
   log is offline; record how verification was performed. The current project
   does not provide a tool that proves a copy is complete or a checkpoint is
   the newest one.
4. Inspect a copy, not the original. Record which of `.lock`, `checkpoint`,
   `checkpoint.tmp`, `segment-000000.log` and unexpected files exist; their
   sizes and cryptographic hashes can help identify later changes, but do not
   establish which bytes were acknowledged or whether a checkpoint is fresh.
   Do not follow unexpected symlinks. If checkpoint rollback is suspected,
   **do not open the original log**: normal recovery can truncate bytes beyond
   an older valid checkpoint.
5. A missing or invalid checkpoint, missing certified segment, unknown entry,
   or temporary checkpoint needs investigation against trusted backups and
   application records. Keep the state unchanged until a separately reviewed
   recovery action is selected. A valid checkpoint plus `checkpoint.tmp` can
   open for reads, but opening can truncate an uncertified tail and appends stay
   blocked; do that only on an expendable copy.
6. If the data is explicitly confirmed disposable, the operator may choose a
   **new empty directory at a separate path** for a new log. This does not
   recover the old log, establish continuity, or resolve uncertain appends.
   Keep the original until its disposition is explicitly decided.

For durable or valuable data, involve someone with storage recovery expertise.
The project currently has no documented automatic procedure for proving the
newest checkpoint, reconstructing one, or merging an uncertain append.
