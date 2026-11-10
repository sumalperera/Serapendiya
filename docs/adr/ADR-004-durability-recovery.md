# ADR-004: Durability and recovery

Status: **ACCEPTED**

The first storage slice treats a successfully synced segment plus an atomically
replaced, checksummed checkpoint as the certified durable boundary. The
checkpoint records certified bytes, next offset, and generation. Its temporary
file is synced before rename, and the parent directory is synced after rename.
On recovery, corruption inside the certified range is fatal; bytes after the
certified range are truncated only after the certified prefix validates.

The supported crash model is initially Linux ext4/XFS, with equivalent local
filesystem behaviour required before broader claims. Sync failures, checkpoint
errors, and directory-sync errors return errors and never report a successful
durable append. Group commit and fault-injection coverage remain follow-up G1
work.

Approval record: approved by founder in the project conversation on 2026-11-07.

## Current implementation review

The accepted ordering above does not mean an append that returns an error is
absent: after checkpoint rename, the outcome may remain unknown until recovery.
The experimental initialization and directory-classification protocol has not
been accepted. See [ADR-004 initialization proposal](ADR-004-initialization-proposal.md)
and [the ADR reconciliation review](../verification/G1-adr-reconciliation.md)
for current behavior, rollback assumptions and outstanding decisions.
