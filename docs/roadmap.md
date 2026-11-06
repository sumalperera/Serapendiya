# Serapendiya delivery plan

## Open-source repository

`https://github.com/sumalperera/Serapendiya`

Completed:

- G0 foundation, CLI skeleton, configuration, domain types, CI, and ADRs.
- First approved G1 storage slice: append log, RecordBatch v2-shaped framing,
  CRC32C, durable checkpoints, and certified-prefix recovery.

Next:

- PR05: fault-injectable I/O and durability-failure tests — implemented in the
  current change.
- PR06: certified batch readers and malformed/contiguous recovery checks — the
  next implementation step.
- PR07: sparse derived segment indexes and index-rebuild tests — implemented in
  the current change.
- PR08: explicit append/visible/durable position tracking — implemented in the
  current change.
- Subsequent G1 work: measured group-commit coordination and durable-index
  persistence decisions.

## Commercial repository

`https://github.com/sumalperera/Serapendiya-commercial`

All paid or proprietary features must be implemented and licensed there. The
public repository must not receive commercial code, proprietary assets,
entitlement checks, paid packaging, or private service credentials.

If an open-source component is required by both tracks, implement it here only
when it is independently useful, MIT-compatible, and explicitly approved as
open source. Commercial integrations should depend on the public track rather
than the public track depending on commercial code.

## Change-control rule

Before starting any feature, classify it as open source or commercial and
record the destination repository. A feature with uncertain classification is
blocked until that decision is made. The two repositories must remain separate
in commits, release artifacts, credentials, and licensing claims.
