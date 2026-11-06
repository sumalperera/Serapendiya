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
- PR09: bounded synchronous group-commit coordination — implemented in the
  current change.
- PR10: reproducible Linux Docker test environment — the next implementation
  step.
- PR11: recovery hardening and certified-boundary matrix — implemented.
- G1 closeout: process-level crash recovery, ADR-003 derived-index decision,
  and consolidated verification — implemented in the current change.
- Next gate: explicit authorization for G2 Kafka compatibility work.

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
