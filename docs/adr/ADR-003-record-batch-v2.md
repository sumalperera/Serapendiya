# ADR-003: RecordBatch v2 storage design

Status: **ACCEPTED for the original prototype**. The current experimental
container differs from this original text; its amendment remains proposed and
the storage format is not accepted as stable. See
[the ADR reconciliation review](../verification/G1-adr-reconciliation.md).

The first storage slice uses one `segment-000000.log` with Kafka RecordBatch v2
header framing, broker-assigned contiguous base offsets, checked signed batch
lengths, and CRC32C over the bytes beginning at `attributes` (excluding base
offset, length, leader epoch, magic, and CRC itself). The storage boundary keeps
the record bytes opaque; admission and record decoding are separate work.

G1 initially accepts one uncompressed, non-transactional batch shape with one
opaque record payload. Compression, producer sequencing, multi-record semantics,
indexes, and independent decoder fixtures remain explicit follow-up decisions;
the implementation rejects unsupported shapes rather than claiming protocol
compatibility.

Approval record: approved by founder in the project conversation on 2026-11-07.

## G1 closeout amendment

For G1, sparse indexes remain derived in memory and are rebuilt from the
certified segment during startup. No persistent index file is introduced. This
avoids adding a second durable format before its framing, checksum, atomic
replacement, rebuild, and migration rules are independently specified. A
persistent index requires a future ADR amendment and approval before code is
added.
