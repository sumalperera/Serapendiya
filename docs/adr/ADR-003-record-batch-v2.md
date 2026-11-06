# ADR-003: RecordBatch v2 storage design

Status: **ACCEPTED**

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
