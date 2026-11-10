# Proposed amendment to ADR-003: versioned internal batch container

Status: **PROPOSED — experimental implementation in progress; not accepted.**

**Review update:** The split-coverage checksum and versioning alternative in
[`ADR-003-checksum-and-recovery-review.md`](ADR-003-checksum-and-recovery-review.md)
is now under consideration. Neither checksum layout is accepted or stable. The
prototype merged in PR #13 implements the full-payload-checksum candidate below, not the
new alternative. Do not claim an operator-safe legacy classification or
production-readiness for either design.

## Direction and scope

The proposed design uses a Serapendiya-owned, versioned storage container
with Kafka-compatible record semantics at the protocol boundary. The direction
does not establish Kafka compatibility or approve replication, queues, Kafka
networking, or other future gates.

The existing ADR-003 remains accepted for the prototype. This amendment must be
reviewed before this experimental persistent format is declared stable.
ADR-004 and the proposed initialization amendment must be reviewed together
with this proposal. The format and test decisions below are a concrete proposal,
not a statement of completed implementation.

## Proposed version 1 container

All multibyte integers are unsigned big-endian unless otherwise stated. Each
frame consists of a fixed 32-byte header followed by one complete Kafka
RecordBatch v2, including its Kafka batch header and record bytes.

| Bytes | Field | Version 1 rule |
| --- | --- | --- |
| 0–3 | Magic | ASCII `SRB1` |
| 4 | Container version | `1` |
| 5 | Batch type | `0`: Kafka data batch |
| 6–7 | Flags | `0`; reject unknown flags |
| 8–11 | Payload length | Length in bytes of the complete Kafka batch |
| 12–19 | Base offset | Signed 64-bit nonnegative Kafka offset |
| 20–23 | Record count | Exactly `1` for the initial supported subset |
| 24–27 | Payload CRC32C | CRC32C of the entire Kafka batch payload |
| 28–31 | Header CRC32C | CRC32C of header bytes 0–27 |

The initial experimental append API encodes a single uncompressed,
nontransactional, non-idempotent record with zero relative offset, null key,
non-null value, zero timestamp delta and no headers. It does not yet accept
Kafka-produced batches or support arbitrary keys, timestamps, nullable values or
headers. Those inputs must eventually be validated against a pinned Kafka
specification; unsupported inputs must fail before any write. Opaque application
values are allowed, but the surrounding Kafka record must be valid.
The container base offset must equal the payload batch's base offset, and both
are included in integrity checks. Reject a mismatch. Bounds must be checked before
allocating or reading; group limits include both container and payload bytes.

The payload retains Kafka's own CRC32C rules. The separate payload checksum
additionally covers the full stored Kafka batch including its base offset and
length. The header checksum allows validating length and offset metadata before
trusting them. Checksums detect accidental corruption; they do not establish
durability or protect against deliberate modification. No future internal batch
types, fields, replication semantics, codecs or configuration are implemented in
version 1. Unknown types, versions and flags fail closed.

Authenticated provenance, tenant identity and durable idempotency may place
future requirements on the container's extension mechanism. Their placement and
semantics remain unresolved in the [agent trust-model proposal](ADR-023-agent-trust-model-proposal.md).
The experimental header reserves no fields for them; this proposal does not
authorize identity, tenancy or deduplication code.

A length-prefixed, integrity-covered extension area with explicit ignorable and
critical tags is a candidate for future metadata. Specify canonical encoding,
length bounds, duplicate tags, checksum coverage, unknown-tag handling,
old-reader/old-writer behavior, downgrade and migration before adoption. Merely
adding TLVs does not guarantee future compatibility or permit old writers to
drop critical metadata. No extension area exists in the experimental header.

## Recovery and existing data

Record byte position and next offset in the existing checkpoint remain the
certified boundary; the format transition and initialization ordering require
ADR-004 review. Certified frames must pass container and Kafka validation before
recovery can advance. Invalid certified data is a hard failure. Uncertified data
is handled only according to the accepted recovery protocol.

Existing prototype frames are not valid version 1 containers. Never interpret or
silently truncate them as the new format. Opening an existing nonempty legacy
segment must fail with an actionable error and preserve all bytes until a
separately approved migration or explicit user-directed reset procedure exists.
An empty legacy log may be initialized only under an accepted initialization
protocol. No destructive migration is authorized by this proposal.

## Required proof before declaring the format usable

1. Pin the Kafka specification version and use independently produced Kafka
   fixtures; verify decode/encode, checksums, nullable keys and values, timestamp
   and header handling, malformed lengths and offset invariants.
2. Record exact header bytes, CRC coverage, size limits, versioning and format
   evolution rules; test unknown flags, versions, types and corruption.
3. Prove legacy data is rejected without mutation, including missing or invalid
   checkpoints and interrupted conversion. Decide separately whether migration
   is needed and obtain approval before performing it.
4. Exercise initialization and append failure points, process interruption and
   the approved Linux ext4/XFS crash matrix. Never equate process aborts with
   power-loss verification.
5. Review implementation provenance and licences before incorporating any
   external code, generated material or dependencies.

The founder requested this direction and its implementation on 2026-11-09.
The exact byte layout, Kafka subset, migration and checkpoint interaction remain
experimental and require review; no claim of acceptance is made. The current
implementation builds and decodes the narrow record described above and computes
the checksums. Two fixtures generated independently by kafka-python 3.0.13
match encoded Kafka batches, including a multi-byte length. Kafka batch admission, broader fixtures,
the full crash matrix, and an accepted initialization protocol remain open.
An experimental version-2 empty checkpoint permits recovery of an interrupted
partial first frame; see ADR-004 initialization proposal for its limits.
