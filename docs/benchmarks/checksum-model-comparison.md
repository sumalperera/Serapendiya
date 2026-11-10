# Experimental checksum coverage comparison

Date: 2026-11-11. This is a checksum-only microbenchmark, not an append,
fetch, recovery, replication, filesystem or durability benchmark. The current
on-disk format remains unchanged and no alternative is accepted by this work.

## Method

The ignored release-mode test `measure_checksum_coverage_models` uses the
production bitwise CRC32C implementation on a pre-encoded batch, avoiding
new dependencies. It compares:

- **Full model:** CRC32C of the existing 28-byte container-header portion,
  the full encoded batch, and the batch's CRC-covered suffix. This models the
  three checksum calculations in the existing format, not all of append or
  recovery.
- **Split model:** CRC32C of a synthetic 73-byte metadata input and the batch's
  CRC-covered suffix. The synthetic input contains a previous checksum, entry
  index, byte position, offset, 24 bytes of existing header fields and the
  21-byte batch prefix. It approximates chained metadata coverage. Its bytes,
  field duplication, endianness and initial state are **measurement choices**,
  not an approved or complete on-disk format.

Both models checksum the body once for the batch CRC; the full model reads the
whole batch once more. The split model computes a new chain value per iteration.
The benchmark alternates execution order, uses five samples and processes about
8 MiB of batch data per sample (at least eight batches). It runs in-process with
hot reused buffers. It does not measure construction, validation, disk reads,
checkpoint sync, fsync, allocation, network, compression, concurrency, p99 or
actual client/server behavior. Results should not be used to predict end-to-end
throughput or compare products.

Command:

```sh
cargo test --release --locked --lib measure_checksum_coverage_models -- --ignored --nocapture
```

Environment: macOS ARM64 laptop, Rust `1.99.1 (58cf8f9c3 2026-10-13)`,
source based on `aad31c8` with this benchmark added; lockfile SHA-256
`345c528fb99f919920404d013f088290f4f39d03e2470f3c91c60245a8c1909c`.
No CPU affinity or system isolation was used. Full raw output is in
[checksum-model-macos-raw.txt](checksum-model-macos-raw.txt).

| Value size | Full median ns/batch | Split median ns/batch | Full / split |
| ---: | ---: | ---: | ---: |
| 32 B | 742.5 | 548.4 | 1.35 |
| 1 KiB | 8,190.1 | 4,224.5 | 1.94 |
| 64 KiB | 487,364.2 | 243,494.4 | 2.00 |
| 1 MiB | 7,850,697.9 | 3,888,635.4 | 2.02 |

The model suggests the second full-payload pass matters for this software CRC
implementation on this machine. It does not establish that a real split format
would achieve the same improvement, or that checksums are a bottleneck once
I/O and sync are included. Hardware CRC implementations and other workloads
could materially change the result.

## Integrity review

The existing format checks the container header, the complete stored batch,
and the batch CRC-covered region. The model checks metadata and the batch's
first 21 bytes separately from that CRC-covered region. A unit test flips one
bit at each covered byte position and checks detection; it also exercises
changes to the previous chain state, entry index and position. These are
examples of accidental fault detection, not proof that the alternatives detect
identical corruption patterns. A checksum can collide, CRC is not a MAC, and
the extra full-batch CRC may detect some patterns that escape the other checks.
Neither design alone detects rollback to a valid older checkpoint or establishes
that data was synced.

Before deciding ADR-003, specify the alternative's exact layout, byte order,
length limits, chain construction, segment identity and recovery rules; add
golden vectors, corruption and interrupted-write tests; and benchmark a real
implementation on Linux with representative batch sizes and storage. Compare
append, recovery and fetch under the agreed durability and verification policy.
Changing the experimental format also requires a version and explicit handling
of existing data. Until then, keep the existing format and its checks.
