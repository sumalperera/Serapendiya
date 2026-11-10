# G1 CRC library integration verification

Date: 2026-11-11. Base: merged main `b6e725d`. The containing commit identifies
the candidate source. Founder authorization: proceed with the proposed CRC
library replacement, compatibility checks and storage measurements.

## Change and contract

Pin `crc32c` 0.6.9 and route the existing one-shot checksum wrapper through it.
All call sites, byte coverage, CRC serialization, container/checkpoint versions,
sync ordering and error handling are unchanged. The former bitwise routine is
retained only as a test oracle. No dependency code is copied into the project.
Licenses, build dependencies, source dispatch and maintenance observations are
recorded in [PROVENANCE.md](../../PROVENANCE.md).

Differential coverage includes empty input, deterministic pseudorandom inputs,
lengths 0–256 plus boundary sizes through 1 MiB, 16 alignments, incremental
updates and zero/0xff buffers. Existing independent kafka-python fixtures still
pass. Container header/payload checks and a persisted checkpoint are compared
with the old algorithm. Existing corruption, injected I/O/ENOSPC, simulated
power-cut and process-abort recovery tests pass. These tests do not constitute
an exhaustive proof or an audit of the dependency's unsafe hardware code.

## Commands and evidence

On ARM64 macOS 26.6.2, Rust 1.99.1:

Cargo.lock SHA-256:
`5e6c8a53fd1086884074f5c9b846a214b0a01c3d2857afc850e1fd41081ee504`.

```text
cargo check                                                     exit 0
cargo fmt --all --check                                          exit 0
cargo clippy --locked --workspace --all-targets -- -D warnings     exit 0
cargo test --locked --workspace                                  exit 0
cargo +1.85.0 test --locked --workspace                           exit 0
git diff --check                                                exit 0
```

The library suite has 49 passing tests and one ignored manual experiment;
the architecture and seven process-crash tests also pass. Destructive
block-device/VM tests and the manual benchmark are ignored in ordinary runs.
Full test and clippy output: [G1-crc-library-output.txt](G1-crc-library-output.txt).

Both release benchmark executables completed successfully. See
[method, results and raw output](../benchmarks/crc-library-comparison.md).
Measurements use actual local append/read/reopen operations, identical checksum
coverage and durability policy. Small appends remain sync-bound; larger reads
and recovery scans improve on this host. No claim is made for network traffic,
Linux filesystems, other processors or production p99.9 latency.

## Remaining limits

The published crate has no declared MSRV; Rust 1.85 was tested directly.
ARM64 runtime dispatch is supported by inspected source; forced software
fallback and x86 hardware paths were not independently executed here.
Linux execution is pending PR CI: local Docker inspection failed because the
daemon socket was absent. No Docker artifacts were created. No new filesystem
replay or physical power-cut run was performed for this algorithm-equivalent
change. G1 acceptance and the ADR-003/004 decisions remain open.
