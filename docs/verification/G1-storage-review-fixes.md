# G1 storage review repairs

Date: 2026-11-11. Base `ca3d361` (the CRC-library branch). The containing
commit identifies this candidate. Founder requested all findings be fixed,
then requested one consolidated MR after the work is complete.

## Implemented repairs

- Certified read corruption latches unavailable; subsequent reads/appends fail
  terminally. `terminal_reason()` exposes the diagnosis. Ordinary I/O errors
  keep their original classification. There is no automatic repair.
- Append encodes directly into one pre-reserved group buffer with small offset
  and frame-boundary vectors; no per-record/batch/container payload buffers.
- Validation returns the decoded borrowed value, avoiding a second parse.
- Out-of-range reads return before I/O. A 4 KiB byte threshold supplements the
  existing 16-record index stride, reducing navigation over unrelated large
  records. The index remains derived and deterministically rebuildable.
- Replace custom unsafe flock FFI with the existing rustix wrapper.
- Expose checkpoint generation as a diagnostic sequence, explicitly not fencing.
- Replace source-file presence checks with CLI process behavior tests. These do
  not claim to enforce future crate/module dependency boundaries.
- Benchmark distinct records and assert returned offsets as well as content.

Existing stored bytes, versions, checksum coverage and durable ordering remain
unchanged. Golden Kafka fixtures and differential comparison with the previous
encoder verify format compatibility across length/offset boundaries and grouped
encoding. The prior encoder is retained only as a test oracle.

## Established approaches and deliberate differences

The preceding review inspected Kafka file records and sparse indexes,
Redpanda bounded chunks and batch metadata, ReductStore block/index lifecycle,
and Liftbridge partition/cursor persistence. Sources and relevance are in
[ADR-024](../adr/ADR-024-storage-lifecycle-and-commit-proposal.md). The repairs
apply buffer ownership and byte-sensitive indexing while preserving full
payload verification. No upstream code was copied and no dependency was added.

## Validation

```text
cargo fmt --all --check                                        exit 0
cargo clippy --locked --workspace --all-targets -- -D warnings   exit 0
cargo test --locked --workspace                                exit 0
cargo +1.85.0 test --locked --workspace                         exit 0
git diff --check                                              exit 0
```

52 library tests pass, including new corruption-health, format compatibility
and byte-index recovery tests; two CLI tests and seven process-crash tests pass.
Selected storage tests also exercise injected I/O/ENOSPC and simulated power
loss. Destructive filesystem/VM suites remain opt-in and were not rerun. Linux
execution remains pending; local Docker is stopped and no Docker artifacts
were created. Full command output is in
[G1-storage-review-fixes-output.txt](G1-storage-review-fixes-output.txt).

The matched release benchmark completed for both baseline and candidate. See
[measurement method and results](../benchmarks/storage-review-comparison.md).
No production latency, full memory-accounting or universal speedup claim follows.

## Remaining architectural work; not implemented

Full-history startup verification, unbounded segment lifecycle, overlapping
payload checksum coverage and independent-request commit coordination remain.
[ADR-024](../adr/ADR-024-storage-lifecycle-and-commit-proposal.md) specifies the
proposed work and required recovery/format decisions. The local blueprint's
hard rule 8 requires human approval of decisions affecting disk layout,
durability, Kafka compatibility or persistent state before code lands; the
accepted ADR-003 also requires approval before adding a persistent index.
No self-approval or gate acceptance is recorded by this repair.

The standing engineering rule and template from the other open documentation
MR have been included on the consolidation branch. Agent instructions and the
local blueprint stay ignored. The two pre-existing staged images are excluded.
The consolidated MR includes the completed repairs and the remaining design
proposal together, superseding the separate CRC and engineering-rule MRs at the
founder's request. It must not describe the architectural proposals as implemented
or claim all review findings are resolved.
