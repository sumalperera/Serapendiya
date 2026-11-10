# Storage review fixes: local measurement

Date: 2026-11-11. Baseline `ca3d361`; candidate is the containing repair commit.
Both use CRC32C 0.6.9, the same storage format and the same sync/checkpoint policy.
Both ran with the updated `tests/storage_benchmark.rs`: distinguishable payloads,
returned-offset assertions and content checks. This run is separate from the
previous CRC-library comparison and must not be mixed with its raw results.

Environment and workload match [the CRC benchmark method](crc-library-comparison.md):
ARM64 macOS 26.6.2, APFS, Rust 1.99.1, release builds, one caller and warm reads.
Each configuration has five fresh logs, 64 commits/log, 256 scattered fetches/log
and three reopens/log. Per-commit input construction is outside append timing.
The baseline source was exported to a disposable directory; only the benchmark
harness was updated there. Compile each before measuring, then run each compiled
executable sequentially using the Python resource wrapper documented in that
method. No project tests or builds ran concurrently. Baseline ran first, so
ordering and external-host noise are uncontrolled.

Raw output: [baseline](storage-review-baseline-macos.txt) and
[candidate](storage-review-candidate-macos.txt). Both completed with exit 0.
Restart tails have only 15 samples; no production p99.9 or Linux claim follows.
Peak RSS is for the benchmark process, not all client-owned buffers or a broker.
No allocation counter or individual sync-latency instrumentation was added.

| Value | Group | Append group ms, before → after | Warm fetch µs, before → after | Restart ms, before → after |
| --- | --- | --- | --- | --- |
| 32 B | 1 | 12.036 → 12.829 | 12.292 → 17.250 | 0.383 → 0.480 |
| 32 B | 16 | 11.957 → 12.712 | 17.417 → 15.875 | 2.114 → 2.246 |
| 1024 B | 1 | 11.808 → 12.182 | 19.541 → 3.666 | 0.428 → 0.318 |
| 1024 B | 16 | 12.678 → 13.128 | 22.833 → 8.709 | 2.616 → 2.751 |
| 65536 B | 1 | 12.031 → 11.986 | 141.250 → 27.250 | 0.898 → 1.213 |
| 65536 B | 16 | 13.820 → 12.973 | 160.417 → 38.542 | 13.624 → 20.296 |

Direct encoding removes intermediate payload buffers by construction. The
byte-spaced index reduces large-record navigation while adding index entries;
full startup payload verification is retained and remains O(history). Sync and
host noise can dominate append timings. These measurements do not establish a
universal throughput improvement or validate the proposed lifecycle protocol.
