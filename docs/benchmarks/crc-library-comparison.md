# CRC implementation: local storage comparison

Date: 2026-11-11. Baseline: merged main `b6e725d` plus the new benchmark harness,
with the original bitwise CRC. Candidate: the same harness and production
checksum coverage using `crc32c` 0.6.9. Production changes only the CRC function
implementation; no format or durability ordering change is intended.

## Method and reproduction

Host: ARM64 macOS 26.6.2 (Darwin 25.6.0), APFS local journaled filesystem;
Rust 1.99.1, release profile, default target features. The crate source performs
runtime ARM CRC detection, x86-64 SSE4.2 detection or software fallback. No x86
execution or forced software-fallback test was performed here.

Compile each implementation with the identical `tests/storage_benchmark.rs`:

```sh
cargo test --release --locked --test storage_benchmark --no-run
cargo test --release --locked --test storage_benchmark -- --ignored --nocapture
```

For the recorded measurements, the compiled baseline executable was saved
before changing the checksum wrapper. Each compiled executable was then run
sequentially, with no project test/build job running concurrently. The baseline
ran first; this does not control external host activity, thermal effects or
systematic ordering bias. An initial overlapping run was discarded. Compilation
was excluded. Python wrapped each invocation to capture wall time and
`resource.getrusage(resource.RUSAGE_CHILDREN)` user/system CPU and peak RSS:

```python
import resource, subprocess, sys, time
start = time.monotonic()
result = subprocess.run([sys.argv[1], '--ignored', '--nocapture'])
r = resource.getrusage(resource.RUSAGE_CHILDREN)
print(f'PROCESS exit={result.returncode} wall_s={time.monotonic()-start:.3f} user_s={r.ru_utime:.3f} sys_s={r.ru_stime:.3f} maxrss_bytes_macos={r.ru_maxrss}', flush=True)
raise SystemExit(result.returncode)
```

Each value size (32 B, 1 KiB, 64 KiB) and group size (1, 16) uses five fresh
logs. Each log receives 64 synchronous commits, followed by 256 deterministic
scattered reads and three close/reopens. The unchanged protocol syncs segment,
checkpoint and directory for each commit. Append timing excludes initialization;
throughput is records divided by aggregate append call time. Fetch includes
reading, checksums and decode; content assertions follow the timed interval.
Restart includes full certified-prefix verification and derived-index rebuild.
The largest individual log is approximately 64 MiB. Scratch logs are removed.

Reads and restarts are warm-cache operations after writes. There is one caller,
one log and no network, concurrency or background processing. Allocations,
copied bytes and individual sync latency are not instrumented. Peak RSS is for
the complete benchmark process; it is not a broker memory budget measurement.
Append/fetch percentiles have 320/1,280 observations per configuration. Restart
has only 15: its p99/p99.9 are the maximum, not reliable tail estimates. These
short runs cannot establish production p99.9 performance or Linux durability.

Raw output: [baseline](crc-library-baseline-macos.txt) and
[candidate](crc-library-candidate-macos.txt). Results below describe this host
and workload only. No comparison with another product is implied.

## Observed medians

| Value | Group | Append group ms (before → after) | Warm fetch µs (before → after) | Restart ms (before → after) |
| --- | --- | --- | --- | --- |
| 32 B | 1 | 12.027 → 12.154 | 36.375 → 16.750 | 0.518 → 0.410 |
| 32 B | 16 | 12.269 → 11.982 | 36.000 → 18.583 | 3.813 → 1.982 |
| 1024 B | 1 | 12.227 → 11.917 | 129.792 → 20.500 | 1.029 → 0.421 |
| 1024 B | 16 | 13.534 → 12.854 | 146.208 → 24.167 | 11.846 → 2.608 |
| 65536 B | 1 | 13.095 → 12.993 | 4396.083 → 126.125 | 31.661 → 0.887 |
| 65536 B | 16 | 22.005 → 13.992 | 4406.250 → 164.583 | 509.031 → 10.919 |

Small-write append medians remain dominated by sync latency. Large-payload
reads and recovery benefit substantially on this host. Checksum coverage stays
the same; these results do not select the proposed split-checksum format.
