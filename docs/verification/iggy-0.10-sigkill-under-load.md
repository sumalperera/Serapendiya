# Iggy persisted SIGKILL-under-load probe

Date: 2026-11-16. Source pin: Apache Iggy `server-0.10.0`, commit
[`91e0206367aad2d30259f7d45203832b08ae84e7`](https://github.com/apache/iggy/tree/91e0206367aad2d30259f7d45203832b08ae84e7).

## Scope

This is a repeated broker-process-crash probe, not a power-loss test. It used
the pinned debug server and CLI on macOS 26.6.2, arm64 (Darwin 25.6.0),
with rustc/cargo 1.99.1. The topic used one partition and one replica, with
message and consumer-offset durability both set to `persisted`.

For each of 20 rounds, four CLI producer workers concurrently sent four uniquely
identified messages per send request. After at least two requests had returned
success and at least one send remained in flight, the broker process was sent
SIGKILL (exit status -9). The same data directory was restarted, and the topic
was polled from its first offset. The probe checked each message in every
successful send request for exactly one recovered copy before starting the next
round. Calls that did not return success are classified as unknown outcome,
since the probe cannot tell whether the broker accepted them before the kill.

## Results

Across the 20 rounds, 150 send groups returned success (600 messages), and
all 600 were present exactly once after every subsequent restart. No
acknowledged message was missing or duplicated. Eighty send groups returned
without a successful client acknowledgement; five groups (20 messages) were
present after restart and 75 groups were absent. That is the expected
unknown-outcome window: a request can persist even though its sender did not
receive success.

| Round | Newly ACKed groups | Unknown groups cumulative | Recovered messages | Unknown groups recovered | Missing/duplicate ACKed messages |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6 | 4 | 24 | 0 | 0 |
| 2 | 8 | 8 | 56 | 0 | 0 |
| 3 | 8 | 12 | 88 | 0 | 0 |
| 4 | 8 | 16 | 120 | 0 | 0 |
| 5 | 4 | 20 | 136 | 0 | 0 |
| 6 | 4 | 24 | 152 | 0 | 0 |
| 7 | 8 | 28 | 184 | 0 | 0 |
| 8 | 8 | 32 | 216 | 0 | 0 |
| 9 | 8 | 36 | 252 | 1 | 0 |
| 10 | 8 | 40 | 284 | 1 | 0 |
| 11 | 8 | 44 | 316 | 1 | 0 |
| 12 | 8 | 48 | 348 | 1 | 0 |
| 13 | 8 | 52 | 380 | 1 | 0 |
| 14 | 8 | 56 | 428 | 5 | 0 |
| 15 | 8 | 60 | 460 | 5 | 0 |
| 16 | 8 | 64 | 492 | 5 | 0 |
| 17 | 8 | 68 | 524 | 5 | 0 |
| 18 | 8 | 72 | 556 | 5 | 0 |
| 19 | 8 | 76 | 588 | 5 | 0 |
| 20 | 8 | 80 | 620 | 5 | 0 |

## Reproduction

Build the pinned checkout's debug server and CLI, then run the committed
[probe script](../../scripts/iggy_sigkill_load_probe.py):

```sh
python3 scripts/iggy_sigkill_load_probe.py \
  --iggy-root /path/to/iggy-at-91e0206 \
  --output-dir /tmp/iggy-sigkill-run \
  --rounds 20 --workers 4 --messages-per-group 4
```

The output directory must not already exist. It contains the isolated broker
data, server log, per-round poll output and a machine-readable `results.json`.
The script starts the server on an available loopback port, creates a fresh
stream/topic, and kills only the broker process it started.

## Limits

SIGKILL leaves the host kernel and page cache running. These observations do
not test loss of unsynced pages during power failure, device-cache behavior,
filesystem recovery, data corruption, a specific injected syscall cut, release
build behavior, or Linux ext4/XFS. They do not replace the deterministic
cut-point injection or block/VM replay required for stronger durability claims.
