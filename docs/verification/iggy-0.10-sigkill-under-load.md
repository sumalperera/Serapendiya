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

## Linux follow-up: ext4 and XFS

Date: 2026-11-17. Source pin remains Apache Iggy `server-0.10.0`,
commit [`91e0206367aad2d30259f7d45203832b08ae84e7`](https://github.com/apache/iggy/tree/91e0206367aad2d30259f7d45203832b08ae84e7).

The same SIGKILL-under-load runner was extended to select the `debug` or
`release` binary directory. On Docker Desktop 29.9.2 (aarch64), its LinuxKit
VM reported kernel `7.0.14-linuxkit`. The build container used the pinned
`rust:1.99.1` image
(`sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d8710131e0b39c50348e`)
with Debian 13 (Trixie), rustc 1.99.1, and Cargo 1.99.1. The image digest recorded
by Docker was `sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d871684bc9c5433ae07`.

The repository's default release profile enables fat LTO and one codegen unit.
That link attempt was terminated by SIGKILL near the Docker VM's 4 GiB memory
limit (container use reached about 3.4 GiB of its 3.5 GiB limit); no binary from
that attempt was used. To stay within the available VM memory, the completed
build used the optimized release profile with LTO disabled and eight codegen
units:

```sh
CARGO_BUILD_JOBS=1 \
CARGO_PROFILE_RELEASE_LTO=false \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=8 \
cargo build --locked --release -p server -p iggy-cli
```

The successful build finished in 11m16s. The probe container was capped at
1.5 CPUs and 3,000 MiB RAM. For each filesystem, a 512 MiB loopback image was
formatted and mounted inside the privileged container using the LinuxKit
kernel; the broker data directory was on that mount. Each run used the same
four workers, four messages per request, and 20 SIGKILL/restart rounds as the
macOS run. The container's privilege was used only for these disposable loop
mounts.

| Filesystem | Rounds passed | ACKed groups / messages | Unknown-outcome groups | Unknown groups observed after restart | Recovered messages at round 20 | ACKed messages missing or duplicated |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ext4 loopback | 20/20 | 227 / 908 | 80 | 4 | 924 | 0 |
| XFS loopback | 20/20 | 242 / 968 | 79 | 1 | 972 | 0 |

All 40 rounds passed: every ACKed message was present exactly once after each
restart. Some requests whose clients did not receive a successful response
were present after restart (four groups on ext4 and one on XFS), consistent with
the unknown-outcome contract. Full machine-readable results are in
[the ext4 JSON](iggy-0.10-sigkill-under-load-linux-ext4.json) and
[the XFS JSON](iggy-0.10-sigkill-under-load-linux-xfs.json). Their SHA-256
hashes are respectively
`01d3ead2c82e868968a58615fdab51307f33a63ea6fbe5cc75e7c685672ef0df` and
`255f097879d1acb73637351de1c22d9330487f22cc77ff245bad35eb460204eb`.

## Limits

These are process-crash observations. SIGKILL leaves the Linux kernel and its
page cache running. The ext4 and XFS filesystems were loopback files inside
Docker Desktop's Linux VM, not physical block devices. The runs do not test
power loss, device-cache behavior, filesystem recovery after power loss,
storage corruption, injected syscall cut points, block/VM replay, or performance.
The default fat-LTO release binary was not built; the Linux probes used the
optimized release profile described above. These results do not establish
default-profile release behavior, bare-metal filesystem durability, production
readiness, or cross-platform guarantees.
