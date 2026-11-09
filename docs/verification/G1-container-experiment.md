# G1 container experiment verification

Status: **UNVERIFIED for G1 acceptance and production use.** The experiment was
merged in PR #13, with code commit `6a845e5762b4f7b610e47609f58496ce78f06ec1`.
PR #12 containing safety commit `a2dcf97621e2b41e89596fa77996a8893566ab68`
was merged previously. The experiment changes the on-disk checkpoint version
and segment frame; legacy version-1 checkpoints are refused, not migrated.

On 2026-11-09, with Rust `1.99.1` and lockfile SHA-256
`5f9e35ccbe672f87d2abff5e4fe29090f26c04b766e80fe9216bd6e442e1b04b`,
local `cargo test --workspace --locked` and
`cargo clippy --workspace --all-targets --locked -- -D warnings` exited 0.
The Linux aarch64 Docker command
`docker build --progress plain -f Dockerfile.g1-test -t serapendiya-container-final-check .`
exited 0. Its Dockerfile ran `cargo build --locked`, `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace`. The temporary image was removed. The Docker run
included the unknown-flag and expanded simulated-failure tests.
After that Docker run, a change added fail-closed handling and a test for an
ambiguous `checkpoint.tmp` without a durable checkpoint. The G0 and Linux Docker
G1 jobs passed for the PR commit in
[GitHub Actions run 42560362950](https://github.com/sumalperera/Serapendiya/actions/runs/42560362950).
That result does not verify later changes made after the PR commit.

Coverage includes independent kafka-python 3.0.13 fixtures for two narrow
single-record batches; malformed record lengths; header corruption; legacy
checkpoint/segment preservation; first-append process aborts including one-byte
partial write; exclusive log access; uncertain append outcome; and a deterministic
test model of selected write/sync/rename cuts. The test model represents a fixed
set of lost, partial or reordered writes and both old/new rename outcomes. It
is not a general filesystem simulator, randomized state-machine test, sector
model, real power-cut test or ext4/XFS matrix.

The container's full-payload CRC is experimental; a split-checksum design is
also proposed. The app currently accepts only opaque values and builds a narrow
Kafka record; it cannot ingest arbitrary Kafka batches, and no client protocol
is implemented. No Kafka compatibility, performance, production durability,
cross-filesystem guarantees or migration of legacy data is claimed. Directory
classification still uses path-based operations with check/use races and does not
establish the proposed no-mutation-on-refusal guarantee. ADR-003/004 format,
initialization, upgrade and recovery reviews remain open.
