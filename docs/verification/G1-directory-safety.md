# G1 directory safety verification

This change keeps an open directory descriptor for classification, segment and
checkpoint operations, and holds advisory locks on the directory and the
existing `.lock` file. It refuses symbolic links, non-regular files and
multiply-linked storage files. Checkpoint temporary files are created
exclusively; an existing temporary file is preserved and blocks appends until
an operator inspects it. An interrupted initialization without a checkpoint is
also refused and requires operator inspection. No automatic removal or repair
of ambiguous data is attempted. The parent of a new data directory must already
exist.

The protection assumes a trusted parent path during initial lookup and
cooperating processes for advisory locking; it does not protect against a
privileged or non-cooperating process modifying an open file or directory.
Network filesystems and real power-loss behavior have not been validated.
G1 acceptance remains open.

Verification on macOS with rustc 1.99.1:

- `cargo test --locked`: 45 library tests, one architecture test and seven
  crash-recovery tests passed.
- `cargo clippy --locked --workspace --all-targets -- -D warnings`: passed.
- `cargo fmt --all --check`: passed.
- Linux Docker `Dockerfile.g1-test`: build, formatting, clippy and the same
  tests passed on the container filesystem. This is not an ext4/XFS or
  power-loss test.

The new cases exercise directory path replacement, hard-linked segment
refusal, preservation of a stale checkpoint temporary file, and concurrent
initialization. Existing process-abort and simulated power-cut tests remain in
place.

The new direct dependency is rustix 1.1.6, with its filesystem feature; its
published license is Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT.
The updated Cargo.lock SHA-256 is
`345c528fb99f919920404d013f088290f4f39d03e2470f3c91c60245a8c1909c`.
