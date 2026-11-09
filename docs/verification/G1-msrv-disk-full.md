# G1 minimum Rust version and disk-full injection

On macOS aarch64, `cargo +1.85.0 test --locked --workspace` passed with
46 library tests, one architecture test and seven crash-recovery tests. CI now
runs the same locked workspace test on Linux using Rust 1.85.0; its result is
separate from this local evidence. The normal formatting and lint checks remain
on the pinned project toolchain.

The `disk_full_preserves_acknowledged_data_and_fences_further_writes` test
injects ENOSPC after a partial segment write, after a partial checkpoint write,
and at directory sync after checkpoint rename. It checks preservation of a
previously acknowledged record, terminal state, and recovered outcome. A
checkpoint temporary file is preserved and blocks subsequent appends.

These are injected errors, not evidence from an actually full filesystem.
Real ENOSPC behavior, Linux filesystem-specific behavior, and power-loss
verification remain open. A container restart is a process interruption, not a
power cut: the host kernel and caches can retain unsynced writes. Even a hard
VM stop alone does not establish which host-cached writes survive. G1 acceptance
remains open.
