# G1 initialization and first-append replay

On 2026-11-10, an isolated QEMU 11.1.3 ARM64 VM ran Ubuntu 24.04, Linux
`6.8.0-147-generic`, ext4, Rust 1.85.0 and `dm-log-writes`. The replay tool
was built from commit `7b70d8a6863c5de30933d42a7672d35d01d2dc6c`. The tested
storage code is from `25e107d`; the harness changes are in this change.

The harness recorded 14 initialization and first-append marks, followed by
8 existing marks on a separately initialized log. It replayed each mark into
independent images for normal ext4 journal recovery and explicit `e2fsck`
repair. All 44 checks passed. Following normal mount and unmount, all 22
read-only filesystem checks returned zero before the log was opened.

At four early initialization marks, replay produced existing files without a
checkpoint. In both recovery paths, `Log::open` refused and the test confirmed
that existing names and contents were unchanged. This comparison does not
assert that timestamps or all filesystem metadata are unchanged. Once the
initialization directory sync completed, the checkpoint existed and reopening
succeeded; the first record was present after its directory sync and after its
acknowledgement. The existing acknowledged-record checks also passed.

This run models the write ordering represented by `dm-log-writes` on one VM
and filesystem. It does not cover every possible write loss, torn sector,
hardware cache behavior, initialization syscall boundary or XFS. In
particular, the harness marks I/O boundaries exposed by `StorageIo`, not each
individual directory or lock-file operation. It does not establish G1
acceptance. Complete output is in `G1-init-replay-output.txt`; setup and the
explicit disposable-disk safety requirements are in
`G1-vm-powerloss-replay.md`.
