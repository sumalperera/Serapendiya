# G1 ext4 normal-mount comparison

On 2026-11-10, the `dm-log-writes` replay workload was rerun in a disposable
QEMU 11.1.3 ARM64 VM with Ubuntu 24.04, kernel `6.8.0-147-generic`, ext4,
Rust 1.85.0 and replay-log commit `7b70d8a6863c5de30933d42a7672d35d01d2dc6c`.
The tested storage code is from merged commit `d91f03f`.

For each of the eight previously documented marks, the trace was replayed into
fresh, independent images for two paths:

- **Normal mount:** mount and unmount the replayed ext4 image, allowing the
  kernel's normal journal recovery; run `e2fsck -fn` while unmounted, before
  the application opens the log; then remount and verify the log. All eight
  read-only filesystem checks exited 0, and all eight log checks passed.
- **Explicit fsck:** replay again from scratch, run `e2fsck -fy`, mount and
  verify the log. All eight log checks passed; the check reported repairs on
  the replayed images as in the earlier run.

The first acknowledged record survived every case. The second record survived
after directory sync and after its acknowledgement. Full output is in
`G1-ext4-mount-comparison-output.txt`.

This shows that, for this workload and VM, a normal mount recovered the journal
and produced a clean subsequent read-only filesystem check without a prior
explicit repair. It is consistent with the earlier repair messages reflecting
running a full check before normal mount recovery. It does not isolate the
specific cause of each accounting correction, prove all power-loss outcomes,
cover XFS, or establish G1 acceptance. The block replay and virtual disk impose
their own model and do not reproduce every physical storage cache or torn write.

The guarded `scripts/run-g1-vm-replay.sh` now performs both paths on independent
copies. It is destructive to `/dev/vdb` and `/dev/vdc` and must run only in a
disposable VM; see `G1-vm-powerloss-replay.md` for setup and opt-in requirements.
