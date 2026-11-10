# G1 virtual-disk power-loss replay

A disposable QEMU 11.1.3 ARM64 VM ran Ubuntu 24.04 with Linux
`6.8.0-147-generic`, ext4 and Rust 1.85.0. The tested storage source matches
commit `7bf06cd`; its storage source and lockfile match `5b7e742`. The
`dm-log-writes` user-space replay tool was built from commit
`7b70d8a6863c5de30933d42a7672d35d01d2dc6c` of
https://github.com/josefbacik/log-writes.

On two disposable virtual disks, the harness wrote an acknowledged record,
then a second record while marking each append boundary. It replayed the block
log from the beginning into a new disposable image for each of these eight
marks: acknowledged, segment write, segment sync, checkpoint write, checkpoint
sync, checkpoint rename, directory sync, and second acknowledgement. After
`e2fsck`, each image retained the first acknowledged record. The directory-sync
and second-acknowledgement images also retained the second record. The complete
output is in `G1-vm-replay-output.txt`.

Every replayed image required filesystem repair: `e2fsck` recovered the ext4
journal and corrected free block/inode accounting. Therefore the result is
**recovery after filesystem repair**, not a claim that replayed filesystems
mounted cleanly without intervention. A subsequent comparison using normal
mount recovery is documented in [G1-ext4-mount-comparison.md](G1-ext4-mount-comparison.md).
The test does not explore every possible
write ordering, sector tear, hardware cache behavior, XFS, or multiple
partitions. It is not real power-cut certification or G1 acceptance.

## Reproduction in a disposable VM only

**The script destroys all contents of `/dev/vdb` and `/dev/vdc`. Never run it
on a machine or VM containing valuable disks.** Create a Linux VM with two
fresh virtual disks of 256 MiB and at least 512 MiB, respectively. Install
`dm-log-writes`, `dmsetup`, `losetup`, `e2fsprogs`, Rust and the replay tool.
Build `replay-log` at the pinned revision above and build the Rust test using
`cargo test --locked --test powerloss_replay --no-run`. Locate the executable
under `target/debug/deps` and run as root:

```sh
SERAPENDIYA_DISPOSABLE_VM=destroy-vdb-and-vdc \
SERAPENDIYA_TEST_BINARY=/absolute/path/to/powerloss_replay-test-binary \
SERAPENDIYA_REPLAY_TOOL=/absolute/path/to/replay-log \
  scripts/run-g1-vm-replay.sh
```

The script checks for the explicit opt-in and mounted scratch disks. Those
checks are safeguards, not proof that a device is disposable. It runs fsck on
replayed copies and never attempts to validate a real machine's power-loss
behavior.

For XFS, use a scratch data disk of at least 512 MiB and a 1 GiB trace disk,
install `xfsprogs`, and set `SERAPENDIYA_FS=xfs`. The script performs normal
journal recovery and `xfs_repair -n` after unmount; it does not zero or repair
a dirty XFS log. See [G1-xfs-replay.md](G1-xfs-replay.md).
