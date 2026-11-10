# G1 XFS replay

On 2026-11-10, a disposable QEMU 11.1.3 ARM64 VM ran Ubuntu 24.04, kernel
`6.8.0-147-generic`, `xfsprogs` 6.6.0, Rust 1.85.0 and the `dm-log-writes`
replay tool at commit `7b70d8a6863c5de30933d42a7672d35d01d2dc6c`. The tested
storage source is from merged commit `77f76ea`. The XFS filesystem was created
on a disposable 512 MiB virtual disk, with a separate 1 GiB trace disk.

The 22 initialization and append marks used by the ext4 run were replayed
independently. For each image, a normal read-write mount recovered the XFS
journal; after unmount, `xfs_repair -n` returned zero; the log then reopened
and its assertions passed. The four early checkpoint-less initialization
states refused opening and preserved existing file names and contents. Both the
first and subsequent acknowledged-record assertions passed. Raw output is in
`G1-xfs-replay-output.txt`.

This comparison uses only the **normal mount recovery** path for XFS. Unlike
ext4's prior experiment, it does not run an offline repair against a dirty
journal: XFS journal replay requires a kernel mount. No `xfs_repair -L` or
log zeroing is used. `xfs_repair -n` is a non-modifying check with documented
coverage limitations. See the [xfs_repair manual](https://man7.org/linux/man-pages/man8/xfs_repair.8.html).

The ext4 path was rerun with the updated script on a freshly formatted virtual disk;
all 44 checks passed. Its output is in `G1-ext4-after-xfs-output.txt`. The
results match the ext4 normal-mount run at these marks. The runs used
separate filesystems and virtual-disk sizes; they do not exhaust possible
persistence, reordering, sector tears, hardware-cache or repeated-crash states.
They do not establish physical power-loss behavior or G1 acceptance. The
reproducible script is destructive to its scratch disks and must only run in a
disposable VM; see `G1-vm-powerloss-replay.md`.
