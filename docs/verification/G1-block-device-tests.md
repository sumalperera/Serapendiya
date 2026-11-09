# G1 block-device testing

`./scripts/run-g1-block-tests.sh` runs a privileged disposable Alpine container,
compiles the repository, and creates a new 64 MiB loop-backed ext4 filesystem
behind a device-mapper linear target. It never intentionally selects an existing
block device. The container uses the repository read-only; its loop image and
build files are ephemeral. The script attempts to unmount and detach the device
on exit. Privileged containers have broad access to the Docker Linux VM, so run
this only in a trusted environment.

On Docker Desktop for macOS (Linux aarch64, Alpine 3.22, Rust 1.87), the run
passed on the isolated ext4 filesystem:

1. Fill until ENOSPC, attempt a large append, and verify the earlier acknowledged
   record remains readable after freeing space and reopening the log.
2. Commit a record, replace the device-mapper linear target with its `error`
   target during an append, verify that append returns an error, and restore the
   device. Unmount, run `e2fsck`, remount and verify the acknowledged record.

`e2fsck` reported no repairs in this run. The test exercises a real filesystem
and kernel block I/O errors. It does **not** cut power, model arbitrary loss,
tearing or reordering of unsynced writes, prove persistence against host cache
loss, or test XFS. Device-mapper `flakey` and `log-writes` were unavailable in
the Docker Linux kernel. G1 acceptance and real power-loss verification remain
open. The regular Rust test suite skips these tests unless explicitly invoked.
