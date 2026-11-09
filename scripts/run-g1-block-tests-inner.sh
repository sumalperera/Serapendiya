#!/bin/sh
set -eu
apk add --no-cache cargo rust build-base linux-headers device-mapper util-linux e2fsprogs >/dev/null
mkdir -p /work
cp /source/Cargo.toml /source/Cargo.lock /work/
cp -R /source/src /source/tests /work/
cd /work
cargo test --locked --test block_device_fault --no-run
truncate -s 64M /tmp/serapendiya-block-test.img
loop=$(losetup --find --show /tmp/serapendiya-block-test.img)
name="serapendiya-g1-test-$$"
mounted=0
mapped=0
cleanup() {
  if [ "$mounted" -eq 1 ]; then umount /mnt/g1 || true; fi
  if [ "$mapped" -eq 1 ]; then dmsetup remove "$name" || true; fi
  losetup -d "$loop" || true
}
trap cleanup EXIT
sectors=$(blockdev --getsz "$loop")
dmsetup create "$name" --table "0 $sectors linear $loop 0"
mapped=1
mkfs.ext4 -q "/dev/mapper/$name"
mkdir -p /mnt/g1
mount "/dev/mapper/$name" /mnt/g1
mounted=1
printf 'disposable\n' > /mnt/g1/.serapendiya-disposable
export SERAPENDIYA_BLOCK_TEST_DIR=/mnt/g1
export SERAPENDIYA_BLOCK_TEST_DM_NAME="$name"
export SERAPENDIYA_BLOCK_TEST_LOOP="$loop"
export SERAPENDIYA_BLOCK_TEST_SECTORS="$sectors"
cargo test --locked --test block_device_fault actual_full_ext4 -- --ignored --nocapture
cargo test --locked --test block_device_fault block_error_during_append -- --ignored --nocapture
umount /mnt/g1
mounted=0
# fsck may return 1 when it has repaired the disposable filesystem.
e2fsck -fy "/dev/mapper/$name" || [ "$?" -eq 1 ]
mount "/dev/mapper/$name" /mnt/g1
mounted=1
cargo test --locked --test block_device_fault block_error_recovered -- --ignored --nocapture
