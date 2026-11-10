#!/bin/bash
set -euo pipefail
# DESTRUCTIVE: only inside a disposable VM with two dedicated scratch disks.
if [[ ${SERAPENDIYA_DISPOSABLE_VM:-} != destroy-vdb-and-vdc ]]; then
  echo "Refusing to overwrite /dev/vdb and /dev/vdc without explicit disposable VM opt-in" >&2
  exit 2
fi
[[ $(id -u) == 0 ]] || { echo "Run as root inside the disposable VM" >&2; exit 2; }
for disk in /dev/vdb /dev/vdc; do
  [[ -b $disk ]] || { echo "Missing scratch disk $disk" >&2; exit 2; }
  [[ -z $(lsblk -nrpo MOUNTPOINTS "$disk" | tr -d '[:space:]') ]] || { echo "Mounted scratch disk $disk" >&2; exit 2; }
done
: "${SERAPENDIYA_TEST_BINARY:?Set path to the compiled powerloss_replay test binary}"
: "${SERAPENDIYA_REPLAY_TOOL:?Set path to the compiled replay-log tool}"
bin=$SERAPENDIYA_TEST_BINARY
for mount_dir in /mnt/live /mnt/replay; do
  if mountpoint -q "$mount_dir"; then echo "$mount_dir already mounted" >&2; exit 2; fi
done
loop=""
sectors=$(blockdev --getsz /dev/vdb)
dmsetup create serapendiya-replay --table "0 $sectors log-writes /dev/vdb /dev/vdc"
trap 'umount /mnt/replay 2>/dev/null || true; umount /mnt/live 2>/dev/null || true; dmsetup remove serapendiya-replay 2>/dev/null || true; if [[ -n $loop ]]; then losetup -d "$loop" 2>/dev/null || true; fi' EXIT
mkfs.ext4 -F -q /dev/mapper/serapendiya-replay
dmsetup message serapendiya-replay 0 mark mkfs
mkdir -p /mnt/live /mnt/replay
mount /dev/mapper/serapendiya-replay /mnt/live
REPLAY_LOG_DIR=/mnt/live/log "$bin" --ignored --exact generate_trace --nocapture
umount /mnt/live
dmsetup remove serapendiya-replay
# Replay each mark twice: normal kernel journal recovery and explicit fsck
# repair must not influence one another. Each pass starts from a fresh image.
for mark in acknowledged segment-write segment-sync checkpoint-write checkpoint-sync checkpoint-rename directory-sync second-ack; do
  for recovery in mount fsck; do
    echo "=== REPLAY $mark RECOVERY $recovery ==="
    truncate -s 0 /tmp/serapendiya-replay.img
    truncate -s 256M /tmp/serapendiya-replay.img
    loop=$(losetup --find --show /tmp/serapendiya-replay.img)
    "$SERAPENDIYA_REPLAY_TOOL" --log /dev/vdc --replay "$loop" --end-mark "$mark"
    if [[ $recovery == fsck ]]; then
      e2fsck -fy "$loop" || [ "$?" -eq 1 ]
    else
      # A normal read-write ext4 mount replays the journal. Inspect the result
      # without repairing it before the application has opened the log.
      mount "$loop" /mnt/replay
      umount /mnt/replay
      set +e
      e2fsck -fn "$loop"
      fsck_status=$?
      set -e
      echo "POST_MOUNT_FSCK_STATUS=$fsck_status"
      # A nonzero result means normal mount recovery did not establish a
      # clean check (or the check itself failed); do not report success.
      if (( fsck_status != 0 )); then exit "$fsck_status"; fi
    fi
    mount "$loop" /mnt/replay
    REPLAY_LOG_DIR=/mnt/replay/log REPLAY_MARK="$mark" "$bin" --ignored --exact verify_replay --nocapture
    umount /mnt/replay
    losetup -d "$loop"
    loop=""
  done
done
