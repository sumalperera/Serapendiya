#!/bin/sh
# Uses a privileged disposable Linux container and an isolated loop-backed ext4
# filesystem. Never point this script at an existing block device or data dir.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
docker run --rm --privileged -v "$root:/source:ro" alpine:3.22 sh -eu /source/scripts/run-g1-block-tests-inner.sh
