#!/usr/bin/env sh
set -eu

image_name="serapendiya-g1-test"

docker build \
  --file Dockerfile.g1-test \
  --tag "$image_name" \
  .

docker run --rm "$image_name"
