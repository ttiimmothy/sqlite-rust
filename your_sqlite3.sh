#!/bin/sh
exec cargo run \
  --quiet \
  --release \
  --target-dir=/tmp/codecrafters-sqlite-target \
  --manifest-path "$(dirname "$0")/Cargo.toml" "$@"
