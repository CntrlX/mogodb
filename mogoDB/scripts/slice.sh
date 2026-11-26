#!/usr/bin/env bash
# Isolated full-crate copy for one work slice: ~/.cache/mogo-slices/NAME.
# OWN_FILES (relative to src/) are symlinked to the repo; everything else is a snapshot copy,
# so concurrent edits elsewhere never break this build.
#   scripts/slice.sh NAME own/file.rs ...    then: cd ~/.cache/mogo-slices/NAME && cargo build
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
name="$1"; shift
dir="$HOME/.cache/mogo-slices/$name"
mkdir -p "$dir"
rm -rf "$dir/src" "$dir/tests"
cp -r "$repo/src" "$dir/src"
cp -r "$repo/tests" "$dir/tests"
cp "$repo/Cargo.toml" "$repo/Cargo.lock" "$dir/"
for f in "$@"; do
  rm -f "$dir/src/$f"
  ln -s "$repo/src/$f" "$dir/src/$f"
done
echo "slice ready: $dir"
