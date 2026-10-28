#!/usr/bin/env bash
# Release packaging for murmur-sqlite-ext.
#
# Builds release dynamic + static artifacts with versioned names, verifies
# their linkage, runs the static smoke test, and writes checksums:
#
#   dist/murmur-sqlite-ext-<version>-<triple>.so
#   dist/murmur-sqlite-ext-<version>-<triple>-static.so
#   dist/SHA256SUMS
#
# Usage: ./dist.sh [--target TRIPLE]
# Cross builds compile only (tests can't execute foreign binaries).
set -euo pipefail

SQLITE_AMALGAMATION_VERSION=3450100
ROOT="$(cd "$(dirname "$0")" && pwd)"
HOST_TRIPLE="$(rustc -vV | sed -n 's/^host: //p')"
TRIPLE="$HOST_TRIPLE"
TARGET_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --target) TRIPLE="$2"; TARGET_ARGS=(--target "$2"); shift 2;;
    *) echo "usage: dist.sh [--target TRIPLE]" >&2; exit 1;;
  esac
done

VERSION="$(grep '^version' "$ROOT/crates/murmur-sqlite-ext/Cargo.toml" | head -1 | cut -d'"' -f2)"
case "$TRIPLE" in
  *linux*)   EXT=so;    PREFIX=lib;;
  *darwin*|*macos*) EXT=dylib; PREFIX=lib;;
  *windows*) EXT=dll;   PREFIX=;;
  *) echo "dist.sh: unknown platform for triple $TRIPLE" >&2; exit 1;;
esac
ART_DIR="$ROOT/target"
if [ "$TRIPLE" != "$HOST_TRIPLE" ]; then ART_DIR="$ART_DIR/$TRIPLE"; fi
ART_DIR="$ART_DIR/release"
BUILT="$ART_DIR/${PREFIX}murmur_sqlite_ext.$EXT"

export MURMUR_SQLITE_AMALGAMATION="$SQLITE_AMALGAMATION_VERSION"
if [ ! -f "$ROOT/vendor/sqlite-amalgamation-$SQLITE_AMALGAMATION_VERSION/sqlite3.c" ]; then
  echo "dist.sh: missing vendor/sqlite-amalgamation-$SQLITE_AMALGAMATION_VERSION (see vendor/README.md)" >&2
  exit 1
fi

mkdir -p "$ROOT/dist"
DYNAMIC="$ROOT/dist/murmur-sqlite-ext-$VERSION-$TRIPLE.$EXT"
STATIC="$ROOT/dist/murmur-sqlite-ext-$VERSION-$TRIPLE-static.$EXT"

echo "==> dynamic ($TRIPLE)"
cargo build --release -p murmur-sqlite-ext "${TARGET_ARGS[@]}"
cp "$BUILT" "$DYNAMIC"

echo "==> static (amalgamation $SQLITE_AMALGAMATION_VERSION)"
cargo build --release -p murmur-sqlite-ext --features static-sqlite "${TARGET_ARGS[@]}"
cp "$BUILT" "$STATIC"

if [ "$EXT" = "so" ] && [ "$TRIPLE" = "$HOST_TRIPLE" ]; then
  echo "==> linkage checks"
  readelf -d "$DYNAMIC" | grep -q 'libsqlite3\.so' \
    || { echo "dynamic artifact lost its libsqlite3 DT_NEEDED" >&2; exit 1; }
  if readelf -d "$STATIC" | grep -q 'libsqlite3'; then
    echo "static artifact still needs libsqlite3" >&2; exit 1
  fi
  echo "==> static smoke test"
  MURMUR_EXT_STATIC_CDYLIB="$STATIC" \
    cargo test --release -p murmur-sqlite-ext --test static_smoke -- --ignored
else
  echo "==> (skipping linkage checks and smoke test: cross build or non-ELF)"
fi

echo "==> checksums"
(cd "$ROOT/dist" && sha256sum ./*."$EXT" > SHA256SUMS)
ls -la "$ROOT/dist"
