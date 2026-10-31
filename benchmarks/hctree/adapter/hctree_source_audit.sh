#!/usr/bin/env bash
# One-off source audit to map the pinned HC-tree POSIX I/O call sites.
set -euo pipefail
rev=e3820e788797940ae4556b35f7c6fd190d61d852c30637b9d3ab583b91223c5a
out="${1:-hctree-source-audit}"
mkdir -p "$out/src"
curl -fL --retry 3 --max-time 180 "https://www.sqlite.org/hctree/tarball/hctree.tar.gz?r=$rev" -o "$out/hctree.tar.gz"
tar -xzf "$out/hctree.tar.gz" -C "$out/src" --strip-components=1
test "$(tr -d '\r\n' < "$out/src/manifest.uuid")" = "$rev"
echo "PINNED_HCTREE_REV=$rev"
pattern='(^|[^[:alnum:]_])(open|close|pread|pwrite|read|write|mmap|munmap|msync|ftruncate|fsync|fstat)[[:space:]]*[(]'
for f in hct_file.c hct_pman.c hct_database.c hct_journal.c; do
  path="$out/src/src/$f"
  test -f "$path" || continue
  echo "=== $f direct POSIX call sites ==="
  grep -nE "$pattern" "$path" | head -n 120 || true
  if test "$f" = hct_file.c; then
    echo "=== $f surrounding code (15 lines before/after each call, max 50 sites) ==="
    grep -nE "$pattern" "$path" | head -n 50 | cut -d: -f1 | while read -r n; do
      lo=$((n > 15 ? n - 15 : 1))
      hi=$((n + 15))
      echo "--- $f:$n ---"
      sed -n "${lo},${hi}p" "$path"
    done
  fi
done
echo "SOURCE_AUDIT_DONE"
