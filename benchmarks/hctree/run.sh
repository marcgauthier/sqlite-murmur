#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/hctree-benchmark-results"
DURATION=60
mkdir -p "$OUT"
echo "MURMUR_COMMIT $(git -C "$ROOT" rev-parse HEAD)" | tee "$OUT/revisions.txt"
lscpu > "$OUT/system.txt"
mkdir -p "$OUT/hctree-src"
curl -fL --retry 3 --max-time 180 'https://www.sqlite.org/hctree/tarball/hctree.tar.gz?r=hctree' -o "$OUT/hctree.tar.gz"
sha256sum "$OUT/hctree.tar.gz" | tee "$OUT/hctree.sha256"
tar -xzf "$OUT/hctree.tar.gz" -C "$OUT/hctree-src" --strip-components=1
test ! -f "$OUT/hctree-src/manifest.uuid" || cp "$OUT/hctree-src/manifest.uuid" "$OUT/hctree-fossil-commit.txt"
(cd "$OUT/hctree-src" && ./configure --enable-shared && make -j2 libsqlite3.so)
# HC-tree's makefile generates libsqlite3.so directly in the build directory.
LIB="$OUT/hctree-src"
test -f "$LIB/libsqlite3.so"
g++ -std=c++17 -O3 -Wall -Wextra -pedantic -I"$OUT/hctree-src" \
  "$ROOT/benchmarks/hctree/hctree_bench.cpp" -L"$LIB" -Wl,-rpath,"$LIB" \
  -lsqlite3 -ldl -lpthread -o "$OUT/hctree_bench"
ldd "$OUT/hctree_bench" | tee "$OUT/hctree-linkage.txt"
ldd "$OUT/hctree_bench" | grep -F "$LIB"
# Validate the HCTree-specific loader, file format and leader journal BEFORE
# compiling the Rust/RocksDB backend or starting 60-second measurements.
for repl in off; do
  LD_LIBRARY_PATH="$LIB" "$OUT/hctree_bench" \
    --database="$OUT/hctree-smoke-$repl.db" --seconds=0.01 \
    --batch=1 --replication="$repl" > "$OUT/hctree-smoke-$repl.txt"
done
cmake -S "$ROOT" -B "$ROOT/build-hctree-comparison" -DCMAKE_BUILD_TYPE=Release \
  -DSQLITE_MURMUR_BUILD_TESTS=OFF -DSQLITE_MURMUR_BUILD_BENCHMARKS=ON \
  -DSQLITE_MURMUR_ENABLE_CRSQLITE=ON
cmake --build "$ROOT/build-hctree-comparison" --target benchmark --parallel 2
for repl in off; do
 for batch in 1 100; do
  key="$repl-$batch"
  echo "RUN murmur $key"
  "$ROOT/build-hctree-comparison/benchmark" --suite=single --backend=murmur --seconds="$DURATION" --batch-size="$batch" --replication="$repl" | tee "$OUT/murmur-$key.txt"
  echo "RUN hctree $key"
  LD_LIBRARY_PATH="$LIB" "$OUT/hctree_bench" --database="$OUT/hctree-$key.db" --seconds="$DURATION" --batch="$batch" --replication="$repl" | tee "$OUT/hctree-$key.txt"
 done
done
python3 "$ROOT/benchmarks/hctree/report.py" "$OUT"
