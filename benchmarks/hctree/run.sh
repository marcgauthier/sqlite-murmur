#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/hctree-benchmark-results"
DURATION="${HCTREE_BENCH_SECONDS:-60}"
CACHE_FLAGS=()
if command -v ccache >/dev/null 2>&1; then
  CACHE_FLAGS=(-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache)
fi
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
# Validate the HCTree-specific loader and file format BEFORE
# compiling the Rust/RocksDB backend or starting 60-second measurements.
for repl in off; do
  LD_LIBRARY_PATH="$LIB" "$OUT/hctree_bench" \
    --database="$OUT/hctree-smoke-$repl.db" --seconds=0.01 \
    --batch=1 --replication="$repl" > "$OUT/hctree-smoke-$repl.txt"
done
# Integrated comparison: identical Murmur+RocksDB+CRR workloads, changing
# only the SQLite engine. HC-tree native leader/follower journal stays disabled.
cmake -S "$ROOT" -B "$ROOT/build-hctree-integrated" -DCMAKE_BUILD_TYPE=Release "${CACHE_FLAGS[@]}" \
  -DSQLITE_MURMUR_BUILD_TESTS=OFF -DSQLITE_MURMUR_BUILD_BENCHMARKS=ON \
  -DSQLITE_MURMUR_ENABLE_CRSQLITE=ON \
  -DSQLITE_MURMUR_SQLITE_ENGINE=hctree \
  -DSQLITE_MURMUR_HCTREE_SOURCE_DIR="$OUT/hctree-src"
cmake --build "$ROOT/build-hctree-integrated" --target benchmark --parallel 2
# Fail quickly if the HC-tree URI, file format, or cr-sqlite integration fails.
for repl in off on; do
 echo "SMOKE integrated-hctree $repl"
 timeout --signal=TERM --kill-after=10s 180s "$ROOT/build-hctree-integrated/benchmark" --suite=single --backend=murmur --seconds=0.01 --batch-size=1 --replication="$repl" 2>&1 | tee "$OUT/integrated-smoke-$repl.txt"
done
cmake -S "$ROOT" -B "$ROOT/build-hctree-comparison" -DCMAKE_BUILD_TYPE=Release "${CACHE_FLAGS[@]}" \
  -DSQLITE_MURMUR_BUILD_TESTS=OFF -DSQLITE_MURMUR_BUILD_BENCHMARKS=ON \
  -DSQLITE_MURMUR_ENABLE_CRSQLITE=ON
cmake --build "$ROOT/build-hctree-comparison" --target benchmark --parallel 2
# Murmur is measured with change tracking off and on. HCTree is storage-only;
# the on cases are not HCTree+Murmur integration.
# HCTree native leader/follower replication is never enabled.
for batch in 1 100; do
 for repl in off on; do
  key="$repl-$batch"
  echo "RUN murmur $key"
  timeout --signal=TERM --kill-after=10s 300s "$ROOT/build-hctree-comparison/benchmark" --suite=single --backend=murmur --seconds="$DURATION" --batch-size="$batch" --replication="$repl" | tee "$OUT/murmur-$key.txt"
 done
 echo "RUN hctree off-$batch"
 LD_LIBRARY_PATH="$LIB" "$OUT/hctree_bench" --database="$OUT/hctree-off-$batch.db" --seconds="$DURATION" --batch="$batch" --replication=off | tee "$OUT/hctree-off-$batch.txt"
done
for repl in off on; do
 for batch in 1 100; do
  echo "RUN integrated-hctree $repl-$batch"
  timeout --signal=TERM --kill-after=10s 300s "$ROOT/build-hctree-integrated/benchmark" --suite=single --backend=murmur --seconds="$DURATION" --batch-size="$batch" --replication="$repl" | tee "$OUT/integrated-hctree-$repl-$batch.txt"
 done
done
python3 "$ROOT/benchmarks/hctree/report.py" "$OUT"
