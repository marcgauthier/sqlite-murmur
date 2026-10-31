#!/usr/bin/env python3
"""Stage pinned HC-tree for smaller mappings; NOT a Murmur storage adapter.

HC-tree maps 1024 x 2 MiB (2 GiB) per data-file region by default.
An eventual copy-backed or shared-shadow adapter must operate on bounded
chunks. This strictly pinned source change reduces the mapping quantum to
one 2 MiB chunk. All POSIX I/O and MAP_SHARED behavior remain unchanged.
Do not enable the integrated benchmark on the strength of this patch.
"""
import argparse
import hashlib
from pathlib import Path
import shutil

PINNED_REV = "e3820e788797940ae4556b35f7c6fd190d61d852c30637b9d3ab583b91223c5a"
PINNED_HCT_FILE_SHA256 = "4425dd504377c35209ddd4b350d66df0fa7a81a8bea444910b6f98f83be69070"
ORIGINAL = b"#define HCT_MMAP_QUANTA          1024"
REPLACEMENT = b"#define HCT_MMAP_QUANTA          1 /* HCT_MURMUR_STAGING_SMALL_MAPPINGS */"


def prepare(source: Path, output: Path) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("refusing to modify pinned source in place")
    revision = (source / "manifest.uuid").read_text().strip()
    if revision != PINNED_REV:
        raise ValueError(f"unexpected HC-tree revision: {revision}")
    data = (source / "src" / "hct_file.c").read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != PINNED_HCT_FILE_SHA256:
        raise ValueError(f"HC-tree source changed unexpectedly: {digest}")
    if data.count(ORIGINAL) != 1:
        raise ValueError("mapping quantum definition missing or ambiguous")
    if output.exists():
        raise ValueError(f"output already exists: {output}")
    shutil.copytree(source, output)
    patched = data.replace(ORIGINAL, REPLACEMENT)
    (output / "src" / "hct_file.c").write_bytes(patched)
    # The full benchmark checks this exact marker in hct_file.c. This staging
    # patch must NOT satisfy it before both stores actually use Murmur VFS.
    if b"HCTREE_MURMUR_IO_ADAPTER" in patched:
        raise ValueError("staged source unexpectedly enables full benchmark")
    print(f"HCTREE_STAGING_READY,revision={revision},quantum=1,source_sha256={digest}")
    print("HCTREE_INTEGRATION_BLOCKED,POSIX_I/O_AND_MAP_SHARED_NOT_ADAPTED")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source, args.output)


if __name__ == "__main__":
    main()
