#!/usr/bin/env python3
"""Verify the committed source inventory without downloading anything."""
import hashlib
import json
from pathlib import Path


def digest(path):
    if path.is_symlink():
        return hashlib.sha256(("symlink:" + str(path.readlink())).encode()).hexdigest()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def files(root):
    # root is the repo root; manifest lives in third-party/. Each unit resolves
    # under its "dir" (default "third-party"); entry names stay repo-relative.
    sources = json.loads((root / "third-party" / "sources.json").read_text())
    found = {str(path.relative_to(root)): path
             for source in sources
             for path in (root / source.get("dir", "third-party") / source["name"]).rglob("*")
             if path.is_file() or path.is_symlink()}
    # Cargo workspace manifest + lockfile (dependency pins stay checksummed).
    for name in ("crates/Cargo.toml", "crates/Cargo.lock"):
        path = root / name
        if path.is_file():
            found[name] = path
    return found


def main(regenerate=False):
    root = Path(__file__).resolve().parent.parent
    if regenerate:
        actual = files(root)
        lines = sorted(f"{digest(path)}  {name}" for name, path in actual.items())
        (root / "third-party" / "source-files.sha256").write_text("\n".join(lines) + "\n")
        print(f"Regenerated {len(lines)} entries.")
        return
    expected = dict(line.split("  ", 1)[::-1]
                    for line in (root / "third-party" / "source-files.sha256").read_text().splitlines())
    actual = files(root)
    failures = [f"missing: {name}" for name in expected.keys() - actual.keys()]
    failures += [f"unexpected: {name}" for name in actual.keys() - expected.keys()]
    failures += [f"changed: {name}" for name in expected.keys() & actual.keys()
                 if digest(actual[name]) != expected[name]]
    if failures:
        raise SystemExit("\n".join(sorted(failures)))
    print(f"Verified {len(actual)} vendored source files.")


if __name__ == "__main__":
    import sys
    main(regenerate="--regenerate" in sys.argv[1:])
