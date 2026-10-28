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
    sources = json.loads((root / "sources.json").read_text())
    return {str(path.relative_to(root)): path
            for source in sources for path in (root / source["name"]).rglob("*")
            if path.is_file() or path.is_symlink()}


def main():
    root = Path(__file__).resolve().parent
    expected = dict(line.split("  ", 1)[::-1]
                    for line in (root / "source-files.sha256").read_text().splitlines())
    actual = files(root)
    failures = [f"missing: {name}" for name in expected.keys() - actual.keys()]
    failures += [f"unexpected: {name}" for name in actual.keys() - expected.keys()]
    failures += [f"changed: {name}" for name in expected.keys() & actual.keys()
                 if digest(actual[name]) != expected[name]]
    if failures:
        raise SystemExit("\n".join(sorted(failures)))
    print(f"Verified {len(actual)} vendored source files.")


if __name__ == "__main__":
    main()
