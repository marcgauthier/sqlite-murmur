#!/usr/bin/env python3
"""Compare C++ CRSB serialization byte-for-byte with the vendored Python API."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True

reference = Path(__file__).resolve().parents[1] / "third-party/cr-sqlite/py/correctness/src/crsql_correctness/bulk.py"
spec = importlib.util.spec_from_file_location("crsql_bulk", reference)
bulk = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bulk)
rows = [
    ("t", b"\x00\x01", "c", None, None, None, None, None, None),
    ("t", b"\xff", "c", -(2**63), 1, 2, b"\x00\xff", 3, 4),
    ("t", b"", "c", 1.25, 1, 2, None, 3, 4),
    ("é", b"", "c", "é\x00z", 1, 2, None, 3, 4),
    ("t", b"", "c", b"", 1, 2, None, 3, 4),
    ("t", b"", "c", "", 1, 2, None, 3, 4),
    ("t", b"", "c", b"\x00\xff", 1, 2, None, 3, 4),
]
with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "fixture.crsb"
    subprocess.run([sys.argv[1], "--pack-fixture", str(path)], check=True)
    assert path.read_bytes() == bulk.pack_changes(rows)
print("C++ CRSB v1 matches Python pack_changes byte-for-byte.")
