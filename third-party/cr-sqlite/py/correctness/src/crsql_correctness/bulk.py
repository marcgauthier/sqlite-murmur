"""Packer for crsql_ingest_blob: crsql_changes rows -> packed v1 blob.

Row layout mirrors `SELECT * FROM crsql_changes` column order:
(tbl, pk, cid, val, col_vrsn, db_vrsn, site_id, cl, seq).
"""
import struct

_MAGIC = 0x42535243  # 'CRSB' little-endian
_VERSION = 1

_HDR = struct.Struct("<III")
_U32 = struct.Struct("<I")
_I64 = struct.Struct("<q")
_F64 = struct.Struct("<d")

_NULL, _INT, _FLOAT, _TEXT, _BLOB = 0, 1, 2, 3, 4


def _pack_bytes(buf, b):
    buf += _U32.pack(len(b))
    buf += b


def pack_changes(changes):
    """Pack an iterable of 9-tuples into a v1 ingest blob (bytes)."""
    rows = list(changes)
    buf = bytearray()
    buf += _HDR.pack(_MAGIC, _VERSION, len(rows))
    for (tbl, pk, cid, val, col_vrsn, db_vrsn, site_id, cl, seq) in rows:
        tbl_b = tbl.encode("utf-8") if isinstance(tbl, str) else bytes(tbl)
        _pack_bytes(buf, tbl_b)
        _pack_bytes(buf, bytes(pk))
        cid_b = cid.encode("utf-8") if isinstance(cid, str) else bytes(cid)
        _pack_bytes(buf, cid_b)
        if val is None:
            buf += bytes((_NULL,))
        elif isinstance(val, bool):
            buf += bytes((_INT,)) + _I64.pack(int(val))
        elif isinstance(val, int):
            buf += bytes((_INT,)) + _I64.pack(val)
        elif isinstance(val, float):
            buf += bytes((_FLOAT,)) + _F64.pack(val)
        elif isinstance(val, str):
            buf += bytes((_TEXT,))
            _pack_bytes(buf, val.encode("utf-8"))
        elif isinstance(val, (bytes, bytearray, memoryview)):
            buf += bytes((_BLOB,))
            _pack_bytes(buf, bytes(val))
        else:
            raise TypeError(f"unsupported staged value type: {type(val)!r}")
        # NULL versions/site mirror row-by-row xUpdate, which reads them
        # via int64()/blob() and sees 0/empty for NULL.
        buf += _I64.pack(col_vrsn if col_vrsn is not None else 0)
        buf += _I64.pack(db_vrsn if db_vrsn is not None else 0)
        _pack_bytes(buf, b"" if site_id is None else bytes(site_id))
        buf += _I64.pack(cl if cl is not None else 0)
        buf += _I64.pack(seq if seq is not None else 0)
    return bytes(buf)


def ingest_changes_blob(dst, blob):
    """Apply a packed blob on dst, returning the applied count."""
    return dst.execute("SELECT crsql_ingest_blob(?)", (blob,)).fetchone()[0]
