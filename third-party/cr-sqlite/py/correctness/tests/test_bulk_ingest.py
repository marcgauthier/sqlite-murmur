"""Equivalence of the grouped bulk/blob ingest paths with row-by-row merge.

`crsql_ingest_begin/commit` (staging) and `crsql_ingest_blob` (packed blob)
must produce byte-identical base rows, logical clocks, and db versions to
applying the same changes one `INSERT INTO crsql_changes` at a time, under
arrival, reversed, shuffled, and duplicated orderings.
"""
import random

from crsql_correctness import close, connect
from crsql_correctness.bulk import ingest_changes_blob, pack_changes

TABLES = ("t1", "t2", "t3", "pkonly", "wide")


def mkschema(c):
    c.execute("CREATE TABLE t1 (a primary key not null, b, c)")
    c.execute("CREATE TABLE t2 (a not null, b not null, v, primary key (a, b))")
    c.execute("CREATE TABLE t3 (id primary key not null, d default 'dd', e default 7)")
    c.execute("CREATE TABLE pkonly (id primary key not null)")
    wide_cols = ", ".join("w%d" % i for i in range(12))
    c.execute("CREATE TABLE wide (id primary key not null, %s)" % wide_cols)
    for t in TABLES:
        c.execute("SELECT crsql_as_crr('%s')" % t)
    c.commit()


def rand_val(rng):
    r = rng.random()
    if r < 0.15:
        return None
    if r < 0.35:
        return rng.randint(-10**12, 10**12)
    if r < 0.45:
        return rng.choice([0.0, -1.5, 3.14159, 1e300, float("inf")])
    if r < 0.60:
        return bytes(rng.randrange(256) for _ in range(rng.randint(0, 8)))
    if r < 0.65:
        # Affinity-sensitive: numeric-looking text must stay text.
        return rng.choice(["1", "007", "1.5", " 2", "0x10"])
    return "".join(rng.choice("abZ019 _-") for _ in range(rng.randint(0, 6)))


def gen_ops(rng, nops):
    ops = []
    ids = list(range(30))
    for _ in range(nops):
        t = rng.random()
        i = rng.choice(ids)
        if t < 0.35:
            ops.append(("INSERT OR REPLACE INTO t1 VALUES (?,?,?)",
                        (i, rand_val(rng), rand_val(rng))))
        elif t < 0.50:
            ops.append(("UPDATE t1 SET b=? WHERE a=?", (rand_val(rng), i)))
            ops.append(("UPDATE t1 SET c=? WHERE a=?", (rand_val(rng), i)))
        elif t < 0.58:
            ops.append(("DELETE FROM t1 WHERE a=?", (i,)))
        elif t < 0.68:
            ops.append(("INSERT OR REPLACE INTO t2 VALUES (?,?,?)",
                        (i % 7, i % 5, rand_val(rng))))
        elif t < 0.74:
            ops.append(("DELETE FROM t2 WHERE a=? AND b=?", (i % 7, i % 5)))
        elif t < 0.82:
            ops.append(("INSERT OR REPLACE INTO t3 (id) VALUES (?)", (i,)))
            ops.append(("UPDATE t3 SET d=? WHERE id=?", (rand_val(rng), i)))
        elif t < 0.86:
            ops.append(("INSERT OR IGNORE INTO pkonly VALUES (?)", (i,)))
            if rng.random() < 0.5:
                ops.append(("DELETE FROM pkonly WHERE id=?", (i,)))
        else:
            col = "w%d" % rng.randrange(12)
            ops.append(("INSERT OR IGNORE INTO wide (id) VALUES (?)", (i,)))
            ops.append(("UPDATE wide SET %s=? WHERE id=?" % col,
                        (rand_val(rng), i)))
    return ops


def ingest_rowby(c, changes):
    for ch in changes:
        c.execute("INSERT INTO crsql_changes VALUES (?,?,?,?,?,?,?,?,?)", ch)
    c.commit()


def ingest_bulk(c, changes):
    c.execute("SELECT crsql_ingest_begin()")
    if changes:
        c.executemany(
            "INSERT INTO crsql_staging VALUES (?,?,?,?,?,?,?,?,?)", changes)
    n = c.execute("SELECT crsql_ingest_commit()").fetchone()[0]
    c.commit()
    return n


def ingest_blob(c, changes):
    n = ingest_changes_blob(c, pack_changes(changes))
    c.commit()
    return n


def full_state(c):
    out = {}
    pks = {"t1": ["a"], "t2": ["a", "b"], "t3": ["id"],
           "pkonly": ["id"], "wide": ["id"]}
    for t in TABLES:
        cols = pks[t]
        out[t] = c.execute(
            "SELECT * FROM %s ORDER BY %s" % (t, ",".join(cols))).fetchall()
        pksel = ", ".join("p.%s" % p for p in cols)
        out[t + "_clk"] = c.execute(
            "SELECT %s, c.col_name, c.col_version, c.db_version, c.seq, s.site_id "
            "FROM %s__crsql_clock c JOIN %s__crsql_pks p ON c.key = p.__crsql_key "
            "LEFT JOIN crsql_site_id s ON c.site_id = s.ordinal "
            "ORDER BY %s, c.col_name" % (pksel, t, t, pksel)).fetchall()
    out["ver"] = c.execute("SELECT crsql_db_version()").fetchone()[0]
    out["rows_impacted"] = c.execute("SELECT crsql_rows_impacted()").fetchone()[0]
    return out


def reorder(changes, seed, order):
    if order == "arrival":
        return list(changes)
    if order == "reverse":
        return list(reversed(changes))
    rng = random.Random(seed)
    out = list(changes)
    if order == "shuffle":
        rng.shuffle(out)
        return out
    assert order == "dupe"
    if out:
        out = out + [rng.choice(out) for _ in range(len(out) // 3)]
    return out


def check_equivalence(seed, order):
    rng = random.Random(seed)
    ops = gen_ops(rng, 150)
    src = connect(":memory:")
    mkschema(src)
    for sql, args in ops:
        src.execute(sql, args)
    src.commit()
    changes = reorder(
        src.execute("SELECT * FROM crsql_changes").fetchall(), seed, order)

    a = connect(":memory:")
    mkschema(a)
    ingest_rowby(a, changes)
    sa = full_state(a)

    b = connect(":memory:")
    mkschema(b)
    n_bulk = ingest_bulk(b, changes)
    sb = full_state(b)

    g = connect(":memory:")
    mkschema(g)
    n_blob = ingest_blob(g, changes)
    sg = full_state(g)

    for c in (src, a, b, g):
        close(c)

    assert n_bulk == n_blob
    assert sb == sa
    assert sg == sa


def test_bulk_blob_equivalence_orders():
    for seed in (11, 12):
        for order in ("arrival", "reverse", "shuffle", "dupe"):
            check_equivalence(seed, order)


def test_null_site_merge_matches():
    # Hand-crafted NULL-site changes (as in rows-impacted.test.c): legacy
    # OR REPLACE maps the NULL site to the DEFAULT 0; all three ingest
    # paths must store the same 0 and count the row.
    changes = [
        ("t1", b"\x01\x09\x01", "b", 2, 1, 1, None, 1, 1),
        ("t1", b"\x01\x09\x02", "c", 3, 1, 1, None, 1, 2),
    ]
    states = []
    for mode in ("rowby", "bulk", "blob"):
        c = connect(":memory:")
        mkschema(c)
        c.execute("BEGIN")
        if mode == "rowby":
            for ch in changes:
                c.execute(
                    "INSERT INTO crsql_changes VALUES (?,?,?,?,?,?,?,?,?)", ch)
        elif mode == "bulk":
            c.execute("SELECT crsql_ingest_begin()")
            c.executemany(
                "INSERT INTO crsql_staging VALUES (?,?,?,?,?,?,?,?,?)", changes)
            c.execute("SELECT crsql_ingest_commit()").fetchone()
        else:
            ingest_changes_blob(c, pack_changes(changes))
        impacted = c.execute("SELECT crsql_rows_impacted()").fetchone()[0]
        c.execute("COMMIT")
        assert impacted == 2
        st = full_state(c)
        # Ordinal 0 is the per-database local site row (random id per
        # :memory: db), so compare the stored ordinal, not the joined blob.
        st["t1_clk"] = c.execute(
            "SELECT p.a, c.col_name, c.col_version, c.db_version, c.seq, c.site_id "
            "FROM t1__crsql_clock c JOIN t1__crsql_pks p ON c.key = p.__crsql_key "
            "ORDER BY p.a, c.col_name").fetchall()
        states.append(st)
        close(c)
    assert states[1] == states[0]
    assert states[2] == states[0]
    assert states[0]["t1"] == [(1, 2, None), (2, None, 3)]
    assert states[0]["rows_impacted"] == 0
    # NULL sites stored as ordinal 0 on all paths.
    assert [r[5] for r in states[0]["t1_clk"]] == [0, 0]


def test_empty_batch():
    c = connect(":memory:")
    mkschema(c)
    assert ingest_bulk(c, []) == 0
    assert ingest_blob(c, []) == 0
    assert c.execute("SELECT * FROM t1").fetchall() == []
    close(c)


def test_blob_rejects_garbage():
    c = connect(":memory:")
    mkschema(c)
    for bad in (b"\x00" * 16, b"CRSB" + b"\x00" * 100,
                pack_changes([]) + b"trailing"):
        try:
            c.execute("SELECT crsql_ingest_blob(?)", (bad,)).fetchone()
            raised = False
        except Exception:
            raised = True
        assert raised
    c.rollback()
    close(c)


def test_failed_commit_preserves_staging():
    c = connect(":memory:")
    mkschema(c)
    c.execute("SELECT crsql_ingest_begin()")
    c.execute("INSERT INTO crsql_staging VALUES "
              "('no_such_table', x'00', 'b', 1, 1, 1, x'00', 1, 1)")
    try:
        c.execute("SELECT crsql_ingest_commit()").fetchone()
        raised = False
    except Exception:
        raised = True
    assert raised
    # Staging is only cleared on success; the bad row is still inspectable.
    assert c.execute("SELECT COUNT(*) FROM crsql_staging").fetchone()[0] == 1
    c.rollback()
    close(c)
