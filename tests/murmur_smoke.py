#!/usr/bin/env python3
"""Real TLS/QUIC replication and kill/restart tests. No Python SQLite ABI involved."""
import fcntl
import json
import pathlib
import select
import socket
import subprocess
import sys
import tempfile
import time


def openssl(*args):
    subprocess.run(['openssl', *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def certificates(root):
    openssl('req','-x509','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256','-nodes','-keyout',root/'ca.key','-out',root/'ca.pem','-days','2','-subj','/CN=Murmur Test CA','-addext','basicConstraints=critical,CA:TRUE')
    for i in range(1,7):
        prefix=root/str(i)
        openssl('req','-new','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256','-nodes','-keyout',str(prefix)+'.key','-out',str(prefix)+'.csr','-subj',f'/CN=node-{i}')
        ext=root/f'{i}.ext'
        ext.write_text(f'subjectAltName=URI:replicateddb://node/{i:032x}\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n')
        openssl('x509','-req','-in',str(prefix)+'.csr','-CA',root/'ca.pem','-CAkey',root/'ca.key','-CAcreateserial','-out',str(prefix)+'.pem','-days','2','-extfile',ext)


class Node:
    def __init__(self, root, number, port, seeds, libs, existing=False, group='1'*32, schema=None, start=True, seed_host='127.0.0.1'):
        self.path=root/f'db-{number}'
        self.p=subprocess.Popen([sys.argv[1],str(self.path),*libs],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        assert self.line()=='READY', self.p.stderr.read() if self.p.poll() is not None else 'not ready'
        if not existing:
            self.sql(schema or "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body DEFAULT '',payload); SELECT crsql_as_crr('main','records')")
            for name,value in [('group_id',group),('bind',f'127.0.0.1:{port}'),('advertise',f'127.0.0.1:{port}'),('bootstrap',json.dumps([f'{seed_host}:{p}' for p in seeds])),('certificate',str(root/f'{number}.pem')),('private_key',str(root/f'{number}.key')),('ca',str(root/'ca.pem')),('batch_bytes','65536'),('max_batch_bytes','1048576'),('anti_entropy_ms','200')]:
                self.sql(f"PRAGMA murmur_{name}='{value}'")
        if start:self.sql('SELECT murmur_start()')

    def line(self):
        if not select.select([self.p.stdout],[],[],20)[0]:
            raise AssertionError('driver deadline')
        line=self.p.stdout.readline().strip()
        if not line:
            raise AssertionError(f'driver exited: {self.p.poll()} {self.p.stderr.read()}')
        return line

    def sql(self, query, fail=False):
        self.p.stdin.write(query+'\n');self.p.stdin.flush()
        answer=self.line()
        assert answer.startswith('ERR:' if fail else 'OK:'), (query,answer)
        return answer[4:] if fail else answer[3:]

    def close(self, kill=False):
        if self.p.poll() is None:
            if kill:self.p.kill()
            else:self.p.stdin.write('EXIT\n');self.p.stdin.flush()
            self.p.wait(timeout=20)
            if not kill:assert self.p.returncode==0,self.p.stderr.read()


def wait(predicate, nodes, seconds=20):
    until=time.monotonic()+seconds
    while time.monotonic()<until:
        if predicate():return
        time.sleep(.1)
    raise AssertionError([n.sql('SELECT murmur_status()') for n in nodes if n.p.poll() is None])


def main():
    libs=sys.argv[2:]
    lock_file=open('/tmp/sqlite_murmur_cluster.lock','a')
    fcntl.flock(lock_file.fileno(),fcntl.LOCK_EX)
    with tempfile.TemporaryDirectory(prefix='murmur-test-') as directory:
        root=pathlib.Path(directory);certificates(root)
        sockets=[socket.socket(socket.AF_INET,socket.SOCK_DGRAM) for _ in range(6)]
        for s in sockets:s.bind(('127.0.0.1',0))
        ports=[s.getsockname()[1] for s in sockets]
        for s in sockets:s.close()
        nodes=[]
        try:
            a=Node(root,1,ports[0],[],libs);nodes.append(a)
            # A6: b bootstraps via a DNS seed (resolved once at start).
            b=Node(root,2,ports[1],[ports[0]],libs,seed_host='localhost');nodes.append(b)
            c=Node(root,3,ports[2],[ports[1]],libs);nodes.append(c)
            wait(lambda:all(int(n.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=2 for n in [a,b,c]),nodes)
            if a.sql('SELECT murmur_test_integrated()')=='1':
                a.sql('@REOPEN')
                assert json.loads(a.sql('SELECT murmur_status()'))['running']
            a.sql("PRAGMA murmur_unknown='x'",fail=True)
            a.sql("PRAGMA murmur_poll_ms='10'",fail=True)
            assert a.sql('PRAGMA murmur_poll_ms')=='100'
            a.sql("BEGIN; INSERT INTO records VALUES(999,'rollback',NULL); ROLLBACK")
            a.sql("INSERT INTO records VALUES(1,'from a',x'00ff')")
            b.sql("INSERT INTO records VALUES(2,'from b',NULL)")
            c.sql("INSERT INTO records VALUES(3,'from c','1')")
            wait(lambda:all(n.sql('SELECT count(*) FROM records')=='3' for n in [a,b,c]),nodes)
            # A1: per-origin progress heads advance; waits observe them.
            for n, own in [(a,'01'),(b,'02'),(c,'03')]:
                wait(lambda node=n, o=own: len(json.loads(node.sql('SELECT murmur_progress()')))>=3 and any(h['origin'].endswith(o) and h['applied']>=1 for h in json.loads(node.sql('SELECT murmur_progress()'))), nodes)
                heads={h['origin']:h for h in json.loads(n.sql('SELECT murmur_progress()'))}
                assert heads['0'*30+own]['applied']>=1,(own,heads)
                assert len(heads)>=3,(own,heads)
            assert b.sql("SELECT murmur_wait_applied('00000000000000000000000000000001',1,5000)")=='1'
            assert a.sql('SELECT murmur_wait_lag(0,5000)')=='1'
            assert a.sql("SELECT murmur_wait_applied('00000000000000000000000000000001',999999999,200)")=='0'
            a.sql("SELECT murmur_wait_applied('zz',1,100)",fail=True)
            a.sql('SELECT murmur_wait_lag(-1,100)',fail=True)
            a.sql('BEGIN; SELECT murmur_wait_lag(0,100)',fail=True)
            a.sql('ROLLBACK')
            # A4: table flags gate both wire directions; scratch stays local.
            for n in [a,b,c]:
                n.sql("CREATE TABLE scratch(id INTEGER PRIMARY KEY NOT NULL,v DEFAULT ''); SELECT crsql_as_crr('main','scratch')")
            for n in [a,b,c]:
                n.sql('SELECT murmur_stop()')
                n.sql("PRAGMA murmur_table_replication='{\"scratch\":false}'")
                n.sql('SELECT murmur_start()')
            wait(lambda:all(int(n.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=2 for n in [a,b,c]),nodes)
            a.sql("INSERT INTO scratch VALUES(1,'a')")
            b.sql("INSERT INTO scratch VALUES(2,'b')")
            time.sleep(3)
            assert a.sql('SELECT count(*) FROM scratch')=='1'
            assert b.sql('SELECT count(*) FROM scratch')=='1'
            assert c.sql('SELECT count(*) FROM scratch')=='0'
            for n in [a,b,c]:
                n.sql('SELECT murmur_stop()')
                n.sql("PRAGMA murmur_table_replication='{}'")
                n.sql('SELECT murmur_start()')
            wait(lambda:all(int(n.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=2 for n in [a,b,c]),nodes)
            # Re-enabling publishes only subsequently written rows; rows
            # written while disabled stay local permanently (no backfill).
            a.sql("INSERT INTO scratch VALUES(3,'a2')")
            b.sql("INSERT INTO scratch VALUES(4,'b2')")
            wait(lambda:a.sql('SELECT count(*) FROM scratch')=='3' and b.sql('SELECT count(*) FROM scratch')=='3' and c.sql('SELECT count(*) FROM scratch')=='2',nodes)
            assert a.sql('SELECT count(*) FROM scratch WHERE id=2')=='0'
            assert b.sql('SELECT count(*) FROM scratch WHERE id=1')=='0'
            # Concurrent conflict + delete + a value spanning multiple QUIC chunks.
            a.sql("UPDATE records SET body='a conflict' WHERE id=1")
            b.sql("UPDATE records SET body='b conflict' WHERE id=1; DELETE FROM records WHERE id=2")
            c.sql("INSERT INTO records VALUES(4,'large',zeroblob(250000))")
            snapshot="SELECT group_concat(id||':'||quote(body)||':'||typeof(payload)||':'||coalesce(length(payload),-1),',') FROM (SELECT * FROM records ORDER BY id)"
            wait(lambda:len({n.sql(snapshot) for n in [a,b,c]})==1 and a.sql('SELECT count(*) FROM records')=='3',nodes)
            # Several export pages, plus forwarding without a direct A-C connection.
            a.sql("SELECT murmur_remove_peer('00000000000000000000000000000003')")
            c.sql("SELECT murmur_remove_peer('00000000000000000000000000000001')")
            a.sql("WITH RECURSIVE x(i) AS (VALUES(100) UNION ALL SELECT i+1 FROM x WHERE i<1300) INSERT INTO records SELECT i,printf('%0100d',i),NULL FROM x")
            wait(lambda:c.sql('SELECT count(*) FROM records')=='1204',nodes)
            # Durable ACK means a killed recipient can restart without losing data.
            wait(lambda:int(json.loads(c.sql('SELECT murmur_status()'))['applied_batches'])>0,nodes)
            expected=c.sql(snapshot)
            c.close(kill=True)
            a.sql("INSERT INTO records VALUES(5,'while c offline',NULL)")
            c=Node(root,3,ports[2],[],libs,existing=True);nodes.append(c)
            assert 'large' in expected
            wait(lambda:c.sql('SELECT count(*) FROM records')=='1205',nodes)
            wait(lambda:len({n.sql(snapshot) for n in [a,b,c]})==1,nodes)
            assert c.sql('PRAGMA murmur_group_id')=='1'*32
            # Fail the recipient's barrier after data+progress commit, before ACK.
            c.sql('SELECT murmur_test_fault(2)')
            a.sql("INSERT INTO records VALUES(7777,'unacknowledged barrier fault',NULL)")
            wait(lambda:c.sql('SELECT count(*) FROM records WHERE id=7777')=='1',nodes)
            wait(lambda:'persistence barrier failed' in (json.loads(c.sql('SELECT murmur_status()'))['last_error'] or ''),nodes)
            c.close(kill=True)
            c=Node(root,3,ports[2],[],libs,existing=True,start=False);nodes.append(c)
            assert c.sql('SELECT count(*) FROM records WHERE id=7777')=='0'
            c.sql('SELECT murmur_start()')
            wait(lambda:c.sql('SELECT count(*) FROM records WHERE id=7777')=='1',nodes)
            # A failed flush must not make the source forget the unacknowledged batch.
            wait(lambda:len({n.sql(snapshot) for n in [a,b,c]})==1,nodes)
            # Wrong replication groups must not receive CRR data.
            d=Node(root,4,ports[3],[ports[0]],libs,group='2'*32);nodes.append(d)
            wait(lambda:'mismatch' in (json.loads(d.sql('SELECT murmur_status()'))['last_error'] or ''),nodes)
            assert d.sql('SELECT count(*) FROM records')=='0'
            # Matching group with incompatible table DDL also rejects synchronization.
            e=Node(root,5,ports[4],[ports[0]],libs,schema="CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body DEFAULT '',payload,extra); SELECT crsql_as_crr('main','records')");nodes.append(e)
            wait(lambda:'schema mismatch' in (json.loads(e.sql('SELECT murmur_status()'))['last_error'] or ''),nodes)
            assert e.sql('SELECT count(*) FROM records')=='0'
            e.sql('SELECT murmur_stop()')
            # The allowlist is certificate identity based, not an address hint.
            e.sql("PRAGMA murmur_allowed_peers='[\"00000000000000000000000000000003\"]'")
            e.sql('SELECT murmur_start()')
            wait(lambda:'denied' in (json.loads(e.sql('SELECT murmur_status()'))['last_error'] or ''),nodes)
            assert e.sql('SELECT count(*) FROM records')=='0'
            e.sql('SELECT murmur_stop()')
            d.sql('SELECT murmur_stop()')
            a.sql('SELECT murmur_stop()')
            assert a.sql('SELECT murmur_stop()')=='0'
            a.sql("PRAGMA murmur_poll_ms='0'",fail=True)
            a.sql("BEGIN; PRAGMA murmur_poll_ms=50",fail=True)
            a.sql('ROLLBACK')
            a.sql("PRAGMA murmur_poll_ms='50'")
            a.sql('SELECT murmur_start()')
            wait(lambda:int(json.loads(a.sql('SELECT murmur_status()'))['connected_peers'])>=1,nodes)
            # A3b: snapshot join — a fresh node installs a donor snapshot,
            # then follow-up pulls advance past the snapshot boundary.
            # Schema DDL must match the donor byte-for-byte (records + scratch).
            f=Node(root,6,ports[5],[ports[0]],libs,schema="CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body DEFAULT '',payload); SELECT crsql_as_crr('main','records'); CREATE TABLE scratch(id INTEGER PRIMARY KEY NOT NULL,v DEFAULT ''); SELECT crsql_as_crr('main','scratch')");nodes.append(f)
            wait(lambda:int(f.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=1,nodes)
            f.sql('SELECT murmur_stop()')
            f.sql("PRAGMA murmur_snapshot_trusted_sources='[\"00000000000000000000000000000001\"]'")
            f.sql('SELECT murmur_start()')
            wait(lambda:int(f.sql("SELECT count(*) FROM murmur_peers WHERE connected AND node_id='00000000000000000000000000000001'"))>=1,nodes)
            def snapshot_done(node, donor):
                st = json.loads(node.sql('SELECT murmur_status()'))['snapshot'] or ''
                if st.startswith('ok '): return True
                if st.startswith('error:'):
                    node.sql(f"SELECT murmur_snapshot_request('{donor}')")
                return False
            assert f.sql("SELECT murmur_snapshot_request('00000000000000000000000000000001')")=='1'
            wait(lambda:snapshot_done(f, '00000000000000000000000000000001'),nodes,seconds=60)
            assert f.sql('SELECT count(*) FROM records')==a.sql('SELECT count(*) FROM records')
            assert f.sql(snapshot)==a.sql(snapshot)
            a.sql("INSERT INTO records VALUES(9999,'post snapshot',NULL)")
            wait(lambda:f.sql('SELECT count(*) FROM records WHERE id=9999')=='1',nodes)
            # A5b: replicated schema — master DDL adopts; destructive never
            # does. a is self-gated master (adopts from nobody); b/c adopt.
            a.sql('SELECT murmur_stop()')
            a.sql("PRAGMA murmur_schema_master='00000000000000000000000000000001'")
            a.sql("PRAGMA murmur_schema_master_enabled='1'")
            a.sql('SELECT murmur_start()')
            wait(lambda:int(a.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=1,nodes)
            a.sql("CREATE TABLE adopted1(id INTEGER PRIMARY KEY NOT NULL,v DEFAULT ''); SELECT crsql_as_crr('main','adopted1')")
            wait(lambda:all(n.sql("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='adopted1'")=='1' for n in [b,c,f]),nodes,seconds=60)
            wait(lambda:int(json.loads(b.sql('SELECT murmur_status()'))['schema_adoptions'])>=1,nodes,seconds=60)
            assert int(json.loads(b.sql('SELECT murmur_status()'))['schema_adoptions'])>=1
            a.sql("INSERT INTO adopted1 VALUES(1,'via schema')")
            wait(lambda:all(n.sql('SELECT count(*) FROM adopted1')=='1' for n in [b,c,f]),nodes,seconds=60)
            a.sql("ALTER TABLE adopted1 ADD COLUMN w DEFAULT 'x'")
            wait(lambda:all(n.sql("SELECT count(*) FROM pragma_table_info('adopted1') WHERE name='w'")=='1' for n in [b,c,f]),nodes,seconds=60)
            a.sql("DROP TABLE adopted1")
            time.sleep(3)
            assert b.sql("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='adopted1'")=='1'
            assert c.sql('SELECT count(*) FROM adopted1')=='1'
            assert f.sql('SELECT count(*) FROM adopted1')=='1'
            assert a.sql("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='adopted1'")=='0'
            # A2b: tombstone GC — deletes replicate, then prune locally.
            # Minimum-kept tail drops to 1 so test-scale versions prune.
            # Align schemas (drop leftover adopted1 on keepers) and reconnect A-C
            # so the 4-node cluster has uniform schemas and full direct peering.
            for n in [b,c,f]:
                n.sql("DROP TABLE adopted1")
            a.sql("DELETE FROM murmur_excluded")
            c.sql("DELETE FROM murmur_excluded")
            for n in [a,b,c,f]:
                n.sql('SELECT murmur_stop()')
                n.sql("PRAGMA murmur_retention_min_kept='1'")
                n.sql('SELECT murmur_start()')
            wait(lambda:all(int(n.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=2 for n in [a,b,c]) and int(f.sql("SELECT count(*) FROM murmur_peers WHERE connected AND node_id='00000000000000000000000000000001'"))>=1,nodes,seconds=60)
            a.sql('DELETE FROM records WHERE id>=100 AND id<200')
            wait(lambda:b.sql('SELECT count(*) FROM records')==c.sql('SELECT count(*) FROM records')==f.sql('SELECT count(*) FROM records')==a.sql('SELECT count(*) FROM records'),nodes,seconds=60)
            a.sql("INSERT INTO records VALUES(7999,'advance version past deletes',NULL)")
            wait(lambda:b.sql('SELECT count(*) FROM records WHERE id=7999')=='1' and c.sql('SELECT count(*) FROM records WHERE id=7999')=='1' and f.sql('SELECT count(*) FROM records WHERE id=7999')=='1',nodes,seconds=60)
            time.sleep(2)
            before=int(a.sql("SELECT count(*) FROM records__crsql_clock WHERE col_name='-1'"))
            assert before>=100
            print("TOMBSTONE VERSIONS BEFORE GC:", a.sql("SELECT db_version, count(*) FROM records__crsql_clock WHERE col_name='-1' GROUP BY db_version"), flush=True)
            def decode_varint(data, offset):
                res = 0
                shift = 0
                while True:
                    b = data[offset]
                    offset += 1
                    res |= (b & 0x7f) << shift
                    if (b & 0x80) == 0:
                        break
                    shift += 7
                return res, offset

            def decode_peer_retention(hex_val):
                raw = bytes.fromhex(hex_val)
                node = raw[:16].hex()
                offset = 16
                last_seen, offset = decode_varint(raw, offset)
                retired = bool(raw[offset])
                offset += 1
                ack_count, offset = decode_varint(raw, offset)
                acks = {}
                for _ in range(ack_count):
                    origin = raw[offset:offset+16].hex()
                    offset += 16
                    seq, offset = decode_varint(raw, offset)
                    acks[origin] = seq
                return {"node": node, "last_seen": last_seen, "retired": retired, "acks": acks}

            t0 = time.monotonic()
            last_dump = [0]
            def poll_gc():
                for n in [a,b,c,f]:
                    n.sql('SELECT murmur_sync()')
                res = int(a.sql('SELECT murmur_gc()'))
                if res > 0:
                    return True
                now = time.monotonic()
                if now - t0 > 20 and now - last_dump[0] > 5:
                    last_dump[0] = now
                    print(f"\n=== GC STALL DUMP at {now - t0:.1f}s ===", flush=True)
                    for idx, n in [('a', a), ('b', b), ('c', c), ('f', f)]:
                        print(f"Node {idx} status: {n.sql('SELECT murmur_status()')}", flush=True)
                        print(f"Node {idx} peers: {n.sql('SELECT node_id, address, state, connected, incarnation FROM murmur_peers')}", flush=True)
                        kv_raw = n.sql("SELECT quote(key), hex(value) FROM murmur_kv WHERE key GLOB '07/replication/retention/peers/*'")
                        kv_rows = [r.split('~') for r in kv_raw.split('|') if r and '~' in r]
                        decoded_peers = []
                        for k, v in kv_rows:
                            try:
                                decoded_peers.append((k, decode_peer_retention(v)))
                            except Exception as e:
                                decoded_peers.append((k, f"err: {e}"))
                        print(f"Node {idx} retention peers: {decoded_peers}", flush=True)
                return False
            wait(poll_gc,nodes,seconds=60)
            after=int(a.sql("SELECT count(*) FROM records__crsql_clock WHERE col_name='-1'"))
            assert after<before
            assert int(a.sql('SELECT count(*) FROM records__crsql_clock'))>0
            a.sql("INSERT INTO records VALUES(8888,'post gc',NULL)")
            wait(lambda:b.sql('SELECT count(*) FROM records WHERE id=8888')=='1' and c.sql('SELECT count(*) FROM records WHERE id=8888')=='1',nodes,seconds=60)
            # Snapshots bypass the GC watermark (frontier state by design).
            # Donor is b with uniform post-G11b schema (adopted1 dropped on
            # all keepers above): drop-diverged snapshot donors still need
            # per-table filtering (known limitation, same class as
            # keeper->dropper pulls). Best-effort prune on b first so the
            # snapshot also exercises a watermark-advanced donor.
            b.sql('SELECT murmur_gc()')
            f.sql('SELECT murmur_stop()')
            f.sql("PRAGMA murmur_snapshot_trusted_sources='[\"00000000000000000000000000000001\",\"00000000000000000000000000000002\"]'")
            f.sql('SELECT murmur_start()')
            wait(lambda:int(f.sql("SELECT count(*) FROM murmur_peers WHERE connected AND node_id='00000000000000000000000000000002'"))>=1,nodes)
            assert f.sql("SELECT murmur_snapshot_request('00000000000000000000000000000002')")=='1'
            wait(lambda:snapshot_done(f, '00000000000000000000000000000002'),nodes,seconds=60)
            # B7: merge policies — counter ops on two nodes converge via
            # carriers; recompute restores a clobbered projection.
            for n in [a,b]:
                n.sql("CREATE TABLE votes(id INTEGER PRIMARY KEY NOT NULL,n INTEGER DEFAULT 0); SELECT crsql_as_crr('main','votes')")
                n.sql('SELECT murmur_stop()')
                n.sql("PRAGMA murmur_column_policies='{\"votes.n\":\"Counter\"}'")
                n.sql('SELECT murmur_start()')
            wait(lambda:all(int(n.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=1 for n in [a,b]),nodes)
            a.sql("INSERT INTO votes VALUES(1,0)")
            wait(lambda:b.sql('SELECT count(*) FROM votes')=='1',nodes)
            assert a.sql("SELECT murmur_policy_op('votes','n',1,'add',5)")=='5'
            assert b.sql("SELECT murmur_policy_op('votes','n',1,'add',7)")=='7'
            wait(lambda:a.sql('SELECT n FROM votes WHERE id=1')=='12' and b.sql('SELECT n FROM votes WHERE id=1')=='12',nodes,seconds=60)
            a.sql("UPDATE votes SET n=999 WHERE id=1")
            assert a.sql("SELECT murmur_policy_recompute('votes','n',1)")=='12'
            assert a.sql('SELECT n FROM votes WHERE id=1')=='12'
            a.sql("SELECT murmur_policy_op('votes','id',1,'add',1)",fail=True)
            a.sql("SELECT murmur_policy_op('records','body',1,'add',1)",fail=True)
            # B8: High/Low ownership — grant/release/projection round trip.
            assert a.sql("SELECT murmur_hl_projection('records',1,'body')")=='low'
            assert 'owned' in a.sql("SELECT murmur_hl_grant('records',1,'body')")
            assert a.sql("SELECT murmur_hl_projection('records',1,'body')").startswith('high:')
            assert 'false' in a.sql("SELECT murmur_hl_release('records',1,'body')")
            assert a.sql("SELECT murmur_hl_projection('records',1,'body')")=='low'
            assert 'owned' in a.sql("SELECT murmur_hl_grant('records',1,'')")
            assert a.sql("SELECT murmur_hl_projection('records',1,'body')")=='suppress-row'
            assert a.sql("SELECT murmur_hl_projection('records',1,'')")=='owned'
            assert a.sql('SELECT murmur_hl_reduce()')>='1'
            a.sql("SELECT murmur_hl_grant('records','x','body')",fail=True)
            a.sql("SELECT murmur_hl_release('records',999999,'body')",fail=True)
            print('Verified SQL-only configuration, three-node QUIC/SWIM discovery, conflicts, chunking, paging, forwarding, restart, group isolation, snapshot join, replicated schema, DNS seeds, tombstone GC, merge policies and High/Low ownership.')
        finally:
            for n in reversed(nodes):n.close()


if __name__=='__main__':main()
