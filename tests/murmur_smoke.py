#!/usr/bin/env python3
"""Real TLS/QUIC replication and kill/restart tests. No Python SQLite ABI involved."""
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
    for i in range(1,6):
        prefix=root/str(i)
        openssl('req','-new','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256','-nodes','-keyout',str(prefix)+'.key','-out',str(prefix)+'.csr','-subj',f'/CN=node-{i}')
        ext=root/f'{i}.ext'
        ext.write_text(f'subjectAltName=URI:replicateddb://node/{i:032x}\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n')
        openssl('x509','-req','-in',str(prefix)+'.csr','-CA',root/'ca.pem','-CAkey',root/'ca.key','-CAcreateserial','-out',str(prefix)+'.pem','-days','2','-extfile',ext)


class Node:
    def __init__(self, root, number, port, seeds, libs, existing=False, group='1'*32, schema=None, start=True):
        self.path=root/f'db-{number}'
        self.p=subprocess.Popen([sys.argv[1],str(self.path),*libs],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        assert self.line()=='READY', self.p.stderr.read() if self.p.poll() is not None else 'not ready'
        if not existing:
            self.sql(schema or "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body DEFAULT '',payload); SELECT crsql_as_crr('main','records')")
            for name,value in [('group_id',group),('bind',f'127.0.0.1:{port}'),('advertise',f'127.0.0.1:{port}'),('bootstrap',json.dumps([f'127.0.0.1:{p}' for p in seeds])),('certificate',str(root/f'{number}.pem')),('private_key',str(root/f'{number}.key')),('ca',str(root/'ca.pem')),('batch_bytes','65536'),('max_batch_bytes','1048576'),('anti_entropy_ms','200')]:
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
    with tempfile.TemporaryDirectory(prefix='murmur-test-') as directory:
        root=pathlib.Path(directory);certificates(root)
        sockets=[socket.socket(socket.AF_INET,socket.SOCK_DGRAM) for _ in range(5)]
        for s in sockets:s.bind(('127.0.0.1',0))
        ports=[s.getsockname()[1] for s in sockets]
        for s in sockets:s.close()
        nodes=[]
        try:
            a=Node(root,1,ports[0],[],libs);nodes.append(a)
            b=Node(root,2,ports[1],[ports[0]],libs);nodes.append(b)
            c=Node(root,3,ports[2],[ports[1]],libs);nodes.append(c)
            wait(lambda:all(int(n.sql('SELECT count(*) FROM murmur_peers WHERE connected'))>=2 for n in [a,b,c]),nodes)
            a.sql("PRAGMA murmur_unknown='x'",fail=True)
            a.sql("PRAGMA murmur_poll_ms='10'",fail=True)
            assert a.sql('PRAGMA murmur_poll_ms')=='100'
            a.sql("BEGIN; INSERT INTO records VALUES(999,'rollback',NULL); ROLLBACK")
            a.sql("INSERT INTO records VALUES(1,'from a',x'00ff')")
            b.sql("INSERT INTO records VALUES(2,'from b',NULL)")
            c.sql("INSERT INTO records VALUES(3,'from c','1')")
            wait(lambda:all(n.sql('SELECT count(*) FROM records')=='3' for n in [a,b,c]),nodes)
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
            d.sql('SELECT murmur_stop()')
            a.sql('SELECT murmur_stop()')
            assert a.sql('SELECT murmur_stop()')=='0'
            a.sql("PRAGMA murmur_poll_ms='0'",fail=True)
            a.sql("BEGIN; PRAGMA murmur_poll_ms=50",fail=True)
            a.sql('ROLLBACK')
            a.sql("PRAGMA murmur_poll_ms='50'")
            a.sql('SELECT murmur_start()')
            wait(lambda:int(json.loads(a.sql('SELECT murmur_status()'))['connected_peers'])>=1,nodes)
            print('Verified SQL-only configuration, three-node QUIC/SWIM discovery, conflicts, chunking, paging, forwarding, restart and group isolation.')
        finally:
            for n in reversed(nodes):n.close()


if __name__=='__main__':main()
