#!/usr/bin/env python3
"""Shared test cluster harness for sqlite-murmur mesh scenarios (WP-G)."""

import json
import os
import pathlib
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional


def find_test_binaries():
    """Locate driver and extension shared library."""
    workspace = pathlib.Path(__file__).resolve().parent.parent
    driver_candidates = [
        workspace / 'build-a1' / 'sqlite_murmur_murmur_test',
        workspace / 'build' / 'sqlite_murmur_murmur_test',
    ]
    crsql_candidates = [
        workspace / 'build-a1' / 'libsqlite_murmur_crsqlite.so',
        workspace / 'build' / 'libsqlite_murmur_crsqlite.so',
    ]
    driver = next((p for p in driver_candidates if p.exists()), None)
    crsql = next((p for p in crsql_candidates if p.exists()), None)
    if not driver:
        raise RuntimeError(f"Driver binary not found in {driver_candidates}")
    return str(driver), str(crsql) if crsql else None


def generate_certificates(root_dir: pathlib.Path, num_nodes: int):
    """Generate CA and node certificates with EC P-256 and SAN URI."""
    def openssl(*args):
        subprocess.check_call(
            ['openssl', *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    ca_key = root_dir / 'ca.key'
    ca_pem = root_dir / 'ca.pem'
    openssl(
        'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256',
        '-nodes', '-keyout', str(ca_key), '-out', str(ca_pem),
        '-days', '2', '-subj', '/CN=Murmur Live Test CA',
        '-addext', 'basicConstraints=critical,CA:TRUE',
    )

    for i in range(1, num_nodes + 1):
        key = root_dir / f"{i}.key"
        csr = root_dir / f"{i}.csr"
        crt = root_dir / f"{i}.pem"
        ext = root_dir / f"{i}.ext"
        node_id_hex = f"{i:032x}"

        openssl(
            'req', '-new', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256',
            '-nodes', '-keyout', str(key), '-out', str(csr),
            '-subj', f"/CN=node-{i}",
        )
        ext.write_text(f"subjectAltName=URI:replicateddb://node/{node_id_hex}\n")
        openssl(
            'x509', '-req', '-in', str(csr), '-CA', str(ca_pem),
            '-CAkey', str(ca_key), '-CAcreateserial', '-out', str(crt),
            '-days', '2', '-extfile', str(ext),
        )


class Node:
    """Manages a single embedded sqlite-murmur instance driven via stdio."""

    def __init__(self, root: pathlib.Path, number: int, port: int, seeds: list,
                 driver_bin: str, libs: list, group: str = '1' * 32,
                 schema: Optional[str] = None, start: bool = True, seed_host: str = '127.0.0.1',
                 existing: bool = False, bind_addr: Optional[str] = None,
                 advertise_addr: Optional[str] = None):
        self.number = number
        self.port = port
        self.path = root / f"node-{number}"
        self.root = root
        cmd = [driver_bin, str(self.path), *libs]
        self.p = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert self.readline() == 'READY', self.p.stderr.read() if self.p.poll() is not None else 'driver not ready'

        bind = bind_addr or f"127.0.0.1:{port}"
        adv = advertise_addr or f"127.0.0.1:{port}"

        if not existing:
            default_schema = (
                "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL DEFAULT '', payload BLOB); "
                "SELECT crsql_as_crr('main','records');"
            )
            self.sql(schema or default_schema)
            bootstrap_json = json.dumps([f"{seed_host}:{p}" for p in seeds])
            pragmas = [
                ('group_id', group),
                ('bind', bind),
                ('advertise', adv),
                ('bootstrap', bootstrap_json),
                ('certificate', str(root / f"{number}.pem")),
                ('private_key', str(root / f"{number}.key")),
                ('ca', str(root / 'ca.pem')),
                ('batch_bytes', '65536'),
                ('max_batch_bytes', '1048576'),
                ('anti_entropy_ms', '100'),
            ]
            for name, val in pragmas:
                self.sql(f"PRAGMA murmur_{name}='{val}'")
        else:
            if seeds:
                bootstrap_json = json.dumps([f"{seed_host}:{p}" for p in seeds])
                self.sql(f"PRAGMA murmur_bootstrap='{bootstrap_json}'")

        if start:
            self.start()

    def readline(self, timeout: float = 20.0) -> str:
        r, _, _ = select.select([self.p.stdout], [], [], timeout)
        if not r:
            raise TimeoutError(f"Node {self.number} driver read deadline exceeded ({timeout}s)")
        line = self.p.stdout.readline().strip()
        if not line:
            poll = self.p.poll()
            err = self.p.stderr.read() if poll is not None else ''
            raise RuntimeError(f"Node {self.number} driver exited prematurely (rc={poll}): {err}")
        return line

    def sql(self, query: str, fail: bool = False) -> str:
        self.p.stdin.write(query + '\n')
        self.p.stdin.flush()
        line = self.readline()
        if fail:
            assert line.startswith('ERR:'), f"Expected error for '{query}', got: '{line}'"
            return line[4:]
        else:
            assert line.startswith('OK:'), f"Query failed for '{query}', got: '{line}'"
            return line[3:]

    def start(self) -> str:
        return self.sql("SELECT murmur_start()")

    def stop(self) -> str:
        return self.sql("SELECT murmur_stop()")

    def reopen(self):
        self.p.stdin.write("@REOPEN\n")
        self.p.stdin.flush()
        line = self.readline()
        assert line == "OK:reopened", f"Expected OK:reopened, got: {line}"

    def status(self) -> dict:
        return json.loads(self.sql("SELECT murmur_status()"))

    def progress(self) -> list:
        return json.loads(self.sql("SELECT murmur_progress()"))

    def peers(self) -> list:
        out = self.sql("SELECT node_id, address, connected FROM murmur_peers")
        if not out:
            return []
        rows = []
        for row_str in out.split('|'):
            parts = row_str.split('~')
            if len(parts) >= 3:
                rows.append({'node_id': parts[0], 'address': parts[1], 'connected': parts[2] == '1'})
        return rows

    def signal(self, sig: signal.Signals):
        os.kill(self.p.pid, sig)

    def kill(self):
        if self.p.poll() is None:
            self.p.kill()
            self.p.wait(timeout=5)

    def close(self):
        if self.p.poll() is None:
            try:
                self.p.stdin.write("EXIT\n")
                self.p.stdin.flush()
                self.p.wait(timeout=10)
            except Exception:
                self.kill()


class Cluster:
    """Manages an isolated multi-node test mesh."""

    def __init__(self, scenario_name: str, num_nodes: int = 3, topology: str = 'full',
                 group: str = '1' * 32, schema: Optional[str] = None, start: bool = True):
        self.name = scenario_name
        self.num_nodes = num_nodes
        self.driver_bin, self.crsql_lib = find_test_binaries()
        self.libs = [self.crsql_lib] if self.crsql_lib else []
        self.temp_dir = tempfile.TemporaryDirectory(prefix=f"murmur-mesh-{scenario_name}-")
        self.root = pathlib.Path(self.temp_dir.name)
        generate_certificates(self.root, num_nodes)

        # Ephemeral ports
        sockets = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM) for _ in range(num_nodes)]
        for s in sockets:
            s.bind(('127.0.0.1', 0))
        self.ports = [s.getsockname()[1] for s in sockets]
        for s in sockets:
            s.close()

        self.nodes: List[Node] = []
        for i in range(1, num_nodes + 1):
            if topology == 'full':
                seeds = self.ports[:i - 1]
            elif topology == 'line':
                seeds = [self.ports[i - 2]] if i > 1 else []
            elif topology == 'star':
                seeds = [self.ports[0]] if i > 1 else []
            else:
                seeds = []

            node = Node(
                self.root, i, self.ports[i - 1], seeds, self.driver_bin, self.libs,
                group=group, schema=schema, start=start
            )
            self.nodes.append(node)

    def node(self, idx: int) -> Node:
        return self.nodes[idx]

    def __getitem__(self, idx: int) -> Node:
        return self.nodes[idx]

    def __len__(self) -> int:
        return len(self.nodes)

    def wait_until(self, predicate, timeout: float = 30.0, step: float = 0.1):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except Exception:
                pass
            time.sleep(step)
        statuses = []
        for n in self.nodes:
            if n.p.poll() is None:
                try:
                    statuses.append(n.status())
                except Exception as e:
                    statuses.append(f"err: {e}")
            else:
                statuses.append(f"exited: {n.p.poll()}")
        raise TimeoutError(f"Condition not met within {timeout}s. Cluster statuses:\n{json.dumps(statuses, indent=2)}")

    def wait_converged(self, count_query: str = "SELECT count(*) FROM records",
                       expected_count: Optional[int] = None, timeout: float = 30.0):
        if expected_count is not None:
            expected_str = str(expected_count)
            self.wait_until(
                lambda: all(n.sql(count_query) == expected_str for n in self.nodes),
                timeout=timeout
            )
        else:
            self.wait_until(
                lambda: len({n.sql(count_query) for n in self.nodes}) == 1,
                timeout=timeout
            )

    def restart_node(self, idx: int, start: bool = True, seeds: Optional[list] = None) -> Node:
        """Restart an existing node with a fresh process."""
        node = self.nodes[idx]
        node.close()
        s = seeds if seeds is not None else [self.ports[p] for p in range(len(self.nodes)) if p != idx]
        new_node = Node(
            self.root, node.number, node.port, s,
            self.driver_bin, self.libs, existing=True, start=start
        )
        self.nodes[idx] = new_node
        return new_node

    def close(self):
        for n in reversed(self.nodes):
            n.close()
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
