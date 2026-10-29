#!/usr/bin/env python3
"""Build a separate application against an installed static sqlite-murmur package."""
import pathlib
import shutil
import subprocess
import sys
import tempfile


def run(*args):
    subprocess.run(list(map(str, args)), check=True)


def main():
    source = pathlib.Path(__file__).resolve().parents[1]
    build = pathlib.Path(sys.argv[1]).resolve()
    cmake = sys.argv[2] if len(sys.argv) > 2 else 'cmake'
    with tempfile.TemporaryDirectory(prefix='sqlite-murmur-consumer-') as directory:
        root = pathlib.Path(directory)
        install = root / 'install'
        run(cmake, '--install', build, '--prefix', install)
        for archive in ('libcrypto.a', 'crypto.lib', 'libssl.a', 'ssl.lib'):
            assert not list(install.rglob(archive)), f'Unexpected OpenSSL archive: {archive}'
        for config in install.rglob('*.cmake'):
            assert 'sqlite_murmur::crypto' not in config.read_text(), config
        if list(install.rglob('libmurmur_replication.a')):
            for name in ('LICENSE-MIT', 'LICENSE-APACHE'):
                copies = [p for p in install.rglob(name) if p.parent.name == 'ipnet']
                assert len(copies) == 1, f'Missing internal ipnet license: {name}'
                assert copies[0].read_bytes() == (source / 'crates/murmur-ipnet' / name).read_bytes()
        run(cmake, '-S', source / 'examples/consumer', '-B', root / 'build',
            f'-DCMAKE_PREFIX_PATH={install}')
        run(cmake, '--build', root / 'build', '--parallel', '2')
        executable = root / 'build/sqlite_murmur_example'
        # Only the executable is deployed; installed archives cannot be found at runtime.
        deployed = root / 'deployed'
        deployed.mkdir()
        shutil.copy2(executable, deployed / executable.name)
        shutil.move(str(install), str(root / 'hidden-install'))
        binary = deployed / executable.name
        run(binary, root / 'database', '42' * 32)
        run(binary, root / 'database', '42' * 32)
        if sys.platform.startswith('linux'):
            linked = subprocess.check_output(['ldd', str(binary)], text=True)
            for dependency in ['libsqlite3.', 'librocksdb.', 'libzstd.', 'libcrypto.', 'libssl.', 'libaegis.', 'libmurmur']:
                assert dependency not in linked, linked
        print('Verified standalone C application, installed CMake target, persistence, and static third-party linkage.')


if __name__ == '__main__':
    main()
