"""Offline regression tests for the HC-tree benchmark artifact validator."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / "benchmarks/hctree/report.py"
spec = importlib.util.spec_from_file_location("hctree_report", MODULE)
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

FIELDS = ("seconds,reads,writes,operations,ops_per_second,transactions,"
          "mean_write_transaction_ms,tracked_changes,logical_database_bytes,"
          "rocksdb_files_bytes,disk_bytes,suite,backend,replication,batch_size")


def murmur_log(repl="off", batch=1, engine="standard"):
    writes = batch * 2
    return (f"BENCH_ENGINE,{engine}\n"
            f"BENCH_CONFIG,suite=single,backend=murmur,encryption=on,"
            f"vfs_block_size=4096,ingest=local,rocksdb_sync=1,"
            f"replication={repl},batch_size={batch}\n"
            f"BENCH_HEADER,{FIELDS}\n"
            f"BENCH_CSV,1,{4*writes},{writes},{5*writes},{5*writes},2,"
            f"0.1,{writes if repl == 'on' else 0},4096,8192,8192,"
            f"single,murmur,{repl},{batch}\n"
            "BENCH_DONE\nBENCH_SUITE_DONE\n")


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)

    def write(self, name, content):
        (self.path / name).write_text(content)

    def test_valid_murmur(self):
        for repl in ("off", "on"):
            for batch in (1, 100):
                name = f"murmur-{repl}-{batch}.txt"
                self.write(name, murmur_log(repl, batch))
                self.assertEqual(report.murmur_result(self.path, repl, batch)["writes"], str(2*batch))

    def test_rejects_truncation(self):
        name = "murmur-off-1.txt"
        self.write(name, murmur_log().replace("BENCH_DONE\nBENCH_SUITE_DONE\n", ""))
        with self.assertRaisesRegex(ValueError, "BENCH_DONE"):
            report.murmur_result(self.path, "off", 1)

    def test_rejects_wrong_engine(self):
        name = "integrated-hctree-off-1.txt"
        self.write(name, murmur_log(engine="standard"))
        with self.assertRaisesRegex(ValueError, "engine is not hctree"):
            report.murmur_result(self.path, "off", 1, "integrated-hctree")

    def test_rejects_false_batch_prefix_match(self):
        name = "murmur-off-1.txt"
        self.write(name, murmur_log(batch=100))
        with self.assertRaisesRegex(ValueError, "case configuration mismatch"):
            report.murmur_result(self.path, "off", 1)

    def test_rejects_missing_tracking(self):
        name = "murmur-on-1.txt"
        self.write(name, murmur_log(repl="on").replace("0.1,2,4096", "0.1,0,4096"))
        with self.assertRaisesRegex(ValueError, "change count mismatch"):
            report.murmur_result(self.path, "on", 1)

    def test_rejects_nonfinite_throughput(self):
        name = "murmur-off-1.txt"
        self.write(name, murmur_log().replace("BENCH_CSV,1,8,2,10,10,", "BENCH_CSV,1,8,2,10,nan,"))
        with self.assertRaisesRegex(ValueError, "nonfinite ops_per_second"):
            report.murmur_result(self.path, "off", 1)

    def test_rejects_duplicate_completion(self):
        name = "murmur-off-1.txt"
        self.write(name, murmur_log().replace("BENCH_DONE\n", "BENCH_DONE\nBENCH_DONE\n"))
        with self.assertRaisesRegex(ValueError, "duplicate completion"):
            report.murmur_result(self.path, "off", 1)


if __name__ == "__main__":
    unittest.main()
