import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline import run


class PipelineTest(unittest.TestCase):
    def test_replay_tombstone_and_atomic_reconciliation(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "test.db"
            rows = [json.loads(line) for line in Path("data/events.jsonl").read_text().splitlines()]
            self.assertEqual(run(db, rows, {"A": 1800}),
                             dict(applied=3, stale=1, rejected=1, replayed=0))
            self.assertEqual(run(db, rows, {"A": 1800})["replayed"], 5)
            late = dict(rows[2], offset=3)
            self.assertEqual(run(db, [late], {"A": 1800})["stale"], 1)
            with self.assertRaisesRegex(ValueError, "Reconciliation failed"):
                run(db, [dict(rows[0], offset=14, version=3, balance_cents=999)], {"A": 1800})
            with self.assertRaisesRegex(ValueError, "Conflicting replay"):
                run(db, [dict(rows[0], balance_cents=999)], {"A": 1800})
            with sqlite3.connect(db) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM events").fetchone()[0], 6)
                self.assertEqual(conn.execute("SELECT count(*) FROM rejected").fetchone()[0], 1)
