"""Apply full-image CDC events to a local warehouse projection."""
import argparse
import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    partition_id TEXT, offset_id INTEGER, payload TEXT,
    PRIMARY KEY (partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
    balance_cents INTEGER, deleted INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rejected (
    partition_id TEXT, offset_id INTEGER, reason TEXT,
    PRIMARY KEY (partition_id, offset_id)
);
"""


def apply(db, rows):
    stats = dict(applied=0, replayed=0, stale=0, rejected=0)
    for row in rows:
        key = row["partition"], row["offset"]
        if not isinstance(key[0], str) or not key[0] or type(key[1]) is not int or key[1] < 0:
            raise ValueError("Invalid source position")
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
        prior = db.execute("SELECT payload FROM events WHERE partition_id=? AND offset_id=?",
                           key).fetchone()
        if prior:
            if prior[0] != payload:
                raise ValueError(f"Conflicting replay at {key}")
            stats["replayed"] += 1
            continue

        db.execute("INSERT INTO events VALUES (?, ?, ?)", (*key, payload))
        account, version, operation, balance = (
            row.get("account"), row.get("version"), row.get("op"), row.get("balance_cents")
        )
        if (not isinstance(account, str) or not account.strip()
                or type(version) is not int or not 0 <= version < 2**63
                or operation not in ("upsert", "delete")
                or (operation == "upsert" and
                    (type(balance) is not int or not -2**63 <= balance < 2**63))):
            db.execute("INSERT INTO rejected VALUES (?, ?, ?)", (*key, "Invalid event schema"))
            stats["rejected"] += 1
            continue
        deleted = operation == "delete"
        incoming = (version, None if deleted else balance, int(deleted))
        current = db.execute(
            "SELECT version, balance_cents, deleted FROM accounts WHERE account_id=?", (account,)
        ).fetchone()
        if current and version == current[0] and incoming != current:
            raise ValueError(f"Conflicting account version for {account}")
        result = db.execute("""
            INSERT INTO accounts VALUES (?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET version=excluded.version,
                balance_cents=excluded.balance_cents, deleted=excluded.deleted
            WHERE excluded.version > accounts.version
        """, (account, *incoming))
        stats["applied" if result.rowcount else "stale"] += 1
    return stats


def reconcile(db, expected):
    actual = dict(db.execute("SELECT account_id, balance_cents FROM accounts WHERE deleted=0"))
    return {
        key: {"source": expected.get(key), "target": actual.get(key)}
        for key in sorted(expected.keys() | actual.keys())
        if expected.get(key) != actual.get(key)
    }


def run(database, events, snapshot):
    db = sqlite3.connect(database)
    try:
        db.executescript(SCHEMA)
        with db:
            db.execute("BEGIN IMMEDIATE")
            stats = apply(db, events)
            differences = reconcile(db, snapshot)
            if differences:
                raise ValueError(f"Reconciliation failed: {json.dumps(differences, sort_keys=True)}")
        return stats
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--snapshot", type=Path, default=Path("data/snapshot.json"))
    parser.add_argument("--db", type=Path, default=Path("output/cdc.db"))
    args = parser.parse_args()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with args.events.open(encoding="utf-8") as source:
        events = [json.loads(line) for line in source if line.strip()]
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    print(json.dumps(run(args.db, events, snapshot), sort_keys=True))
