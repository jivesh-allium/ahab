from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


class DedupeStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_alerts (
                  dedupe_key TEXT PRIMARY KEY,
                  seen_at INTEGER NOT NULL
                )
                """
            )
            self._conn.commit()

    def has_seen(self, dedupe_key: str) -> bool:
        with self._lock:
            cursor = self._conn.execute("SELECT 1 FROM seen_alerts WHERE dedupe_key = ? LIMIT 1", (dedupe_key,))
            return cursor.fetchone() is not None

    def mark_seen(self, dedupe_key: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO seen_alerts (dedupe_key, seen_at) VALUES (?, ?)",
                (dedupe_key, int(time.time())),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
