"""Local extraction planning and exact, optionally disk-backed key validation.

The SQLite index is an ephemeral per-iterator validation aid, never checkpoint
or ingestion state. It contains source key values and is deleted when the
iterator completes, fails, or is explicitly closed. No value is logged.
"""

import json
import os
import sqlite3
import tempfile

from databricks.labs.community_connector.sources.crm_analytics.crm_analytics_support import (
    CrmAnalyticsError,
)


def extraction_plan(descriptor: dict, options: dict, page_size: int) -> dict:
    """Return a metadata-only estimate, without executing count or row queries.

    The query minimum includes two counts and the mandatory terminal empty
    page. Retries cost additional requests. SQLite's page-cache setting is a
    cache budget, not a bound on the entire Python process or response pages.
    """
    rows = descriptor["expected_row_count"]
    pages = (rows + page_size - 1) // page_size
    generated = bool(options.get("auto_increment_column"))
    storage = "generated_counter" if generated else options["key_validation_mode"]
    return {
        "dataset_id": descriptor["dataset_id"],
        "version_id": descriptor["version_id"],
        "expected_row_count": rows,
        "page_size": page_size,
        "data_pages": pages,
        "terminal_page_queries": 1,
        "count_queries": 2,
        "minimum_query_requests": pages + 3,
        "within_query_budget": pages + 3 <= options["max_query_requests"],
        "within_key_capacity": rows <= options["max_keys"],
        "max_offset": rows,
        "key_validation_storage": storage,
        "key_validation_cache_kib": (
            options["key_validation_cache_kib"] if storage == "sqlite" else None
        ),
        "parallel_extraction": False,
        "source_volume_validation_required": True,
    }


class _KeyValidator:
    """Validate exact keys with per-iterator ownership and deterministic cleanup."""

    def __init__(self, mode: str, cache_kib: int, *, generated: bool = False):
        self.mode = "generated_counter" if generated else mode
        self.cache_kib = cache_kib
        self.count = 0
        self._seen = set() if self.mode == "memory" else None
        self._directory = None
        self._connection = None

    def __enter__(self):
        if self.mode != "sqlite":
            return self
        try:
            self._directory = tempfile.TemporaryDirectory(prefix="crm-analytics-keys-")
            # TemporaryDirectory uses 0700; mkstemp also gives the database 0600.
            handle, path = tempfile.mkstemp(
                prefix="keys-", suffix=".sqlite", dir=self._directory.name
            )
            os.close(handle)
            self._connection = sqlite3.connect(path)
            self._connection.execute("PRAGMA journal_mode=OFF")
            self._connection.execute("PRAGMA synchronous=OFF")
            self._connection.execute("PRAGMA mmap_size=0")
            self._connection.execute("PRAGMA temp_store=FILE")
            self._connection.execute(f"PRAGMA cache_size=-{self.cache_kib}")
            self._connection.execute("CREATE TABLE keys (value BLOB PRIMARY KEY) WITHOUT ROWID")
            return self
        except (OSError, sqlite3.Error):
            self.close()
            raise CrmAnalyticsError("Unable to initialize local key-validation storage") from None

    def add(self, row_key: tuple) -> None:
        if self.mode == "generated_counter":
            # The caller constructs a fixed version ID and sequential 1..N row
            # number. Retaining those predictable keys would add O(N) memory.
            self.count += 1
            return
        if self.mode == "memory":
            if row_key in self._seen:
                raise CrmAnalyticsError("Null or duplicate primary key in snapshot")
            self._seen.add(row_key)
        else:
            # Strict source keys are validated strings. JSON's array framing
            # distinguishes composite keys, empty strings, Unicode and embedded
            # separators exactly. BLOB equality preserves Python's exact string
            # equality rather than applying a database collation or row hash.
            encoded = json.dumps(row_key, ensure_ascii=True, separators=(",", ":")).encode("ascii")
            try:
                self._connection.execute("INSERT INTO keys(value) VALUES (?)", (encoded,))
            except sqlite3.IntegrityError:
                raise CrmAnalyticsError("Null or duplicate primary key in snapshot") from None
            except (OSError, sqlite3.Error):
                raise CrmAnalyticsError(
                    "Local key-validation storage failed; snapshot aborted"
                ) from None
        self.count += 1

    def close(self) -> None:
        failed = False
        if self._connection is not None:
            connection, self._connection = self._connection, None
            try:
                connection.close()
            except sqlite3.Error:
                failed = True
        if self._directory is not None:
            directory, self._directory = self._directory, None
            try:
                directory.cleanup()
            except OSError:
                failed = True
        if self._seen is not None:
            self._seen.clear()
        if failed:
            raise CrmAnalyticsError("Unable to clean local key-validation storage") from None

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
