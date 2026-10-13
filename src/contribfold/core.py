"""Atomic contribution folding with a durable, optimistic SQLite job journal."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
import sqlite3
import time
from typing import Any, Callable, Iterable, Mapping
import uuid


class ConflictError(RuntimeError):
    """The committed revision changed; stage a fresh batch with a new ID."""


class BatchMismatchError(ValueError):
    """A batch ID was reused with different intent."""


class LeaseLostError(RuntimeError):
    """A worker no longer owns its claim; its result was not saved."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _name(value: str, label: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{label} must be a nonempty string without NUL")
    return value


@dataclass(frozen=True)
class Contribution:
    document: str
    text: str


@dataclass(frozen=True)
class WorkItem:
    batch_id: str
    target: str
    contributions: tuple[Contribution, ...]
    reducer_version: str
    idempotency_key: str
    token: str


@dataclass(frozen=True)
class Batch:
    batch_id: str
    base_revision: int
    state: str
    total: int
    ready: int
    running: int
    pending: int


_SCHEMA = """
CREATE TABLE IF NOT EXISTS cf_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cf_documents (id TEXT PRIMARY KEY, content TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cf_outputs (target TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cf_batches (
 id TEXT PRIMARY KEY, intent TEXT NOT NULL, base_revision INTEGER NOT NULL,
 reducer_version TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('staged','committed','aborted')),
 committed_revision INTEGER
);
CREATE TABLE IF NOT EXISTS cf_jobs (
 batch_id TEXT NOT NULL REFERENCES cf_batches(id) ON DELETE CASCADE,
 target TEXT NOT NULL, sources TEXT NOT NULL, job_key TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','running','ready')),
 token TEXT, lease_until REAL, result TEXT, remove_target INTEGER NOT NULL,
 PRIMARY KEY(batch_id, target)
);
"""


class Store:
    """A dedicated local SQLite database. Use one instance per thread/process.

    Never open untrusted databases. Files must be on a local filesystem with
    working SQLite locks; network filesystems and multi-host use are unsupported.
    """

    def __init__(self, path: str, *, timeout: float = 10.0):
        self._db = sqlite3.connect(path, timeout=timeout, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        try:
            self._db.execute("PRAGMA foreign_keys=ON")
            with self._transaction():
                existing = self._db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                names = {row[0] for row in existing}
                if names and "cf_meta" not in names:
                    raise ValueError("use a new database or an existing contribfold database")
                if "cf_meta" in names:
                    row = self._db.execute("SELECT value FROM cf_meta WHERE key='schema'").fetchone()
                    if row is None or row[0] != "1":
                        raise ValueError("unsupported contribfold schema version")
                for statement in _SCHEMA.split(";"):
                    if statement.strip():
                        self._db.execute(statement)
                self._db.execute("INSERT OR IGNORE INTO cf_meta VALUES ('schema','1')")
                self._db.execute("INSERT OR IGNORE INTO cf_meta VALUES ('revision','0')")
                self._db.execute("INSERT OR IGNORE INTO cf_meta VALUES ('reducer_version','')")
            # SQLite may reject a concurrent journal-mode transition immediately,
            # without invoking its busy handler. Retry outside a transaction so
            # simultaneous first opens honor the caller's timeout as well.
            deadline = time.monotonic() + max(0, timeout)
            while True:
                try:
                    self._db.execute("PRAGMA journal_mode=WAL")
                    break
                except sqlite3.OperationalError as exc:
                    code = getattr(exc, "sqlite_errorcode", None)
                    busy = ((code & 0xff) in (5, 6) if code is not None else
                            str(exc) in ("database is locked", "database table is locked"))
                    remaining = deadline - time.monotonic()
                    if not busy or remaining <= 0:
                        raise
                    time.sleep(min(.01, remaining))
            self._db.execute("PRAGMA synchronous=FULL")
        except BaseException:
            self._db.close()
            raise

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @contextmanager
    def _transaction(self):
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    @property
    def revision(self) -> int:
        return int(self._db.execute("SELECT value FROM cf_meta WHERE key='revision'").fetchone()[0])

    def documents(self) -> dict[str, dict[str, str]]:
        """Read a copy of the currently committed document contributions."""
        return {row[0]: json.loads(row[1]) for row in self._db.execute("SELECT id,content FROM cf_documents ORDER BY id")}

    def outputs(self) -> dict[str, Any]:
        """Read a copy of all committed derived outputs in one SQL snapshot."""
        return {row[0]: json.loads(row[1]) for row in self._db.execute("SELECT target,value FROM cf_outputs ORDER BY target")}

    def stage(self, batch_id: str, *, upserts: Mapping[str, Mapping[str, str]] | None = None,
              deletes: Iterable[str] = (), reducer_version: str = "v1",
              expected_revision: int | None = None) -> Batch:
        """Stage full document replacements and deletions as one durable intent.

        A document maps target IDs (entities, topics, etc.) to contribution text.
        Upsert replaces that document's whole map, so removed target associations
        are automatically subtracted. A reducer-version change rebuilds all live
        targets. Reusing an ID with identical intent is idempotent.
        """
        _name(batch_id, "batch_id")
        _name(reducer_version, "reducer_version")
        if expected_revision is not None and (type(expected_revision) is not int or expected_revision < 0):
            raise ValueError("expected_revision must be a nonnegative integer")
        if upserts is not None and not isinstance(upserts, Mapping):
            raise ValueError("upserts must map document IDs to contribution maps")
        normalized: dict[str, dict[str, str]] = {}
        for doc, contributions in (upserts or {}).items():
            _name(doc, "document ID")
            if not isinstance(contributions, Mapping):
                raise ValueError("each document must map target IDs to text")
            normalized[doc] = {}
            for target, text in contributions.items():
                _name(target, "target ID")
                if not isinstance(text, str) or not text.strip():
                    raise ValueError("contribution text must be a nonempty string")
                normalized[doc][target] = text
        if isinstance(deletes, (str, bytes)):
            raise ValueError("deletes must be an iterable of document IDs, not a string")
        deleted = sorted({_name(doc, "document ID") for doc in deletes})
        if set(deleted) & normalized.keys():
            raise ValueError("a document cannot be both upserted and deleted")
        intent = _json({"upserts": normalized, "deletes": deleted, "reducer_version": reducer_version})
        with self._transaction():
            previous = self._db.execute("SELECT intent FROM cf_batches WHERE id=?", (batch_id,)).fetchone()
            if previous:
                if previous[0] != intent:
                    raise BatchMismatchError("batch ID already belongs to different intent")
                return self.status(batch_id)
            revision = self.revision
            if expected_revision is not None and expected_revision != revision:
                raise ConflictError(f"expected revision {expected_revision}; current revision is {revision}")
            before = self.documents()
            after = {doc: dict(value) for doc, value in before.items()}
            for doc in deleted:
                after.pop(doc, None)
            after.update(normalized)
            affected: set[str] = set()
            for doc in set(deleted) | normalized.keys():
                old, new = before.get(doc, {}), after.get(doc, {})
                affected.update(target for target in old.keys() | new.keys() if old.get(target) != new.get(target))
            old_version = self._db.execute("SELECT value FROM cf_meta WHERE key='reducer_version'").fetchone()[0]
            if old_version != reducer_version:
                affected.update(target for values in after.values() for target in values)
            self._db.execute("INSERT INTO cf_batches(id,intent,base_revision,reducer_version,state) VALUES (?,?,?,?,'staged')", (batch_id, intent, revision, reducer_version))
            for target in sorted(affected):
                sources = [[doc, values[target]] for doc, values in sorted(after.items()) if target in values]
                payload = _json(sources)
                key = hashlib.sha256(_json([target, reducer_version, sources]).encode("utf-8")).hexdigest()
                self._db.execute("INSERT INTO cf_jobs(batch_id,target,sources,job_key,state,remove_target) VALUES (?,?,?,?,?,?)", (batch_id, target, payload, key, "pending" if sources else "ready", not sources))
        return self.status(batch_id)

    def status(self, batch_id: str) -> Batch:
        # One statement keeps counts/state coherent during concurrent commits.
        row = self._db.execute("""SELECT b.base_revision,b.state,COUNT(j.target) AS total,
          COALESCE(SUM(j.state='ready'),0) AS ready,COALESCE(SUM(j.state='running'),0) AS running,
          COALESCE(SUM(j.state='pending'),0) AS pending FROM cf_batches b LEFT JOIN cf_jobs j
          ON b.id=j.batch_id WHERE b.id=? GROUP BY b.id""", (batch_id,)).fetchone()
        if row is None:
            raise KeyError(batch_id)
        return Batch(batch_id, row[0], row[1], row[2], row[3], row[4], row[5])

    def _active(self, batch_id: str) -> sqlite3.Row:
        row = self._db.execute("SELECT * FROM cf_batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise KeyError(batch_id)
        if row["state"] != "staged":
            raise ValueError(f"batch is {row['state']}")
        if row["base_revision"] != self.revision:
            raise ConflictError("batch snapshot is stale; abort it and stage current intent under a new ID")
        return row

    def claim(self, batch_id: str, *, lease_seconds: float = 300) -> WorkItem | None:
        """Claim one job, reclaiming expired work. No callback runs under a DB lock."""
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, (int, float)) or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive and finite")
        with self._transaction():
            state = self.status(batch_id)
            if state.state == "committed":
                return None
            batch = self._active(batch_id)
            now = time.time()
            row = self._db.execute("SELECT * FROM cf_jobs WHERE batch_id=? AND (state='pending' OR (state='running' AND lease_until<=?)) ORDER BY target LIMIT 1", (batch_id, now)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            self._db.execute("UPDATE cf_jobs SET state='running',token=?,lease_until=? WHERE batch_id=? AND target=?", (token, now + lease_seconds, batch_id, row["target"]))
            return WorkItem(batch_id, row["target"], tuple(Contribution(*source) for source in json.loads(row["sources"])), batch["reducer_version"], row["job_key"], token)

    def heartbeat(self, item: WorkItem, *, lease_seconds: float = 300) -> None:
        """Extend an unexpired claim; use a separate Store from a heartbeat thread."""
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, (int, float)) or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive and finite")
        with self._transaction():
            self._active(item.batch_id)
            now = time.time()
            row = self._db.execute("UPDATE cf_jobs SET lease_until=? WHERE batch_id=? AND target=? AND state='running' AND token=? AND lease_until>?", (now + lease_seconds, item.batch_id, item.target, item.token, now))
            if row.rowcount != 1:
                raise LeaseLostError("claim expired or changed")

    def complete(self, item: WorkItem, result: Any) -> None:
        """Persist a JSON-compatible reducer result only for the current lease."""
        encoded = _json(result)
        with self._transaction():
            self._active(item.batch_id)
            row = self._db.execute("UPDATE cf_jobs SET state='ready',result=?,token=NULL,lease_until=NULL WHERE batch_id=? AND target=? AND job_key=? AND state='running' AND token=? AND lease_until>?", (encoded, item.batch_id, item.target, item.idempotency_key, item.token, time.time()))
            if row.rowcount != 1:
                raise LeaseLostError("claim expired or changed; result was discarded")

    def release(self, item: WorkItem) -> None:
        """Return an owned failed job to pending. Never releases another worker."""
        with self._transaction():
            self._db.execute("UPDATE cf_jobs SET state='pending',token=NULL,lease_until=NULL WHERE batch_id=? AND target=? AND state='running' AND token=?", (item.batch_id, item.target, item.token))

    def commit(self, batch_id: str) -> int:
        """Atomically publish all document changes and outputs if every job is ready."""
        with self._transaction():
            row = self._db.execute("SELECT * FROM cf_batches WHERE id=?", (batch_id,)).fetchone()
            if row is not None and row["state"] == "committed":
                return row["committed_revision"]
            batch = self._active(batch_id)
            if self.status(batch_id).ready != self.status(batch_id).total:
                raise ValueError("batch still has unfinished jobs")
            changes = json.loads(batch["intent"])
            for doc in changes["deletes"]:
                self._db.execute("DELETE FROM cf_documents WHERE id=?", (doc,))
            for doc, contributions in changes["upserts"].items():
                self._db.execute("INSERT INTO cf_documents VALUES (?,?) ON CONFLICT(id) DO UPDATE SET content=excluded.content", (doc, _json(contributions)))
            for job in self._db.execute("SELECT * FROM cf_jobs WHERE batch_id=?", (batch_id,)).fetchall():
                if job["remove_target"]:
                    self._db.execute("DELETE FROM cf_outputs WHERE target=?", (job["target"],))
                else:
                    self._db.execute("INSERT INTO cf_outputs VALUES (?,?) ON CONFLICT(target) DO UPDATE SET value=excluded.value", (job["target"], job["result"]))
            revision = self.revision + 1
            self._db.execute("UPDATE cf_meta SET value=? WHERE key='revision'", (str(revision),))
            self._db.execute("UPDATE cf_meta SET value=? WHERE key='reducer_version'", (batch["reducer_version"],))
            self._db.execute("UPDATE cf_batches SET state='committed',committed_revision=? WHERE id=?", (revision, batch_id))
            return revision

    def run(self, batch_id: str, reducer: Callable[[WorkItem], Any], *, lease_seconds: float = 300) -> Batch:
        """Work until no claim is available, then commit if ready.

        Returns staged if another worker still owns unfinished work. Callback
        exceptions propagate after releasing this claim; completed jobs survive.
        Callbacks must finish within the lease or renew it with heartbeat().
        """
        state = self.status(batch_id)
        if state.state == "committed":
            return state
        while True:
            item = self.claim(batch_id, lease_seconds=lease_seconds)
            if item is None:
                break
            try:
                result = reducer(item)
                self.complete(item, result)
            except BaseException:
                self.release(item)
                raise
        state = self.status(batch_id)
        if state.ready == state.total:
            self.commit(batch_id)
        return self.status(batch_id)

    def abort(self, batch_id: str) -> None:
        """Invalidate staged work and remove jobs/results, retaining intent text.

        Intent remains for duplicate-ID detection until forget(). Committed
        document state is untouched. This is not data erasure.
        """
        with self._transaction():
            row = self._db.execute("SELECT state FROM cf_batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise KeyError(batch_id)
            if row[0] == "committed":
                raise ValueError("cannot abort a committed batch")
            self._db.execute("UPDATE cf_batches SET state='aborted' WHERE id=?", (batch_id,))
            self._db.execute("DELETE FROM cf_jobs WHERE batch_id=?", (batch_id,))

    def forget(self, batch_id: str) -> None:
        """Delete a terminal batch journal. Also forgets its idempotency record.

        Logical deletion only: SQLite pages/backups may retain data. This is not
        a secure erasure API. An old ID may be reused after forget, so callers
        must retain their own deduplication record if retries can arrive later.
        """
        with self._transaction():
            row = self._db.execute("SELECT state FROM cf_batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise KeyError(batch_id)
            if row[0] == "staged":
                raise ValueError("abort or commit the batch before forgetting it")
            self._db.execute("DELETE FROM cf_batches WHERE id=?", (batch_id,))
