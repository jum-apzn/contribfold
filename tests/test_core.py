from contextlib import closing
import json
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from contribfold import BatchMismatchError, ConflictError, LeaseLostError, Store


def summarize(job):
    return {"summary": " | ".join(f"{c.document}: {c.text}" for c in job.contributions)}


def crash_worker(path, batch):
    # Commit one result, claim a second, then hard-exit without releasing it.
    import os
    store = Store(path)
    first = store.claim(batch, lease_seconds=.1)
    store.complete(first, summarize(first))
    store.claim(batch, lease_seconds=.1)
    os._exit(17)


def crash_commit_worker(path, batch):
    import os
    store = Store(path)
    store._db.create_function("die_now", 0, lambda: os._exit(19))
    store._db.execute("CREATE TEMP TRIGGER crash BEFORE UPDATE ON cf_meta WHEN NEW.key='revision' BEGIN SELECT die_now(); END")
    store.commit(batch)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "kb.sqlite")
        self.store = Store(self.path)
        self.addCleanup(self.store.close)

    def seed(self):
        self.store.stage("seed", upserts={"a": {"shared": "A", "unique": "only A"}, "b": {"shared": "B"}, "c": {"shared": "C", "other": "C other"}})
        self.store.run("seed", summarize)

    def test_coalesces_and_preserves_unaffected(self):
        self.seed()
        old = self.store.outputs()
        self.store.stage("edit", deletes=["a"], upserts={"b": {"shared": "new B", "new": "new topic"}})
        self.assertEqual(self.store.outputs(), old)
        calls = []
        self.store.run("edit", lambda item: calls.append(item.target) or summarize(item))
        self.assertEqual(calls, ["new", "shared"])
        self.assertEqual(self.store.outputs()["other"], old["other"])
        self.assertNotIn("unique", self.store.outputs())
        self.assertNotIn("a", self.store.documents())
        self.assertEqual(self.store.outputs()["shared"]["summary"], "b: new B | c: C")

    def test_remove_association_on_upsert(self):
        self.seed()
        self.store.stage("replace", upserts={"a": {"shared": "A"}})
        self.store.run("replace", summarize)
        self.assertNotIn("unique", self.store.outputs())

    def test_delete_all_no_reducer(self):
        self.seed()
        self.store.stage("all", deletes=["a", "b", "c"])
        self.store.run("all", lambda _: self.fail("must not build empty target"))
        self.assertEqual(self.store.outputs(), {})
        self.assertEqual(self.store.documents(), {})

    def test_noop_no_reducer(self):
        self.seed()
        self.store.stage("noop", upserts={"b": {"shared": "B"}}, deletes=["missing"])
        self.store.run("noop", lambda _: self.fail("unchanged"))

    def test_failure_resume_skips_ready(self):
        self.store.stage("batch", upserts={"doc": {"a": "a", "b": "b"}})
        calls = []
        def fail_second(item):
            calls.append(item.target)
            if item.target == "b":
                raise RuntimeError("provider failed")
            return summarize(item)
        with self.assertRaises(RuntimeError):
            self.store.run("batch", fail_second)
        self.assertEqual(self.store.outputs(), {})
        self.assertEqual(self.store.documents(), {})
        self.assertEqual(self.store.status("batch").ready, 1)
        with Store(self.path) as second:
            second.run("batch", lambda item: calls.append(item.target) or summarize(item))
        self.assertEqual(calls, ["a", "b", "b"])
        self.assertEqual(self.store.status("batch").state, "committed")

    def test_idempotent_intent_and_commit(self):
        args = {"upserts": {"d": {"x": "text"}}, "deletes": ["absent", "absent"]}
        self.store.stage("batch", **args)
        self.store.stage("batch", **args)
        self.store.run("batch", summarize)
        revision = self.store.revision
        self.store.stage("batch", **args)
        self.store.run("batch", lambda _: self.fail("already built"))
        self.assertEqual(self.store.commit("batch"), revision)
        self.assertEqual(self.store.revision, revision)
        with self.assertRaises(BatchMismatchError):
            self.store.stage("batch", upserts={"d": {"x": "different"}})

    def test_expected_revision(self):
        with self.assertRaises(ConflictError):
            self.store.stage("bad", expected_revision=1)
        with self.assertRaises(KeyError):
            self.store.status("bad")

    def test_concurrent_batches_stale_result(self):
        self.seed()
        self.store.stage("first", upserts={"a": {"shared": "first"}})
        self.store.stage("second", upserts={"a": {"shared": "second"}})
        item = self.store.claim("second")
        self.store.run("first", summarize)
        with self.assertRaises(ConflictError):
            self.store.complete(item, "stale result")
        with self.assertRaises(ConflictError):
            self.store.commit("second")
        self.assertNotEqual(self.store.outputs()["shared"], "stale result")
        self.store.abort("second")
        self.store.stage("second-rebased", upserts={"a": {"shared": "second"}})
        self.store.run("second-rebased", summarize)
        self.assertIn("second", self.store.outputs()["shared"]["summary"])

    def test_expired_worker_cannot_write(self):
        self.store.stage("batch", upserts={"d": {"x": "text"}})
        with patch("contribfold.core.time.time", return_value=100):
            first = self.store.claim("batch", lease_seconds=10)
        with patch("contribfold.core.time.time", return_value=111):
            second = self.store.claim("batch", lease_seconds=10)
            self.assertNotEqual(first.token, second.token)
            self.assertEqual(first.idempotency_key, second.idempotency_key)
            with self.assertRaises(LeaseLostError):
                self.store.complete(first, "old")
            self.store.release(first)
            self.store.complete(second, "new")
        self.store.commit("batch")
        self.assertEqual(self.store.outputs(), {"x": "new"})

    def test_expiry_without_reclaim(self):
        self.store.stage("batch", upserts={"d": {"x": "text"}})
        with patch("contribfold.core.time.time", return_value=100):
            item = self.store.claim("batch", lease_seconds=1)
        with patch("contribfold.core.time.time", return_value=102):
            with self.assertRaises(LeaseLostError):
                self.store.complete(item, "late")
            with self.assertRaises(LeaseLostError):
                self.store.heartbeat(item)

    def test_heartbeat(self):
        self.store.stage("batch", upserts={"d": {"x": "text"}})
        with patch("contribfold.core.time.time", return_value=100):
            item = self.store.claim("batch", lease_seconds=5)
        with patch("contribfold.core.time.time", return_value=103):
            self.store.heartbeat(item, lease_seconds=10)
        with patch("contribfold.core.time.time", return_value=110):
            self.assertIsNone(self.store.claim("batch"))
            self.store.complete(item, "done")

    def test_atomic_commit_rollback(self):
        self.seed()
        before_docs, before_outputs = self.store.documents(), self.store.outputs()
        self.store.stage("change", deletes=["a"], upserts={"b": {"shared": "new"}})
        while (item := self.store.claim("change")) is not None:
            self.store.complete(item, summarize(item))
        # Fault injection inside the final SQL transaction.
        self.store._db.execute("CREATE TRIGGER fail_commit BEFORE UPDATE ON cf_meta WHEN NEW.key='revision' BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.commit("change")
        self.assertEqual(self.store.documents(), before_docs)
        self.assertEqual(self.store.outputs(), before_outputs)
        self.assertEqual(self.store.status("change").state, "staged")
        self.store._db.execute("DROP TRIGGER fail_commit")
        self.store.commit("change")

    def test_version_change_rebuilds_all(self):
        self.seed()
        self.store.stage("v2", reducer_version="model2-prompt3")
        calls = []
        self.store.run("v2", lambda item: calls.append(item.target) or summarize(item))
        self.assertEqual(calls, ["other", "shared", "unique"])

    def test_job_keys_include_sources_and_version(self):
        self.store.stage("a", upserts={"d": {"x": "text"}})
        a = self.store.claim("a")
        self.store.stage("b", upserts={"d": {"x": "text"}})
        b = self.store.claim("b")
        self.assertEqual(a.idempotency_key, b.idempotency_key)
        self.store.stage("c", upserts={"d": {"x": "text"}}, reducer_version="v2")
        c = self.store.claim("c")
        self.assertNotEqual(a.idempotency_key, c.idempotency_key)

    def test_invalid_results_released(self):
        self.store.stage("a", upserts={"d": {"x": "text"}})
        with self.assertRaises(ValueError):
            self.store.run("a", lambda _: float("nan"))
        self.assertEqual(self.store.status("a").pending, 1)
        self.store.run("a", lambda _: None)
        self.assertEqual(self.store.outputs(), {"x": None})

    def test_abort_forget(self):
        self.store.stage("a", upserts={"d": {"x": "text"}})
        with self.assertRaises(ValueError):
            self.store.forget("a")
        item = self.store.claim("a")
        self.store.abort("a")
        with self.assertRaises(ValueError):
            self.store.complete(item, "bad")
        self.assertEqual(self.store.outputs(), {})
        self.store.forget("a")
        with self.assertRaises(KeyError):
            self.store.status("a")

    def test_unknown_and_incomplete(self):
        for operation in (self.store.status, self.store.claim, self.store.commit, self.store.abort, self.store.forget):
            with self.assertRaises(KeyError):
                operation("missing")
        self.store.stage("a", upserts={"d": {"x": "t"}})
        with self.assertRaises(ValueError):
            self.store.commit("a")

    def test_validation(self):
        cases = [dict(upserts={"a": {"x": ""}}), dict(upserts={"a": {"": "x"}}), dict(upserts={"a": []}), dict(upserts={"a": {}}, deletes=["a"]), dict(deletes="abc"), dict(expected_revision=True), dict(reducer_version="")]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.store.stage("bad", **case)
        for lease in [0, -1, float("inf"), True, "1"]:
            with self.subTest(lease=lease), self.assertRaises(ValueError):
                self.store.claim("none", lease_seconds=lease)

    def test_unicode_and_json_outputs(self):
        self.store.stage("한국어", upserts={"문서": {"주제": "안녕하세요 🎬"}})
        self.store.run("한국어", lambda _: {"summary": "내용", "embedding": [0.1, -0.2], "nested": [None, True]})
        self.assertEqual(self.store.outputs()["주제"]["summary"], "내용")

    def test_concurrent_workers(self):
        self.store.stage("parallel", upserts={"d": {str(i): str(i) for i in range(30)}})
        lock = threading.Lock()
        calls, errors = [], []
        def worker():
            try:
                with Store(self.path) as store:
                    def reducer(item):
                        with lock:
                            calls.append(item.target)
                        time.sleep(.001)
                        return summarize(item)
                    store.run("parallel", reducer)
            except BaseException as exc:
                errors.append(exc)
        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(calls), 30)
        self.assertEqual(len(set(calls)), 30)
        self.assertEqual(self.store.status("parallel").state, "committed")

    def test_process_crash_resume(self):
        self.store.stage("crash", upserts={"d": {"a": "A", "b": "B", "c": "C"}})
        child = multiprocessing.get_context("spawn").Process(target=crash_worker, args=(self.path, "crash"))
        child.start()
        child.join(10)
        self.assertEqual(child.exitcode, 17)
        self.assertEqual(self.store.status("crash").ready, 1)
        self.assertEqual(self.store.outputs(), {})
        time.sleep(.15)
        calls = []
        self.store.run("crash", lambda item: calls.append(item.target) or summarize(item))
        self.assertEqual(calls, ["b", "c"])

    def test_foreign_database_preserved(self):
        path = str(Path(self.temp.name) / "foreign.sqlite")
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE precious(value TEXT)")
            db.execute("INSERT INTO precious VALUES ('keep')")
        with self.assertRaises(ValueError):
            Store(path)
        with closing(sqlite3.connect(path)) as db, db:
            self.assertEqual(db.execute("SELECT * FROM precious").fetchall(), [("keep",)])

    def test_process_crash_during_commit(self):
        self.seed()
        before_docs, before_outputs = self.store.documents(), self.store.outputs()
        self.store.stage("change", deletes=["a"], upserts={"b": {"shared": "new"}})
        while (item := self.store.claim("change")) is not None:
            self.store.complete(item, summarize(item))
        child = multiprocessing.get_context("spawn").Process(target=crash_commit_worker, args=(self.path, "change"))
        child.start()
        child.join(10)
        self.assertEqual(child.exitcode, 19)
        self.assertEqual(self.store.documents(), before_docs)
        self.assertEqual(self.store.outputs(), before_outputs)
        self.assertEqual(self.store.status("change").state, "staged")
        self.store.run("change", lambda _: self.fail("ready results must survive"))
        self.assertEqual(self.store.status("change").state, "committed")

    def test_simultaneous_first_open(self):
        path = str(Path(self.temp.name) / f"new-{time.monotonic_ns()}.sqlite")
        barrier = threading.Barrier(6)
        errors = []
        def opener():
            try:
                barrier.wait()
                with Store(path) as store:
                    self.assertEqual(store.revision, 0)
            except BaseException as exc:
                errors.append(exc)
        threads = [threading.Thread(target=opener) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

    def test_repeated_simultaneous_first_open(self):
        for _ in range(30):
            self.test_simultaneous_first_open()

    def test_committed_claim_is_terminal(self):
        self.store.stage("empty")
        self.store.commit("empty")
        self.assertIsNone(self.store.claim("empty"))

    def test_journal_mode_busy_retry(self):
        real_connect = sqlite3.connect
        attempts = []

        class InitiallyBusy(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if sql == "PRAGMA journal_mode=WAL":
                    attempts.append(sql)
                    if len(attempts) == 1:
                        raise sqlite3.OperationalError("database is locked")
                return super().execute(sql, parameters)

        def connect(*args, **kwargs):
            return real_connect(*args, factory=InitiallyBusy, **kwargs)

        with patch("contribfold.core.sqlite3.connect", side_effect=connect):
            with Store(":memory:") as store:
                self.assertEqual(store.revision, 0)
        self.assertEqual(len(attempts), 2)

    def test_journal_mode_busy_zero_timeout_and_other_errors(self):
        real_connect = sqlite3.connect
        for message, timeout in [("database is locked", 0), ("disk I/O error", 10)]:
            attempts = []

            class FailingJournal(sqlite3.Connection):
                def execute(self, sql, parameters=()):
                    if sql == "PRAGMA journal_mode=WAL":
                        attempts.append(sql)
                        raise sqlite3.OperationalError(message)
                    return super().execute(sql, parameters)

            def connect(*args, **kwargs):
                return real_connect(*args, factory=FailingJournal, **kwargs)

            with self.subTest(message=message), patch("contribfold.core.sqlite3.connect", side_effect=connect):
                with self.assertRaisesRegex(sqlite3.OperationalError, message):
                    Store(":memory:", timeout=timeout)
            self.assertEqual(len(attempts), 1)

    def test_reads_do_not_mutate_store(self):
        self.seed()
        docs = self.store.documents()
        docs["a"]["shared"] = "mutated"
        self.assertEqual(self.store.documents()["a"]["shared"], "A")

    def test_future_schema_rejected(self):
        self.store._db.execute("UPDATE cf_meta SET value='999' WHERE key='schema'")
        with self.assertRaises(ValueError):
            Store(self.path)


if __name__ == "__main__":
    unittest.main()
