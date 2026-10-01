"""Tests for structured log emission and the log-search query filters."""

import shutil
import tempfile
import time
import unittest

from backend.common.logbus import LogBus
from backend.common.storage import Storage, append_jsonl


class TestLogBus(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bus = LogBus(Storage(self.tmp))
        self.job = "job-test"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _emit(self, ms, level, message, **kw):
        rec = self.bus.emit(
            self.job, level, message, ts_ms=ms,
            stage=kw.pop("stage", "master"),
            task_id=kw.pop("task_id", "job"),
            worker_id=kw.pop("worker_id", ""),
        )
        return rec

    def test_records_carry_ts_ms_and_sort_chronologically(self):
        self._emit(3000, "INFO", "third")
        self._emit(1000, "INFO", "first")
        self._emit(2000, "INFO", "second")
        recs = self.bus.query(self.job)["records"]
        self.assertEqual([r["message"] for r in recs], ["first", "second", "third"])
        self.assertTrue(all("ts_ms" in r for r in recs))
        self.assertEqual([r["ts_ms"] for r in recs], [1000, 2000, 3000])

    def test_keyword_search_matches_message_case_insensitively(self):
        self._emit(1000, "INFO", "job submitted HELLO")
        self._emit(2000, "INFO", "nothing relevant")
        recs = self.bus.query(self.job, search="hello")["records"]
        self.assertEqual([r["message"] for r in recs], ["job submitted HELLO"])

    def test_level_filter(self):
        self._emit(1000, "INFO", "ok")
        self._emit(2000, "WARN", "careful")
        self._emit(3000, "ERROR", "boom")
        self.assertEqual(
            [r["level"] for r in self.bus.query(self.job, level="warn")["records"]],
            ["WARN"],
        )

    def test_stage_filter(self):
        self._emit(1000, "INFO", "m", stage="map", task_id="m1")
        self._emit(2000, "INFO", "r", stage="reduce", task_id="r1")
        recs = self.bus.query(self.job, stage="reduce")["records"]
        self.assertEqual([r["stage"] for r in recs], ["reduce"])

    def test_time_range_inclusive(self):
        self._emit(1000, "INFO", "a")
        self._emit(2000, "INFO", "b")
        self._emit(3000, "INFO", "c")
        q = self.bus.query(self.job, start_ms=2000, end_ms=2000)["records"]
        self.assertEqual([r["message"] for r in q], ["b"])

    def test_filters_combine_with_AND(self):
        self._emit(1000, "INFO", "boom", stage="map", task_id="m1")
        self._emit(2000, "ERROR", "boom", stage="map", task_id="m2")
        self._emit(3000, "ERROR", "other", stage="map", task_id="m3")
        self._emit(4000, "ERROR", "boom", stage="reduce", task_id="r1")
        recs = self.bus.query(
            self.job, search="boom", level="ERROR",
            stage="map", start_ms=1500, end_ms=2500,
        )["records"]
        self.assertEqual([r["task_id"] for r in recs], ["m2"])

    def test_legacy_ts_field_is_normalised(self):
        # Records written by older builds stored the epoch-ms under "ts".
        path = self.bus.storage.path("jobs", self.job, "logs", "master", "job.jsonl")
        append_jsonl(path, {"ts": 1234, "level": "INFO", "message": "old row"})
        recs = self.bus.query(self.job)["records"]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["ts_ms"], 1234)
        self.assertNotIn("ts", recs[0])

    def test_scanned_counts_every_record(self):
        self._emit(1000, "INFO", "a")
        self._emit(2000, "ERROR", "b")
        result = self.bus.query(self.job, level="ERROR")
        self.assertEqual(result["scanned"], 2)
        self.assertEqual(result["total"], 1)

    def test_limit_does_not_change_total(self):
        for i in range(5):
            self._emit(1000 + i, "INFO", f"row{i}")
        result = self.bus.query(self.job, limit=2)
        self.assertEqual(result["total"], 5)
        self.assertEqual(len(result["records"]), 2)


if __name__ == "__main__":
    unittest.main()
