"""Tests for structured job log querying and combined filtering."""

import shutil
import tempfile
import unittest

from backend.common.logbus import LogBus
from backend.common.storage import Storage, append_jsonl


class LogBusQueryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.logs = LogBus(self.storage)
        self.job_id = "job-test"
        self.base_ts = 1_700_000_000_000

        self.logs.info(
            self.job_id, "map keyword event",
            stage="map", task_id="map-1", worker_id="w1",
            ts_ms=self.base_ts,
        )
        self.logs.warn(
            self.job_id, "shuffle warning",
            stage="shuffle", task_id="shuffle-1", worker_id="w2",
            ts_ms=self.base_ts + 1_000,
        )
        self.logs.error(
            self.job_id, "reduce failure keyword",
            stage="reduce", task_id="reduce-1", worker_id="w3",
            ts_ms=self.base_ts + 2_000,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_returns_timestamp_and_chronological_order(self):
        result = self.logs.query(self.job_id)
        self.assertEqual(result["total"], 3)
        self.assertEqual([r["ts_ms"] for r in result["records"]], [
            self.base_ts,
            self.base_ts + 1_000,
            self.base_ts + 2_000,
        ])

    def test_normalises_legacy_timestamp_field(self):
        append_jsonl(
            self.storage.path("jobs", self.job_id, "logs", "master", "old.jsonl"),
            {"ts": self.base_ts + 500, "level": "INFO", "message": "legacy record"},
        )
        result = self.logs.query(self.job_id, search="legacy")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["records"][0]["ts_ms"], self.base_ts + 500)

    def test_search_stage_level_and_time_filters_combine(self):
        result = self.logs.query(
            self.job_id,
            search="keyword",
            level="error",
            stage="reduce",
            start_ms=self.base_ts + 1_500,
            end_ms=self.base_ts + 2_500,
        )
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["records"][0]["message"], "reduce failure keyword")

    def test_each_filter_can_exclude_all_records(self):
        self.assertEqual(self.logs.query(self.job_id, search="missing")["total"], 0)
        self.assertEqual(self.logs.query(self.job_id, level="error", stage="map")["total"], 0)
        self.assertEqual(self.logs.query(self.job_id, start_ms=self.base_ts + 5_000)["total"], 0)
        self.assertEqual(self.logs.query(self.job_id, end_ms=self.base_ts - 1)["total"], 0)

    def test_legacy_task_stage_is_mapped_for_stage_filter(self):
        for task_id, stage in (("m-0000", "map"), ("r-0001", "reduce")):
            append_jsonl(
                self.storage.path("jobs", self.job_id, "logs", "task", f"{task_id}.jsonl"),
                {"ts_ms": self.base_ts, "level": "INFO", "stage": "task", "message": task_id},
            )
            result = self.logs.query(self.job_id, stage=stage, task_id=task_id)
            self.assertEqual([r["task_id"] for r in result["records"]], [task_id])

    def test_level_matching_is_case_insensitive(self):
        result = self.logs.query(self.job_id, level="warn")
        self.assertEqual([r["message"] for r in result["records"]], ["shuffle warning"])


if __name__ == "__main__":
    unittest.main()
