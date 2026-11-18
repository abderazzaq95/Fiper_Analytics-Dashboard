"""Run: python -m unittest discover -s tests -v   (offline)"""
import os
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

from pipeline.swr_cache import SWRCache  # noqa: E402
from api import leads as L  # noqa: E402


class SWRTest(unittest.TestCase):
    def test_fresh_stale_and_background_refresh(self):
        cache = SWRCache("t", ttl=0.2)
        calls = []

        def compute():
            calls.append(1)
            return len(calls)

        self.assertEqual(cache.get("k", compute), 1)          # first load computes
        self.assertEqual(cache.get("k", compute), 1)          # fresh: cached
        time.sleep(0.25)
        self.assertEqual(cache.get("k", compute), 1)          # stale: old value instantly ...
        for _ in range(50):                                   # ... while it rebuilds in background
            if cache.get("k", compute, ttl=10) == 2:
                break
            time.sleep(0.02)
        self.assertEqual(cache.get("k", compute, ttl=10), 2)

    def test_keep_rejects_empty_results(self):
        cache = SWRCache("t", ttl=10)
        self.assertEqual(cache.get("k", lambda: {}, keep=bool), {})
        self.assertEqual(cache.get("k", lambda: {"x": 1}, keep=bool), {"x": 1})  # empty one was not cached

    def test_background_errors_keep_old_value(self):
        cache = SWRCache("t", ttl=0)
        cache.get("k", lambda: "good")
        cache.get("k", lambda: 1 / 0)
        time.sleep(0.05)
        self.assertEqual(cache.get("k", lambda: 1 / 0, ttl=10), "good")


class LeadsAggregatesTest(unittest.TestCase):
    def test_geo_prefixes_and_avg_score(self):
        rows = [{"phone": "+968 9123 4567", "score": 80}, {"phone": "0096891111111", "score": 40},
                {"phone": "966501234567", "score": None}, {"phone": "", "score": 10}]
        self.assertEqual(L._geo_prefixes(rows), {"9689": 2, "9665": 1})
        self.assertEqual(L._avg_score(rows), 43.3)


if __name__ == "__main__":
    unittest.main()
