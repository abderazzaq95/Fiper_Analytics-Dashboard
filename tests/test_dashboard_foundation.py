"""Run: python -m unittest discover -s tests -v   (offline)"""
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

from api import workspace  # noqa: E402
from pipeline import agent_names as N  # noqa: E402
from pipeline import time_ranges as R  # noqa: E402

WED = datetime(2026, 11, 18, 10, 0, tzinfo=timezone.utc)


class AgentNamesTest(unittest.TestCase):
    def test_confirmed_people_have_one_name(self):
        self.assertEqual(N.display("Menatalla Zaky"), "Menna Khaled")
        self.assertEqual(N.display("menna khaled"), "Menna Khaled")
        self.assertEqual(N.display("Zahrae"), "Fatima Zohra Hbada")
        self.assertEqual(N.key("Zahrae"), N.key("fatima zohra hbada"))

    def test_spellings_across_systems(self):
        self.assertEqual(N.display("BASHİR Hallak"), "Bashir Hallak")
        self.assertEqual(N.display("Basher Hallak"), "Bashir Hallak")
        self.assertEqual(N.display("joeseph kesserwany"), "Joseph Kesserwany")
        self.assertEqual(N.display("محمد ضافر"), "Mohamed Dhafeer")

    def test_unknown_names_are_not_merged(self):
        self.assertEqual(N.display("mohamed"), "Mohamed")
        self.assertIsNone(N.canonical("Mohamed"))
        self.assertEqual(N.display("GHAITH ALFAISAL"), "Ghaith Alfaisal")
        self.assertIsNone(N.display("  "))


class PeriodTest(unittest.TestCase):
    def test_yesterday_is_a_closed_day(self):
        start, end = R.period_bounds("yesterday", WED)
        self.assertEqual(start.isoformat(), "2026-11-16T22:00:00+00:00")
        self.assertEqual(end.isoformat(), "2026-11-17T22:00:00+00:00")
        self.assertTrue(R.is_closed("yesterday", WED))
        self.assertFalse(R.is_closed("today", WED))

    def test_custom_range_includes_last_day_and_accepts_reversed_dates(self):
        start, end = R.period_bounds("custom:2026-11-10:2026-11-03", WED)
        self.assertEqual(start.isoformat(), "2026-11-02T22:00:00+00:00")
        self.assertEqual(end.isoformat(), "2026-11-10T22:00:00+00:00")

    def test_open_custom_range_is_not_closed(self):
        self.assertFalse(R.is_closed("custom:2026-11-01:2026-11-18", WED))

    def test_pattern(self):
        import re
        for ok in ["today", "yesterday", "month", "custom:2026-01-01:2026-01-31"]:
            self.assertTrue(re.match(R.RANGE_PATTERN, ok), ok)
        for bad in ["custom:2026-01-01", "custom:x:y", "all"]:
            self.assertFalse(re.match(R.RANGE_PATTERN, bad), bad)


class WorkspaceTest(unittest.TestCase):
    def test_teams_use_display_names_and_skip_inactive(self):
        teams = workspace.build_teams([
            {"name": "menatalla zaky", "groups": ["Menna Group"]},
            {"name": "‏Fatima Akel", "groups": [{"name": "Menna Group"}, "All"]},
            {"name": "Left Agent", "groups": ["Menna Group"], "active": False},
        ])
        self.assertEqual(teams, [{"name": "All", "agents": ["Fatma Aqel"]},
                                 {"name": "Menna Group", "agents": ["Fatma Aqel", "Menna Khaled"]}])

    def test_timeline_is_chronological_and_keeps_latest(self):
        events = workspace.build_timeline(
            [{"sent_at": "2026-11-18T10:00:00+00:00", "direction": "inbound", "body": "hi"},
             {"sent_at": "2026-11-18T10:05:00+00:00", "direction": "outbound", "body": "ok", "agent_name": "Zahrae"}],
            [{"called_at": "2026-11-18T10:02:00+00:00", "agent_name": "Ali Rizk", "outcome": "completed", "duration_seconds": 30}],
            limit=2)
        self.assertEqual([e["type"] for e in events], ["call", "message"])
        self.assertEqual(events[1]["agent"], "Fatima Zohra Hbada")

    def test_phone_variants(self):
        self.assertEqual(workspace.phone_variants("+968 9123-4567"), ["96891234567", "+96891234567"])
        self.assertEqual(workspace.phone_variants("12"), [])


if __name__ == "__main__":
    unittest.main()
