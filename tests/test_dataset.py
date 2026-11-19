"""Filters on the shared dashboard data. Run: python -m unittest discover -s tests -v (offline)"""
import os
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

from api import agents, channels, dataset, leads, overview, quality, workspace  # noqa: E402

START = datetime(2026, 11, 18, 7, 0, tzinfo=timezone.utc)
OMAN, TURKEY = "96897245526", "905318880855"


def at(minutes):
    return (START + timedelta(minutes=minutes)).isoformat()


def lead(lead_id, phone, agent, line=OMAN, channel="whatsapp", status="engaged", score=50):
    return {"id": lead_id, "phone": phone, "wa_contact_id": lead_id, "name": lead_id, "status": status, "score": score,
            "assigned_agent": agent, "channel": channel, "last_message_at": at(50), "created_at": at(0),
            "updated_at": at(0), "whatsapp_business_number": line}


def message(mid, lead_id, direction, minute, agent=None, line=OMAN):
    return {"id": mid, "lead_id": lead_id, "direction": direction, "body": "How do I open an account?", "agent_name": agent,
            "sent_at": at(minute), "whatsapp_business_number": line}


def period():
    rows = {
        "L1": lead("L1", "96891111111", "Menatalla Zaky"),
        "L2": lead("L2", "96892222222", "Bashir", line=TURKEY, status="converted"),
        "M1": lead("M1", "96893333333", None, line=None, channel="maqsam"),
    }
    return dataset.Period(
        "today", start=START, end=START + timedelta(days=1), leads=rows,
        activity=[rows["L1"], rows["L2"]],
        calls=[{"id": "C1", "lead_id": "M1", "agent_name": "Menna Khaled", "duration_seconds": 60, "outcome": "completed", "called_at": at(5)},
               {"id": "C2", "lead_id": "L2", "agent_name": "Basher Hallak", "duration_seconds": 0, "outcome": "no_answer", "called_at": at(6)}],
        messages=[message("A", "L1", "inbound", 0), message("B", "L1", "outbound", 4, "Menna Khaled"),
                  message("C", "L2", "inbound", 1, line=TURKEY), message("D", "L2", "outbound", 41, "Bashir", TURKEY)],
        context_messages=[], context_calls=[],
        alerts=[{"id": "X", "lead_id": "L1", "agent_name": None, "severity": "HIGH", "type": "no_reply", "resolved": False, "created_at": at(10), "message": "m"},
                {"id": "Y", "lead_id": "L2", "agent_name": "Bashir", "severity": "MED", "type": "slow_response", "resolved": False, "created_at": at(11), "message": "m"}],
        analyses=[{"id": "Z", "lead_id": "L1", "sentiment": "negative", "topics": ["pricing"], "treatment_score": 40, "risk_flags": [], "analyzed_at": at(12), "source": "whatsapp", "summary": "s", "outcome": "ongoing"}],
    )


class FilterTest(unittest.TestCase):
    def setUp(self):
        self.original = workspace.teams
        workspace.teams = lambda: {"teams": [{"name": "Menna Group", "agents": ["Menna Khaled"]},
                                             {"name": "Basher Group", "agents": ["Bashir Hallak"]}]}
        self.period = period()
        leads._CACHE.get = lambda *a, **k: []
        leads._attach_summaries = lambda rows: None
        dataset.agent_key.cache_clear()

    def tearDown(self):
        workspace.teams = self.original

    def test_all_sections_change_with_the_team(self):
        menna = self.period.view("Menna Group")
        everyone = self.period.view()
        self.assertEqual(overview._overview_inner(everyone)["calls"]["total"], 2)
        self.assertEqual(overview._overview_inner(menna)["calls"]["total"], 1)
        self.assertEqual(overview._overview_inner(menna)["messages"]["stored_total"], 2)
        self.assertEqual(overview._overview_inner(menna)["alerts"]["open"], 1)
        self.assertEqual(leads._leads_inner(menna)["total"], 2)              # own chat + called lead
        self.assertEqual(quality._quality_inner(menna)["sentiment"]["negative"], 1)
        self.assertEqual(quality._quality_inner(self.period.view("Basher Group"))["sentiment"]["negative"], 0)
        self.assertEqual([a["agent"] for a in agents._agents_inner(menna)["agents"]], ["Menna Khaled"])

    def test_whatsapp_line_filters_chats_but_keeps_calls(self):
        oman = self.period.view("", OMAN)
        result = channels._channels_inner(oman)
        self.assertEqual((result["whatsapp"]["leads"], result["whatsapp"]["stored_messages"]), (1, 2))
        self.assertEqual(result["maqsam"]["calls"], 2)
        self.assertEqual(result["maqsam"]["pickup_rate"], 50)
        self.assertEqual(overview._overview_inner(oman)["alerts"]["open"], 1)

    def test_team_and_line_combine(self):
        self.assertEqual(self.period.view("Basher Group", OMAN).activity, [])
        self.assertEqual(len(self.period.view("Basher Group", TURKEY).activity), 1)

    def test_unknown_team_is_ignored_and_views_are_reused(self):
        self.assertEqual(len(self.period.view("Old team").calls), 2)
        self.assertIs(self.period.view("Menna Group"), self.period.view("Menna Group"))

    def test_results_do_not_change_shared_rows(self):
        quality._quality_inner(self.period.view())
        self.assertNotIn("lead_phone", self.period.alerts[0])

    def test_stale_period_is_served_while_refreshing(self):
        started = []
        original = dataset.refresh_in_background
        dataset.refresh_in_background = started.append
        original_bounds = dataset.period_bounds
        dataset.period_bounds = lambda r: (START, START + timedelta(days=1))
        try:
            self.period.loaded_at = 0
            dataset._periods["today"] = self.period
            self.assertIs(dataset.get_period("today"), self.period)
            self.assertEqual(started, ["today"])
        finally:
            dataset.refresh_in_background = original
            dataset.period_bounds = original_bounds
            dataset._periods.pop("today", None)


class ReloadPaceTest(unittest.TestCase):
    """Fewer database reads when nobody is looking at the dashboard."""

    def test_idle_reloads_much_less(self):
        for range_ in ("today", "week", "month"):
            self.assertGreater(dataset.ttl_seconds(range_, idle=True), dataset.ttl_seconds(range_) * 3)
        self.assertEqual(dataset.ttl_seconds("today"), 120)

    def test_using_the_dashboard_marks_it_active(self):
        original_used, original_load = dataset._last_used, dataset.load_period
        try:
            dataset._last_used = 0
            self.assertTrue(dataset.is_idle())
            dataset.load_period = lambda r: period()
            dataset.get_period("custom:test")
            self.assertFalse(dataset.is_idle())
        finally:
            dataset._last_used, dataset.load_period = original_used, original_load
            dataset._periods.pop("custom:test", None)

    def test_warm_skips_fresh_enough_periods_when_idle(self):
        loaded = []
        originals = (dataset.refresh, dataset._current, dataset._last_used)
        p = period()
        p.loaded_at = time.time() - 300  # 5 min old: stale when active, fine when idle
        try:
            dataset.refresh = loaded.append
            dataset._current = lambda r: p
            dataset._last_used = 0
            dataset.warm()
            self.assertEqual(loaded, [])
            dataset._last_used = time.time()
            dataset.warm()
            self.assertIn("today", loaded)
        finally:
            dataset.refresh, dataset._current, dataset._last_used = originals


if __name__ == "__main__":
    unittest.main()
