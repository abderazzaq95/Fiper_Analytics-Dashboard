"""Run: python -m unittest discover -s tests -v   (offline; no real credentials)"""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

from pipeline import reports as R  # noqa: E402
from pipeline import report_templates as RT  # noqa: E402
from pipeline.reply_tracker import Schedule, parse_work_days  # noqa: E402

UTC = timezone.utc
TZ = ZoneInfo("Etc/GMT-2")
SCHED = Schedule(tz=TZ, work_days=parse_work_days("mon-fri"), start_hour=9, end_hour=19)
MON = datetime(2026, 11, 16, 7, 0, tzinfo=UTC)  # Monday 09:00 local


def msg(direction, at, body="question about my account", agent=None):
    return {"direction": direction, "sent_at": at.isoformat(), "body": body, "agent_name": agent, "lead_id": "L"}


class ReportCalcTest(unittest.TestCase):
    def setUp(self):
        R._identity_cache.clear()
        self._orig = R.reminders._canonical_agent
        R.reminders._canonical_agent = lambda n: n  # no contact-sheet lookup in tests

    def tearDown(self):
        R.reminders._canonical_agent = self._orig
        R._identity_cache.clear()

    def test_responsiveness_per_agent_and_sla(self):
        convs = {
            "1": {"messages": [msg("inbound", MON), msg("outbound", MON + timedelta(minutes=5), "hi", "Fatma Aqel")], "leads": {}},
            "2": {"messages": [msg("inbound", MON), msg("outbound", MON + timedelta(minutes=40), "hi", "Fatma Aqel")], "leads": {}},
            "3": {"messages": [msg("inbound", MON + timedelta(minutes=10))], "leads": {}},               # unanswered, late
            "4": {"messages": [msg("inbound", MON + timedelta(minutes=58))], "leads": {}},               # unanswered, not late yet
            "5": {"messages": [msg("inbound", MON)], "leads": {}},                                       # answered by call
        }
        agents = {}
        owner = lambda conv: ("bashir hallak", "Bashir Hallak")
        res = R.responsiveness(convs, {"5": [MON + timedelta(minutes=12)]}, MON, MON + timedelta(hours=1), SCHED, owner, agents)
        self.assertEqual(res["customers"], 5)
        self.assertEqual(res["answered"], 3)
        self.assertEqual(res["median_reply"], 12)
        # SLA base excludes conv 4 (only 2 min old): within = conv1(5m) + conv5(12m) of 4
        self.assertEqual(res["sla_pct"], 50)
        fatma = agents["fatma aqel"]
        self.assertEqual((fatma.answered, len(fatma.chats), fatma.within_sla), (2, 2, 1))
        self.assertEqual(agents["bashir hallak"].episodes, 3)  # call + 2 unanswered go to the owner

    def test_call_metrics_pickup_excludes_failed(self):
        agents = {}
        rows = [{"agent_name": "Ali Rizk", "outcome": o, "duration_seconds": d, "called_at": MON.isoformat()}
                for o, d in [("completed", 120), ("completed", 60), ("no_answer", 0), ("busy", 0), ("failed", 0)]]
        res = R.call_metrics(rows, agents, SCHED)
        self.assertEqual(res["calls"], 5)
        self.assertEqual(res["pickup_pct"], 50)
        self.assertEqual(res["talk_seconds"], 180)
        self.assertEqual(agents["ali rizk"].pickup_pct, 50)

    def test_reminder_metrics(self):
        s, e = MON, MON + timedelta(days=1)
        rows = [
            {"status": "answered", "reminder_count": 1, "resolved_at": (MON + timedelta(hours=1)).isoformat(),
             "last_reminded_at": (MON + timedelta(minutes=20)).isoformat()},
            {"status": "open", "reminder_count": 2, "escalated_at": (MON + timedelta(hours=3)).isoformat(),
             "escalation_count": 2, "last_escalated_at": (MON + timedelta(hours=5)).isoformat()},
            {"status": "expired", "resolved_at": (MON + timedelta(hours=6)).isoformat()},
        ]
        res = R.reminder_metrics(rows, s, e)
        self.assertEqual(res["answered_after_reminder"], 1)
        self.assertEqual(res["escalated"], 1)
        self.assertEqual(res["followups"], 1)
        self.assertEqual(res["expired_unanswered"], 1)
        self.assertEqual(res["ai_checked"], 0)

    def test_reminder_metrics_ai_check(self):
        s, e = MON, MON + timedelta(days=1)
        rows = [
            # urgent, answered after 30 working minutes
            {"status": "answered", "ai_source": "claude", "priority": "urgent", "needs_reply": True,
             "waiting_since": MON.isoformat(), "resolved_at": (MON + timedelta(minutes=30)).isoformat()},
            {"status": "open", "ai_source": "claude", "priority": "urgent", "needs_reply": True,
             "waiting_since": (MON + timedelta(hours=2)).isoformat()},
            {"status": "open", "ai_source": "claude", "priority": "low", "needs_reply": False,
             "waiting_since": (MON + timedelta(hours=3)).isoformat()},
            {"status": "open", "ai_source": "rules", "priority": "low", "needs_reply": True,
             "waiting_since": (MON + timedelta(hours=4)).isoformat()},
            {"status": "open", "waiting_since": (MON + timedelta(hours=4)).isoformat()},  # not checked yet
        ]
        res = R.reminder_metrics(rows, s, e, SCHED)
        self.assertEqual((res["decided"], res["ai_checked"], res["urgent"], res["urgent_answered"]), (4, 3, 2, 1))
        self.assertEqual(res["urgent_median_answer_min"], 30)
        self.assertEqual((res["no_reply_needed"], res["low"]), (1, 1))


class ReportRenderTest(unittest.TestCase):
    def _data(self):
        a = R.AgentStats("Fatma Aqel", episodes=4, answered=3, within_sla=2, sla_base=3,
                         reply_minutes=[3, 9, 40], chats={"1", "2", "3"}, calls=20, completed=6, no_answer=10, busy=2,
                         talk_seconds=1800, waiting_now=1)
        resp = {"customers": 10, "episodes": 12, "answered": 10, "median_reply": 8, "sla_pct": 75,
                "by_hour": {9: [3, 5], 14: [40, None]},
                "by_day": {"2026-11-16": {"customers": 10, "median_reply": 8, "sla_pct": 75}}}
        calls = {"calls": 120, "completed": 30, "pickup_pct": 28, "talk_seconds": 7200, "avg_call_seconds": 240,
                 "by_day": {"2026-11-16": {"calls": 120, "pickup_pct": 28}}}
        m = {"resp": resp, "calls": calls, "new_leads": 7, "new_leads_by_channel": {"whatsapp": 5, "maqsam": 2},
             "reminders": {"reminded_customers": 4, "answered_after_reminder": 3, "escalated": 1, "followups": 0,
                           "expired_unanswered": 0}}
        waiting = [{"name": "<b>Ahmed</b>", "phone": "96891234567", "line": "96897245526", "agent": "Fatma Aqel",
                    "elapsed_min": 130, "since": MON, "body": "كيف أسحب؟", "reminders": 2, "escalations": 1}]
        return m, {"fatma aqel": a}, waiting

    def test_daily_render(self):
        m, agents, waiting = self._data()
        subject, html = RT.daily_report({"now": MON + timedelta(hours=8), "start": MON, "metrics": m,
                                         "agents": agents, "waiting": waiting}, TZ, "UTC+2", "19:00 UTC+3")
        self.assertIn("تقرير المبيعات اليومي", subject)
        self.assertIn("Fatma Aqel", html)
        self.assertIn("تم التصعيد", html)
        self.assertNotIn("<b>Ahmed</b>", html)  # escaped
        self.assertIn("fiper-logo-white.png", html)
        self.assertNotIn("عملاء عاجلون", html)  # no AI cards before the AI has checked anything

    def test_daily_render_ai_cards_and_urgent_first(self):
        m, agents, waiting = self._data()
        m["reminders"].update({"ai_checked": 40, "urgent": 6, "urgent_answered": 4, "urgent_median_answer_min": 12,
                               "no_reply_needed": 7, "low": 3})
        waiting.append({"name": "Urgent Sara", "phone": "96890000001", "line": "96897245526", "agent": "Ali Rizk",
                        "elapsed_min": 20, "since": MON, "body": "وين المبلغ", "reminders": 1, "escalations": 0,
                        "priority": "urgent"})
        _, html = RT.daily_report({"now": MON + timedelta(hours=8), "start": MON, "metrics": m,
                                   "agents": agents, "waiting": waiting}, TZ, "UTC+2")
        self.assertIn("عملاء عاجلون", html)
        self.assertIn("لا يحتاج رداً", html)
        self.assertIn("12 min", html)
        self.assertLess(html.index("Urgent Sara"), html.index("&lt;b&gt;Ahmed"))  # urgent listed first

    def test_weekly_render_with_deltas_and_insights(self):
        m, agents, _ = self._data()
        m.update({"after_hours_pct": 22, "topics": [("account_info", 12), ("pricing", 4)], "topics_total": 16,
                  "sentiment": {}, "analyses": 30})
        prev = {"resp": dict(m["resp"], customers=8, median_reply=12, sla_pct=60), "calls": dict(m["calls"], calls=100, pickup_pct=25),
                "new_leads": 5}
        data = {"now": MON + timedelta(days=7), "start": MON, "end": MON + timedelta(days=7), "metrics": m, "prev": prev,
                "agents": agents, "prev_agents": {}, "insights": R.weekly_insights(m, prev, agents)}
        subject, html = RT.weekly_report(data, TZ, "UTC+2")
        self.assertIn("التقرير الأسبوعي", subject)
        self.assertIn("▲", html)                 # customers up
        self.assertIn("الحساب والتسجيل", html)    # topic translated
        self.assertIn("22%", html)               # after-hours share
        self.assertTrue(any("15" in en for _, en in data["insights"]))


if __name__ == "__main__":
    unittest.main()
