"""Run: python -m unittest discover -s tests -v   (no credentials needed)"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import reminder_templates as T  # noqa: E402

TZ = ZoneInfo("Etc/GMT-2")
NOW = datetime(2026, 11, 18, 9, 0, tzinfo=timezone.utc)


def item(**kw):
    base = {"name": "Ahmed", "phone": "96891234567", "line": "96897245526", "elapsed_min": 20,
            "since": NOW - timedelta(minutes=20), "body": "كيف أفتح حساب؟", "stage": 1,
            "agent": "Fatma Aqel", "reminders": 1}
    base.update(kw)
    return base


class TemplatesTest(unittest.TestCase):
    def test_agent_first_reminder(self):
        subject, text, html = T.agent_reminder("Fatma Aqel", [item()], 2, TZ, "UTC+2")
        self.assertIn("⏰", subject)
        self.assertIn('dir="rtl"', html)
        self.assertIn('<span dir="ltr">+96891234567</span>', html)
        self.assertIn("كيف أفتح حساب؟", html)
        self.assertIn("+2", html)  # other waiting customers
        self.assertIn("عُمان", text)
        self.assertIn("ManyContacts", text)

    def test_second_reminder_is_red(self):
        subject, _, html = T.agent_reminder("Fatma", [item(stage=2, elapsed_min=50)], 0, TZ, "UTC+2")
        self.assertIn("🔴", subject)
        self.assertIn(T.RED_FG, html)

    def test_customer_text_is_escaped(self):
        _, _, html = T.agent_reminder("Fatma", [item(name="<b>x</b>", body="<script>alert(1)</script>")], 0, TZ, "UTC+2")
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_escalation_groups_by_agent_and_marks_shadow(self):
        items = [item(), item(agent="Bashir Hallak", elapsed_min=300), item(agent=None, phone="lead:xyz")]
        subject, text, html = T.escalation(items, TZ, "UTC+2", 120, shadow=True)
        self.assertTrue(subject.startswith("[SHADOW]"))
        self.assertIn("Bashir Hallak", html)
        self.assertIn("UNASSIGNED", html)
        # longest wait first
        self.assertLess(text.index("Bashir Hallak"), text.index("Fatma Aqel"))

    def test_shadow_digest(self):
        subject, html = T.shadow_digest([{"agent": "Fatma", "items": [item()], "other": 0, "at": NOW}], TZ, "UTC+2")
        self.assertIn("SHADOW", subject)
        self.assertIn("Fatma", html)

    def test_wait_formats(self):
        self.assertEqual(T.wait_ar(25), "25 دقيقة")
        self.assertEqual(T.wait_ar(80), "1 س 20 د")
        self.assertEqual(T.wait_en(120), "2h")


if __name__ == "__main__":
    unittest.main()
