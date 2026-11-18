"""Run: python -m unittest discover -s tests -v   (no credentials needed)"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.reply_tracker import (  # noqa: E402
    Ladder,
    Schedule,
    compute_wait_state,
    due_actions,
    is_banal_message,
    parse_work_days,
    phone_key,
)

UTC = timezone.utc
SCHED = Schedule(tz=ZoneInfo("Etc/GMT-2"), work_days=parse_work_days("mon-fri"), start_hour=9, end_hour=19)
LADDER = Ladder(first_min=15, second_min=45, escalate_min=120)


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def msg(direction, at, body="hello, how do I open an account?", agent=None, lead="L1"):
    return {"direction": direction, "sent_at": at.isoformat(), "body": body, "agent_name": agent, "lead_id": lead}


class WorkingClockTest(unittest.TestCase):
    # 2026-11-16 is a Monday. 09:00 UTC+2 == 07:00 UTC.

    def test_same_day_inside_hours(self):
        self.assertEqual(SCHED.working_minutes_between(utc(2026, 11, 16, 8, 0), utc(2026, 11, 16, 8, 30)), 30)

    def test_overnight_message_starts_at_nine(self):
        # Mon 23:00 local -> Tue 09:20 local = 20 working minutes
        start = utc(2026, 11, 16, 21, 0)
        end = utc(2026, 11, 17, 7, 20)
        self.assertEqual(SCHED.working_minutes_between(start, end), 20)

    def test_weekend_is_skipped(self):
        # Fri 18:50 local -> Mon 09:10 local = 10 + 10 minutes
        start = utc(2026, 11, 20, 16, 50)
        end = utc(2026, 11, 23, 7, 10)
        self.assertEqual(SCHED.working_minutes_between(start, end), 20)

    def test_is_working_time(self):
        self.assertTrue(SCHED.is_working_time(utc(2026, 11, 16, 7, 0)))    # Mon 09:00 local
        self.assertFalse(SCHED.is_working_time(utc(2026, 11, 16, 17, 0)))  # Mon 19:00 local
        self.assertFalse(SCHED.is_working_time(utc(2026, 11, 21, 10, 0)))  # Saturday

    def test_work_days_parsing(self):
        self.assertEqual(parse_work_days("sun-thu"), frozenset({6, 0, 1, 2, 3}))
        self.assertEqual(parse_work_days("mon,wed"), frozenset({0, 2}))


class WaitStateTest(unittest.TestCase):
    t0 = utc(2026, 11, 16, 8, 0)

    def test_unanswered_customer_is_waiting_from_first_message(self):
        s = compute_wait_state([
            msg("outbound", self.t0, "welcome", agent="Fatma"),
            msg("inbound", self.t0 + timedelta(minutes=5)),
            msg("inbound", self.t0 + timedelta(minutes=9), "are you there?"),
        ])
        self.assertEqual(s.waiting_since, self.t0 + timedelta(minutes=5))
        self.assertEqual(s.last_inbound["body"], "are you there?")
        self.assertEqual(s.last_agent_name, "Fatma")

    def test_agent_reply_closes_wait(self):
        s = compute_wait_state([
            msg("inbound", self.t0),
            msg("outbound", self.t0 + timedelta(minutes=3), "sure!", agent="Fatma"),
        ])
        self.assertIsNone(s.waiting_since)

    def test_call_closes_wait(self):
        s = compute_wait_state([msg("inbound", self.t0)], call_times=[self.t0 + timedelta(minutes=20)])
        self.assertIsNone(s.waiting_since)

    def test_call_before_message_does_not_close_wait(self):
        s = compute_wait_state([msg("inbound", self.t0)], call_times=[self.t0 - timedelta(minutes=20)])
        self.assertEqual(s.waiting_since, self.t0)

    def test_thanks_after_reply_is_not_waiting(self):
        s = compute_wait_state([
            msg("inbound", self.t0),
            msg("outbound", self.t0 + timedelta(minutes=3), "here is the link", agent="Fatma"),
            msg("inbound", self.t0 + timedelta(minutes=4), "شكرا"),
            msg("inbound", self.t0 + timedelta(minutes=5), "[sticker]"),
        ])
        self.assertIsNone(s.waiting_since)

    def test_old_message_then_newer_chat(self):
        # The original complaint: an old question was followed by a conversation.
        old = self.t0 - timedelta(days=14)
        s = compute_wait_state([
            msg("inbound", old, "old question"),
            msg("outbound", old + timedelta(days=13), "hi again", agent="Bashir"),
            msg("inbound", old + timedelta(days=13, minutes=2), "ok"),
        ])
        self.assertIsNone(s.waiting_since)

    def test_messages_from_duplicate_lead_rows_are_merged(self):
        s = compute_wait_state([
            msg("inbound", self.t0, lead="L1"),
            msg("outbound", self.t0 + timedelta(minutes=2), "answer", agent="Ali", lead="L2"),
        ])
        self.assertIsNone(s.waiting_since)


class EpisodesTest(unittest.TestCase):
    t0 = utc(2026, 11, 16, 8, 0)

    def test_episodes_answered_by_whatsapp_and_call_and_open(self):
        from pipeline.reply_tracker import wait_episodes
        eps = wait_episodes([
            msg("inbound", self.t0),
            msg("inbound", self.t0 + timedelta(minutes=2), "hello??"),
            msg("outbound", self.t0 + timedelta(minutes=10), "hi", agent="Fatma"),
            msg("inbound", self.t0 + timedelta(minutes=20), "شكرا"),          # banal: no new wait
            msg("inbound", self.t0 + timedelta(minutes=30), "how to withdraw?"),
            msg("inbound", self.t0 + timedelta(minutes=90), "any update?"),
        ], call_times=[self.t0 + timedelta(minutes=45)])
        self.assertEqual(len(eps), 3)
        self.assertEqual((eps[0].kind, eps[0].agent, eps[0].end - eps[0].start), ("whatsapp", "Fatma", timedelta(minutes=10)))
        self.assertEqual((eps[1].kind, eps[1].end - eps[1].start), ("call", timedelta(minutes=15)))
        self.assertIsNone(eps[2].end)

    def test_median(self):
        from pipeline.reply_tracker import median
        self.assertEqual(median([5, 1, 3]), 3)
        self.assertEqual(median([1, 2, 3, 4]), 2.5)
        self.assertIsNone(median([]))


class LadderTest(unittest.TestCase):
    now = utc(2026, 11, 16, 9, 0)

    def due(self, elapsed, count=0, last=None, escalated=False, agent=True):
        return due_actions(elapsed_working_min=elapsed, reminder_count=count, last_reminded_at=last,
                           escalated=escalated, has_agent=agent, now=self.now, ladder=LADDER)

    def test_nothing_before_first_step(self):
        self.assertEqual(self.due(14), set())

    def test_first_and_second_reminder(self):
        self.assertEqual(self.due(15), {"remind"})
        self.assertEqual(self.due(30, count=1, last=self.now - timedelta(minutes=15)), set())
        self.assertEqual(self.due(45, count=1, last=self.now - timedelta(minutes=30)), {"remind"})

    def test_no_third_reminder_before_escalation(self):
        self.assertEqual(self.due(90, count=2, last=self.now - timedelta(minutes=45)), set())

    def test_escalation_once(self):
        self.assertEqual(self.due(120, count=2), {"escalate"})
        self.assertEqual(self.due(300, count=2, escalated=True), set())

    def test_late_discovery_reminds_agent_once_and_escalates(self):
        self.assertEqual(self.due(200, count=0), {"remind", "escalate"})

    def test_unassigned_goes_to_supervisor(self):
        self.assertEqual(self.due(15, agent=False), {"escalate"})
        self.assertEqual(self.due(500, agent=False, escalated=True), set())

    def test_min_gap_between_reminders(self):
        self.assertEqual(self.due(50, count=1, last=self.now - timedelta(minutes=5)), set())

    def followup(self, sent, hours_since_first):
        return due_actions(elapsed_working_min=999, reminder_count=2, last_reminded_at=None, escalated=True,
                           has_agent=True, now=self.now, ladder=LADDER, escalation_count=sent,
                           first_escalated_at=self.now - timedelta(hours=hours_since_first))

    def test_followup_after_one_day_then_three_days(self):
        self.assertEqual(self.followup(1, 23), set())
        self.assertEqual(self.followup(1, 24), {"followup"})
        self.assertEqual(self.followup(2, 48), set())
        self.assertEqual(self.followup(2, 72), {"followup"})
        self.assertEqual(self.followup(3, 500), set())  # nothing after the final follow-up

    def test_rows_escalated_before_followups_existed_count_as_one(self):
        self.assertEqual(self.followup(0, 25), {"followup"})

    def test_agent_never_pinged_about_very_old_wait(self):
        # More than 20 working hours: supervisor only, no agent reminder.
        self.assertEqual(self.due(21 * 60, count=0), {"escalate"})
        self.assertEqual(self.due(21 * 60, count=0, agent=False), {"escalate"})

    def test_weekend_message_still_reminds_agent_on_monday(self):
        # Friday 20:00 local -> Monday 09:15 local is only 15 working minutes.
        fri = utc(2026, 11, 20, 18, 0)
        mon = utc(2026, 11, 23, 7, 15)
        elapsed = SCHED.working_minutes_between(fri, mon)
        self.assertEqual(elapsed, 15)
        self.assertEqual(self.due(elapsed), {"remind"})


class HelpersTest(unittest.TestCase):
    def test_banal(self):
        for text in ["ok", "Thanks!", "شكرا", "تمام", "👍", "[sticker]", "123", ""]:
            self.assertTrue(is_banal_message(text), text)
        for text in ["How do I deposit?", "ابغى افتح حساب", "ok but what is the minimum deposit for gold?"]:
            self.assertFalse(is_banal_message(text), text)

    def test_phone_key(self):
        self.assertEqual(phone_key("+968 9123-4567"), "96891234567")
        self.assertIsNone(phone_key("abc"))


if __name__ == "__main__":
    unittest.main()
