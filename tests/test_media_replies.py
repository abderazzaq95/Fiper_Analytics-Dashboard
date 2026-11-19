"""Agent voice notes / images (WhatsApp "sent" events) count as replies.
Run: python -m unittest discover -s tests -v (offline)"""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

from api import workspace  # noqa: E402
from pipeline import reminders  # noqa: E402
from pipeline.reply_tracker import agent_send_times, compute_wait_state  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 11, 13, 8, 49, tzinfo=UTC)
PHONE = "212603420704"


def at(minutes, seconds=0):
    return (T0 + timedelta(minutes=minutes, seconds=seconds)).isoformat()


def inbound(minutes, body="واش كينا شي طريقة نعبأبيها؟", seconds=0):
    return {"direction": "inbound", "sent_at": at(minutes, seconds), "body": body, "lead_id": "L1"}


def send(minutes, category="service", seconds=0):
    return {"phone": PHONE, "sent_at": at(minutes, seconds), "category": category}


class AgentSendTimesTest(unittest.TestCase):
    def test_voice_note_reply_closes_the_wait(self):
        messages = [inbound(0), inbound(2)]
        contact = agent_send_times(messages, [send(103)])          # voice note 1h43 later
        state = compute_wait_state(messages, contact)
        self.assertIsNone(state.waiting_since)
        self.assertEqual(state.last_agent_contact_at, T0 + timedelta(minutes=103))

    def test_customer_writing_again_after_the_voice_note_opens_a_new_wait(self):
        messages = [inbound(0), inbound(120, "اقل من 250usdt غير ممكن ياك؟؟؟؟")]
        state = compute_wait_state(messages, agent_send_times(messages, [send(103)]))
        self.assertEqual(state.waiting_since, T0 + timedelta(minutes=120))

    def test_template_broadcasts_do_not_count(self):
        for category in ("marketing", "utility", "authentication"):
            self.assertEqual(agent_send_times([inbound(0)], [send(30, category)]), [])
        self.assertEqual(len(agent_send_times([inbound(0)], [send(30, None)])), 1)  # no category: counts

    def test_instant_auto_reply_does_not_count(self):
        self.assertEqual(agent_send_times([inbound(0)], [send(0, seconds=5)]), [])
        self.assertEqual(len(agent_send_times([inbound(0)], [send(0, seconds=40)])), 1)


class ReminderSyncTest(unittest.TestCase):
    def test_contact_times_merge_calls_and_sends(self):
        call = T0 + timedelta(minutes=500)
        convs = {PHONE: {"messages": [inbound(0)]}}
        with mock.patch.object(reminders, "_load_call_times", return_value={PHONE: [call]}), \
             mock.patch.object(reminders, "_load_sends", return_value={PHONE: [send(103), send(0, seconds=3)]}):
            times = reminders._contact_times(convs, [PHONE], T0)
        self.assertEqual(sorted(times[PHONE]), [T0 + timedelta(minutes=103), call])

    def test_missing_table_means_no_sends_not_a_crash(self):
        broken = mock.Mock()
        broken.table.side_effect = RuntimeError('relation "whatsapp_sends" does not exist')
        with mock.patch.object(reminders, "supabase", broken):
            self.assertEqual(reminders._load_sends([PHONE], T0), {})


class ConversationTimelineTest(unittest.TestCase):
    def test_voice_notes_show_but_sent_texts_are_not_doubled(self):
        messages = [inbound(0), {"direction": "outbound", "sent_at": at(10), "body": "*Zahrae:* Salam", "agent_name": "Zahrae"}]
        sends = [{"sent_at": at(10, 4), "category": "service"},      # the text above
                 {"sent_at": at(103), "category": "service"}]         # a voice note
        events = workspace.build_timeline(messages, [], sends=sends)
        media = [e for e in events if e["type"] == "media"]
        self.assertEqual([e["at"] for e in media], [at(103)])
        self.assertEqual([e["type"] for e in events], ["message", "message", "media"])


class WebhookRecordTest(unittest.TestCase):
    def setUp(self):
        import main
        self.main = main
        self.db = mock.Mock()
        main._sends_retry_after = None

    def record(self, status):
        with mock.patch.object(self.main, "supabase", self.db):
            self.main._record_whatsapp_send(status, "905318880855", "2026-11-13T10:00:00+00:00")

    def test_sent_status_is_stored(self):
        self.record({"id": "wamid.X", "status": "sent", "recipient_id": "212603420704", "timestamp": "1763031120",
                     "pricing": {"category": "service"}})
        row = self.db.table.return_value.upsert.call_args[0][0]
        self.assertEqual((row["wa_message_id"], row["phone"], row["category"], row["business_line"]),
                         ("wamid.X", PHONE, "service", "905318880855"))
        self.assertEqual(row["sent_at"], self.main._ts_to_iso("1763031120", "unused"))
        self.assertTrue(self.db.table.return_value.upsert.call_args[1]["ignore_duplicates"])

    def test_number_read_from_the_message_id_when_missing(self):
        wamid = "wamid.HBgLOTY4OTMxODk2NjkVAgARGBI5MkY3QTYyMkY2QTk1ODExRjkA"   # from the Render log
        self.record({"id": wamid, "status": "sent", "timestamp": "1763031120"})
        self.assertEqual(self.db.table.return_value.upsert.call_args[0][0]["phone"], "96893189669")
        self.assertEqual(self.main._phone_from_wamid("not-an-id"), "")

    def test_delivered_and_read_are_ignored(self):
        for status in ("delivered", "read", "failed"):
            self.record({"id": "wamid.X", "status": status, "recipient_id": PHONE, "timestamp": "1763031120"})
        self.db.table.assert_not_called()

    def test_missing_table_pauses_writes(self):
        self.db.table.return_value.upsert.return_value.execute.side_effect = RuntimeError(
            "Could not find the table 'public.whatsapp_sends' (PGRST205)")
        self.record({"id": "wamid.A", "status": "sent", "recipient_id": PHONE, "timestamp": "1763031120"})
        self.record({"id": "wamid.B", "status": "sent", "recipient_id": PHONE, "timestamp": "1763031120"})
        self.assertEqual(self.db.table.call_count, 1)
        self.assertIsNotNone(self.main._sends_retry_after)


if __name__ == "__main__":
    unittest.main()
