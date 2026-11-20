"""AI check, priorities and the morning list. Run: python -m unittest discover -s tests -v (offline)"""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

from pipeline import reminder_templates as templates  # noqa: E402
from pipeline import reminders, reply_ai  # noqa: E402

UTC = timezone.utc
TUE_0920 = datetime(2026, 11, 17, 7, 20, tzinfo=UTC)   # Tuesday 09:20 local (UTC+2)
TUE_0900 = datetime(2026, 11, 17, 7, 0, tzinfo=UTC)


class FakeQuery:
    def __init__(self, db, table):
        self.db, self.table, self.fields = db, table, None

    def update(self, fields):
        self.fields = fields
        return self

    def select(self, *_):
        return self

    def eq(self, *_):
        return self

    def gte(self, *_):
        return self

    def execute(self):
        if self.fields is not None:
            self.db.updates.append((self.table, self.fields))
        return mock.Mock(data=self.db.rows.get(self.table, []))


class FakeDB:
    def __init__(self, rows=None):
        self.updates, self.rows = [], rows or {}

    def table(self, name):
        return FakeQuery(self, name)


def context(*customer, agent_last="Welcome to Fiper", first=False):
    return {"customer": list(customer), "agent_last": agent_last, "first_contact": first}


class EscalationTelegramTest(unittest.TestCase):
    def test_several_chat_ids_each_get_the_escalation(self):
        sent = []
        with mock.patch.dict(os.environ, {"REMINDER_ESCALATION_TELEGRAM_CHAT_ID": "449195882, 1244415565,449195882",
                                          "REMINDER_ESCALATION_EMAILS": "", "REMINDER_ESCALATION_WHATSAPP": "",
                                          "REMINDER_MODE": "live"}), \
             mock.patch.object(reminders, "_escalation_texts", return_value=("s", "t", "<p>t</p>")), \
             mock.patch.object(reminders, "_send", side_effect=lambda ch, to, *a: sent.append((ch, to)) or ("sent", None)), \
             mock.patch.object(reminders, "_log"):
            reminders._deliver_escalation([{"row": {"id": "r1"}}])
        self.assertEqual([to for ch, to in sent if ch == "telegram"], ["449195882", "1244415565"])

    def test_single_id_and_admin_fallback_still_work(self):
        with mock.patch.dict(os.environ, {"REMINDER_ESCALATION_TELEGRAM_CHAT_ID": "449195882"}):
            self.assertEqual(reminders._escalation_telegram(), ["449195882"])
        with mock.patch.dict(os.environ, {"REMINDER_ESCALATION_TELEGRAM_CHAT_ID": ""}), \
             mock.patch.object(reminders.notify, "TELEGRAM_ADMIN_CHAT_ID", "1244415565"):
            self.assertEqual(reminders._escalation_telegram(), ["1244415565"])


class RulesTest(unittest.TestCase):
    def test_live_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("REMINDER_AI_MODE", None)
            self.assertEqual(reply_ai.mode(), "live")

    def test_rules_never_silence_a_reminder(self):
        for texts in (["شكرا"], ["ok"], ["👍"], [], ["مشكوره في انتظاركم"]):
            self.assertTrue(reply_ai.rules(context(*texts))["needs_reply"], texts)

    def test_rule_priorities(self):
        self.assertEqual(reply_ai.rules(context("بدي اسحب ارباحي"))["priority"], "urgent")
        self.assertEqual(reply_ai.rules(context("انا قاعد اقوللكم ان الفلوس مو واصله"))["priority"], "urgent")
        # Without the AI the rules never escalate faster than before for ordinary chats.
        self.assertEqual(reply_ai.rules(context("a", "b", "c"))["priority"], "normal")
        self.assertEqual(reply_ai.rules(context("كيف افتح حساب", first=True))["priority"], "normal")
        self.assertEqual(reply_ai.rules(context("السلام عليكم"))["priority"], "low")
        self.assertEqual(reply_ai.rules(context("صباح الخير اخ بشير", "[image]"))["priority"], "low")
        self.assertEqual(reply_ai.rules(context("السلام عليكم", "كيف بالنسبه لاضافة الكريديت"))["priority"], "normal")
        self.assertEqual(reply_ai.rules(context("السلام عليكم كيف اضيف الكريديت؟"))["priority"], "normal")
        self.assertEqual(reply_ai.rules(context("السلام عليكم", first=True))["priority"], "normal")
        self.assertEqual(reply_ai.rules(context("what are the trading hours?"))["priority"], "normal")

    def test_parse_answer(self):
        self.assertEqual(reply_ai.parse_answer('```json\n{"needs_reply": false, "priority": "low", "reason": "thanks only"}\n```'),
                         {"needs_reply": False, "priority": "low", "reason": "thanks only", "source": "gemini"})
        self.assertEqual(reply_ai.parse_answer('{"needs_reply": "maybe", "priority": "huge"}')["priority"], "normal")
        self.assertTrue(reply_ai.parse_answer('{"needs_reply": "maybe"}')["needs_reply"])  # only an explicit false skips
        self.assertIsNone(reply_ai.parse_answer("sorry, I cannot"))


def reset_ai_state():
    reply_ai._state.update(day=None, used=0, last_call=0.0, last_error=None, paused={"gemini": None, "claude": None})


def claude_reply(text='{"needs_reply": true, "priority": "urgent", "reason": "withdrawal not received"}', stop="end_turn"):
    client = mock.Mock()
    message = mock.Mock(stop_reason=stop, content=[mock.Mock(type="thinking"), mock.Mock(type="text", text=text)])
    client.beta.messages.create.return_value = message
    client.messages.create.return_value = message
    return client


class ClassifyTest(unittest.TestCase):
    def setUp(self):
        reset_ai_state()
        self.env = mock.patch.dict(os.environ, {"GEMINI_API_KEY": "test-key", "ANTHROPIC_API_KEY": "",
                                                "REMINDER_AI_CLAUDE_MODEL": ""})
        self.env.start()
        self.spacing = mock.patch.object(reply_ai, "MIN_SECONDS_BETWEEN_CALLS", 0)
        self.spacing.start()

    def tearDown(self):
        self.env.stop()
        self.spacing.stop()
        reset_ai_state()

    def reply(self, status, text=""):
        response = mock.Mock(status_code=status)
        response.json.return_value = {"candidates": [{"content": {"parts": [{"text": text}]}}]}
        response.raise_for_status.side_effect = None if status < 400 else RuntimeError(status)
        return response

    def test_ai_answer_is_used(self):
        with mock.patch.object(reply_ai.requests, "post", return_value=self.reply(200, '{"needs_reply": false, "priority": "low", "reason": "closing thanks"}')):
            self.assertEqual(reply_ai.classify(context("مشكور"))["needs_reply"], False)

    def test_quota_error_falls_back_and_pauses(self):
        with mock.patch.object(reply_ai.requests, "post", return_value=self.reply(429)) as post:
            self.assertEqual(reply_ai.classify(context("مشكور"))["source"], "rules")
            self.assertFalse(reply_ai.available())
            self.assertEqual(reply_ai.classify(context("مشكور"))["source"], "rules")
            self.assertEqual(post.call_count, 1)  # paused: no second call

    def test_network_error_and_bad_answer_fall_back(self):
        with mock.patch.object(reply_ai.requests, "post", side_effect=OSError("down")):
            self.assertEqual(reply_ai.classify(context("hi"))["source"], "rules")
        with mock.patch.object(reply_ai.requests, "post", return_value=self.reply(200, "no json here")):
            self.assertEqual(reply_ai.classify(context("hi"))["source"], "rules")

    def test_setup_error_is_visible_without_the_key(self):
        bad = self.reply(404)
        bad.json.return_value = {"error": {"message": "models/gemini-x is not found for API version v1beta"}}
        bad.url = "https://generativelanguage.googleapis.com/...?key=test-key"
        with mock.patch.object(reply_ai.requests, "post", return_value=bad):
            decision = reply_ai.classify(context("hi"))
        self.assertEqual(decision["source"], "rules")
        self.assertIn("HTTP 404", decision["reason"])
        self.assertIn("not found", decision["reason"])
        self.assertNotIn("test-key", decision["reason"])
        self.assertFalse(reply_ai.available())  # paused, no retry storm
        self.assertIn("HTTP 404", reply_ai.classify(context("hi"))["reason"])
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": ""}):
            self.assertIn("no AI key set", reply_ai.classify(context("hi"))["reason"])

    def test_claude_answers_when_gemini_is_out_of_quota(self):
        client = claude_reply()
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-ant-test"}), \
             mock.patch.object(reply_ai.requests, "post", return_value=self.reply(429)) as post, \
             mock.patch.object(reply_ai, "_claude", return_value=client):
            first = reply_ai.classify(context("وين السحب تبعي"))
            second = reply_ai.classify(context("وين السحب تبعي"))
        self.assertEqual((first["source"], first["priority"]), ("claude", "urgent"))
        self.assertEqual(second["source"], "claude")
        self.assertEqual(post.call_count, 1)  # Gemini paused after its 429; Claude answers directly
        kwargs = client.beta.messages.create.call_args.kwargs
        self.assertEqual(kwargs["model"], "claude-opus-5-5")
        self.assertEqual(kwargs["fallbacks"], "default")
        self.assertEqual(kwargs["output_config"]["format"]["schema"], reply_ai.ANSWER_SCHEMA)

    def test_claude_only_without_gemini_key(self):
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "", "ANTHROPIC_API_KEY": "sk-ant-test"}), \
             mock.patch.object(reply_ai.requests, "post") as post, \
             mock.patch.object(reply_ai, "_claude", return_value=claude_reply()):
            self.assertEqual(reply_ai.classify(context("hi"))["source"], "claude")
        post.assert_not_called()

    def test_other_claude_model_uses_plain_endpoint(self):
        client = claude_reply()
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "", "ANTHROPIC_API_KEY": "sk-ant-test",
                                          "REMINDER_AI_CLAUDE_MODEL": "claude-haiku-5-5"}), \
             mock.patch.object(reply_ai, "_claude", return_value=client):
            self.assertEqual(reply_ai.classify(context("hi"))["source"], "claude")
        self.assertEqual(client.messages.create.call_args.kwargs["model"], "claude-haiku-5-5")
        client.beta.messages.create.assert_not_called()

    def test_claude_refusal_or_error_falls_back_to_rules(self):
        import anthropic
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "", "ANTHROPIC_API_KEY": "sk-ant-test"}), \
             mock.patch.object(reply_ai, "_claude", return_value=claude_reply(stop="refusal")):
            self.assertIn("Claude stopped: refusal", reply_ai.classify(context("hi"))["reason"])
        failing = mock.Mock()
        failing.beta.messages.create.side_effect = anthropic.AuthenticationError(
            "invalid x-api-key", response=mock.Mock(status_code=401, headers={}), body=None)
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "", "ANTHROPIC_API_KEY": "sk-ant-test"}), \
             mock.patch.object(reply_ai, "_claude", return_value=failing):
            decision = reply_ai.classify(context("hi"))
            self.assertEqual(decision["source"], "rules")
            self.assertIn("Claude HTTP 401", decision["reason"])
            self.assertFalse(reply_ai.available())  # paused

    def test_daily_limit(self):
        reply_ai._state.update(day=reply_ai.date.today(), used=reply_ai.DAILY_LIMIT)
        self.assertFalse(reply_ai.available())

    def test_no_key_means_rules(self):
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": ""}):
            self.assertFalse(reply_ai.available())


def row(rid, *, agent="Menna Khaled", since=TUE_0900 - timedelta(days=1), checked=None, inbound="2026-11-16T10:00:00+00:00",
        source=None, needs_reply=None, priority=None, reminded=None, with_context=True):
    r = {"id": rid, "phone": f"9689000000{rid}", "agent_name": agent, "waiting_since": since.isoformat(),
         "last_inbound_at": inbound, "last_inbound_body": "متى يصير السحب؟", "customer_name": f"C{rid}",
         "business_line": "96897245526", "reminder_count": 0, "last_reminded_at": reminded.isoformat() if reminded else None,
         "needs_reply": needs_reply, "priority": priority, "ai_reason": None, "ai_source": source,
         "ai_checked_inbound_at": checked}
    if with_context:
        r["_context"] = context("متى يصير السحب؟")
    return r


class EngineTest(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.patches = [mock.patch.object(reminders, "supabase", self.db),
                        mock.patch.dict(os.environ, {"REMINDER_AI_MODE": "live", "REMINDER_MODE": "live"})]
        for p in self.patches:
            p.start()
        reminders._morning_sent.clear()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        reminders._morning_sent.clear()

    def test_ai_context(self):
        start = TUE_0900
        conv = {"messages": [
            {"direction": "outbound", "body": "Welcome", "sent_at": (start - timedelta(hours=1)).isoformat()},
            {"direction": "inbound", "body": "ok", "sent_at": (start - timedelta(minutes=30)).isoformat()},
            {"direction": "inbound", "body": "how to deposit?", "sent_at": start.isoformat()},
            {"direction": "inbound", "body": "", "sent_at": (start + timedelta(minutes=1)).isoformat()},
        ]}
        self.assertEqual(reminders._ai_context(conv, start),
                         {"customer": ["how to deposit?", "[media]"], "agent_last": "Welcome", "first_contact": False})
        self.assertTrue(reminders._ai_context({"messages": conv["messages"][2:]}, start)["first_contact"])

    def test_classify_respects_the_budget_and_rechecks_rules_later(self):
        rows = [row(i) for i in range(1, 12)]
        rows.append(row(20, checked="2026-11-16T10:00:00+00:00", source="ai", priority="normal", needs_reply=True))
        decision = {"needs_reply": False, "priority": "low", "reason": "thanks", "source": "claude"}
        with mock.patch.object(reminders, "AI_MAX_PER_CYCLE", 8), \
             mock.patch.object(reply_ai, "available", return_value=True), \
             mock.patch.object(reply_ai, "classify", return_value=decision) as classify:
            stats = reminders._classify(rows, TUE_0920)
        self.assertEqual(classify.call_count, 8)
        self.assertEqual(stats, {"gemini": 0, "claude": 8, "rules": 3, "no_reply_needed": 8})
        self.assertEqual(len(self.db.updates), 11)  # the already-checked row is left alone
        self.assertEqual(sum(r["ai_source"] == "rules" for r in rows), 3)
        # Next cycle: the 3 rule-checked rows get the AI.
        with mock.patch.object(reply_ai, "available", return_value=True), \
             mock.patch.object(reply_ai, "classify", return_value=decision) as classify:
            reminders._classify(rows, TUE_0920)
        self.assertEqual(classify.call_count, 3)

    def test_classify_waits_for_the_migration(self):
        rows = [row(1)]
        for key in ("needs_reply", "priority", "ai_reason", "ai_source", "ai_checked_inbound_at"):
            del rows[0][key]
        self.assertEqual(reminders._classify(rows, TUE_0920)["claude"], 0)
        self.assertEqual(self.db.updates, [])

    def test_off_hours_sync_runs_every_15_minutes_only(self):
        night = datetime(2026, 11, 17, 21, 0, tzinfo=UTC)  # Tuesday 23:00 local
        calls = []
        with mock.patch.object(reminders, "_sync", side_effect=lambda now: calls.append(now) or ([], {
                "waiting": 0, "opened": 0, "answered": 0, "expired": 0})), \
             mock.patch.object(reminders, "_last_cycle", {}):
            reminders.run_cycle(night)
            reminders.run_cycle(night + timedelta(minutes=5))   # skipped
            reminders.run_cycle(night + timedelta(minutes=10))  # skipped
            reminders.run_cycle(night + timedelta(minutes=15))
            reminders.run_cycle(TUE_0900 + timedelta(days=1))   # working time: always
            reminders.run_cycle(TUE_0900 + timedelta(days=1, minutes=5))
        self.assertEqual(len(calls), 4)

    def test_shadow_ai_records_but_does_not_change_reminders(self):
        r = row(1, needs_reply=False, priority="urgent")
        with mock.patch.dict(os.environ, {"REMINDER_AI_MODE": "shadow"}):
            self.assertFalse(reminders.no_reply_needed(r))
            self.assertIs(reminders.ladder_for(r), reminders.LADDER)
        self.assertTrue(reminders.no_reply_needed(r))
        self.assertEqual(reminders.ladder_for(r).first_min, 5)

    def test_morning_window(self):
        with mock.patch.object(reminders, "MORNING_LIST_AT", "09:15"):
            self.assertIsNone(reminders._morning_window(TUE_0900))
            day, _, work_start = reminders._morning_window(TUE_0920)
            self.assertEqual((day, work_start), ("2026-11-17", TUE_0900))
            self.assertIsNone(reminders._morning_window(TUE_0900 + timedelta(hours=4)))      # 13:00: too late
            self.assertIsNone(reminders._morning_window(datetime(2026, 11, 21, 7, 20, tzinfo=UTC)))  # Saturday
        with mock.patch.object(reminders, "MORNING_LIST_AT", "off"):
            self.assertIsNone(reminders._morning_window(TUE_0920))

    def test_morning_list_once_per_agent_with_only_older_waits(self):
        rows = [
            row(1),                                                    # yesterday: in the list
            row(2, priority="urgent"),                                 # yesterday, urgent: first in the list
            row(3, since=TUE_0900 + timedelta(minutes=5)),             # today: normal ladder only
            row(4, reminded=TUE_0900 + timedelta(minutes=10)),         # pinged this morning: not again
            row(5, needs_reply=False),                                 # AI: no reply needed
            row(6, agent=None),                                        # unassigned: reports only
            row(7, agent="Bashir"),
        ]
        sent = []
        with mock.patch.object(reminders, "MORNING_LIST_AT", "09:15"), \
             mock.patch.object(reminders, "_agents_with_morning_list", return_value={"Bashir"}), \
             mock.patch.object(reminders, "_still_waiting", side_effect=lambda rs: rs), \
             mock.patch.object(reminders, "_deliver_morning_list", side_effect=lambda a, items, now: sent.append((a, items)) or True):
            self.assertEqual(reminders._morning_lists(rows, TUE_0920), 1)
            self.assertEqual(reminders._morning_lists(rows, TUE_0920 + timedelta(minutes=5)), 0)  # once a day
        self.assertEqual(len(sent), 1)
        agent, items = sent[0]
        self.assertEqual(agent, "Menna Khaled")
        self.assertEqual([i["row"]["id"] for i in items], [2, 1])


class TemplateTest(unittest.TestCase):
    def test_morning_list_caps_cards_and_escapes(self):
        from zoneinfo import ZoneInfo
        items = [{"name": f"<b>C{i}</b>", "phone": "96890000000", "line": "96897245526", "elapsed_min": 700,
                  "since": TUE_0900 - timedelta(days=1), "body": "متى السحب؟", "priority": "urgent" if i == 0 else None}
                 for i in range(20)]
        subject, text, html = templates.morning_list("Menna", items, ZoneInfo("Etc/GMT-2"), "UTC+2", max_cards=15)
        self.assertIn("20", subject)
        self.assertNotIn("<b>C0</b>", html)
        self.assertIn("&lt;b&gt;C0", html)
        self.assertIn("+5", html)
        self.assertIn("عاجل", html)
        self.assertIn("🔥", text)
        self.assertLess(len(text), 4000)


if __name__ == "__main__":
    unittest.main()
