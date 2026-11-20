"""Background syncs must not rewrite unchanged rows (Supabase disk IO budget).
Run: python -m unittest discover -s tests -v (offline)"""
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "sb_secret_offline_test")

import main  # noqa: E402

CALL = {"maqsam_id": "1", "lead_id": "L1", "agent_name": "Menna Khaled", "duration_seconds": 60, "outcome": "completed",
        "called_at": "2026-11-20T07:00:00+00:00", "transcript": "t", "summary_en": "s", "summary_ar": None,
        "maqsam_sentiment": "neutral", "auto_tags": ["deposit"]}


class CallRowTest(unittest.TestCase):
    def test_identical_call_is_skipped_even_with_another_time_format(self):
        stored = {**CALL, "called_at": "2026-11-20T07:00:00Z"}
        self.assertFalse(main._call_row_changed(CALL, stored))

    def test_new_or_changed_calls_are_written(self):
        self.assertTrue(main._call_row_changed(CALL, None))
        self.assertTrue(main._call_row_changed({**CALL, "transcript": "longer"}, CALL))
        self.assertTrue(main._call_row_changed({**CALL, "lead_id": "L2"}, CALL))


def fake_leads_db(stored_rows):
    db = mock.Mock()
    db.table.return_value.select.return_value.in_.return_value.execute.return_value.data = stored_rows
    return db


class ChangedLeadsTest(unittest.TestCase):
    row = {"wa_contact_id": "mc1", "phone": "212600000000", "name": "Rachid", "channel": "whatsapp", "status": "engaged",
           "last_message_at": "2026-11-13T11:52:56.000Z", "updated_at": "2026-11-20T07:10:00+00:00",
           "assigned_agent": "Zahrae", "whatsapp_business_number": "905318880855"}

    def changed(self, stored):
        with mock.patch.object(main, "supabase", fake_leads_db(stored)):
            return main._changed_leads([self.row])

    def test_unchanged_contact_is_not_rewritten(self):
        stored = {**self.row, "last_message_at": "2026-11-13T11:52:56+00:00", "updated_at": "2026-11-19T00:00:00+00:00"}
        self.assertEqual(self.changed([stored]), [])

    def test_changed_or_new_contact_is_written(self):
        self.assertEqual(self.changed([{**self.row, "assigned_agent": "Bashir"}]), [self.row])
        self.assertEqual(self.changed([]), [self.row])

    def test_database_error_falls_back_to_writing_everything(self):
        db = mock.Mock()
        db.table.side_effect = RuntimeError("timeout")
        with mock.patch.object(main, "supabase", db):
            self.assertEqual(main._changed_leads([self.row]), [self.row])


class LineTagTest(unittest.TestCase):
    def test_only_untagged_rows_are_updated(self):
        db = mock.MagicMock()
        db.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value.data = [{"id": "L1"}]
        with mock.patch.object(main, "supabase", db):
            main._assign_whatsapp_business_line("L1", "212600000000", "905318880855")
        filters = [c.args[0] for c in db.mock_calls if c[0].endswith(".or_")]
        self.assertTrue(filters)
        self.assertTrue(all(f == "whatsapp_business_number.is.null,whatsapp_business_number.neq.905318880855" for f in filters))
        self.assertEqual(len(filters), 3)   # lead, phone leads, phone messages (the 4th update was already null-only)


if __name__ == "__main__":
    unittest.main()
