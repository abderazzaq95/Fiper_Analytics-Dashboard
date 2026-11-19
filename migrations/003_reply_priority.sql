-- Smarter reminders: AI decision per waiting customer (does it need a reply, how urgent).
-- Run once in Supabase → SQL Editor BEFORE deploying the code that uses it. Safe to re-run.

ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS needs_reply           BOOLEAN;
ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS priority              TEXT;         -- urgent | normal | low
ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS ai_reason             TEXT;
ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS ai_source             TEXT;         -- ai | rules
ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS ai_checked_inbound_at TIMESTAMPTZ;  -- last customer message that was checked
