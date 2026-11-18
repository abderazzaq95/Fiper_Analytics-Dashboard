-- Reply reminders: one open row per customer phone that is waiting for an agent.
-- Run once in Supabase → SQL Editor. Safe to re-run.

CREATE TABLE IF NOT EXISTS pending_replies (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  phone              TEXT NOT NULL,          -- customer phone, digits only
  lead_id            UUID REFERENCES leads(id) ON DELETE SET NULL,
  customer_name      TEXT,
  agent_name         TEXT,                   -- current owner in ManyContacts (NULL = unassigned)
  business_line      TEXT,                   -- ManyContacts line the customer wrote to
  waiting_since      TIMESTAMPTZ NOT NULL,   -- first unanswered customer message
  last_inbound_at    TIMESTAMPTZ,
  last_inbound_body  TEXT,
  status             TEXT NOT NULL DEFAULT 'open',  -- open | answered | expired | dismissed
  resolved_by        TEXT,                   -- agent_contact | no_longer_waiting | too_old | manual
  resolved_at        TIMESTAMPTZ,
  reminder_count     INTEGER NOT NULL DEFAULT 0,
  last_reminded_at   TIMESTAMPTZ,
  escalated_at       TIMESTAMPTZ,
  snoozed_until      TIMESTAMPTZ,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- At most one open wait per customer.
CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_replies_open_phone
  ON pending_replies(phone) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_pending_replies_status ON pending_replies(status);
CREATE INDEX IF NOT EXISTS idx_pending_replies_agent  ON pending_replies(agent_name, waiting_since DESC);

-- Only the backend (service role) may read/write; it bypasses RLS.
ALTER TABLE pending_replies ENABLE ROW LEVEL SECURITY;

-- Speeds up the per-cycle phone lookups and pre-send re-checks.
CREATE INDEX IF NOT EXISTS idx_messages_lead_sent ON messages(lead_id, sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_phone        ON leads(phone);
