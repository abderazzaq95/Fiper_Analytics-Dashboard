-- Agent voice notes, images and files: ManyContacts forwards an agent's TEXT
-- replies to us, but not media. WhatsApp itself still reports every message the
-- business sends ("sent" status, with the customer's number and the time).
-- One row per sent message, so a voice-note reply also counts as an answer for
-- reminders, the waiting list and reply times.
-- Run once in Supabase → SQL Editor. Safe to re-run.

CREATE TABLE IF NOT EXISTS whatsapp_sends (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  wa_message_id  TEXT NOT NULL UNIQUE,     -- WhatsApp message id (wamid…)
  phone          TEXT NOT NULL,            -- customer phone, digits only
  sent_at        TIMESTAMPTZ NOT NULL,     -- when WhatsApp sent it
  business_line  TEXT,                     -- our WhatsApp number, if known
  category       TEXT,                     -- WhatsApp pricing category: service | marketing | utility | …
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_whatsapp_sends_phone_sent ON whatsapp_sends(phone, sent_at);
CREATE INDEX IF NOT EXISTS idx_whatsapp_sends_sent_at    ON whatsapp_sends(sent_at);

-- Only the backend (service role) may read/write; it bypasses RLS.
ALTER TABLE whatsapp_sends ENABLE ROW LEVEL SECURITY;
