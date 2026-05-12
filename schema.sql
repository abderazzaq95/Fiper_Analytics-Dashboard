-- Fiper Intelligence — full database schema
-- Run this in Supabase Dashboard → SQL Editor → New Query → Run All

-- ============================================================
-- leads
-- ============================================================
CREATE TABLE IF NOT EXISTS leads (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  wa_contact_id    TEXT UNIQUE,
  phone            TEXT,
  name             TEXT,
  channel          TEXT,           -- 'whatsapp' | 'maqsam'
  status           TEXT DEFAULT 'new',  -- 'new'|'engaged'|'interested'|'converted'|'lost'
  score            INTEGER,        -- AI-generated 0-100
  assigned_agent   TEXT,
  last_message_at  TIMESTAMPTZ,
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- messages
-- ============================================================
CREATE TABLE IF NOT EXISTS messages (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  wa_message_id  TEXT UNIQUE,
  lead_id        UUID REFERENCES leads(id) ON DELETE CASCADE,
  direction      TEXT,            -- 'inbound' | 'outbound'
  body           TEXT,
  agent_name     TEXT,
  status         TEXT,            -- 'sent'|'delivered'|'read'
  sent_at        TIMESTAMPTZ
);

-- ============================================================
-- calls
-- ============================================================
CREATE TABLE IF NOT EXISTS calls (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  maqsam_id        TEXT UNIQUE,
  lead_id          UUID REFERENCES leads(id) ON DELETE SET NULL,
  agent_name       TEXT,
  duration_seconds INTEGER,
  outcome          TEXT,
  recording_url    TEXT,
  called_at        TIMESTAMPTZ
);

-- ============================================================
-- ai_analysis
-- ============================================================
CREATE TABLE IF NOT EXISTS ai_analysis (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id          UUID REFERENCES leads(id) ON DELETE CASCADE,
  source           TEXT,           -- 'whatsapp' | 'call'
  sentiment        TEXT,           -- 'positive'|'neutral'|'negative'
  score            INTEGER,
  topics           TEXT[],
  outcome          TEXT,
  follow_up_needed BOOLEAN,
  risk_flags       TEXT[],
  treatment_score  INTEGER,        -- 0-100, how well lead was treated
  summary          TEXT,
  analyzed_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- alerts
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id     UUID REFERENCES leads(id) ON DELETE SET NULL,
  agent_name  TEXT,
  severity    TEXT,               -- 'HIGH'|'MED'|'LOW'
  type        TEXT,
  message     TEXT,
  resolved    BOOLEAN DEFAULT FALSE,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- Indexes for common query patterns
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_leads_status        ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_channel       ON leads(channel);
CREATE INDEX IF NOT EXISTS idx_leads_assigned      ON leads(assigned_agent);
CREATE INDEX IF NOT EXISTS idx_leads_updated       ON leads(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_lead       ON messages(lead_id);
CREATE INDEX IF NOT EXISTS idx_messages_sent       ON messages(sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_direction  ON messages(direction);
CREATE INDEX IF NOT EXISTS idx_calls_lead          ON calls(lead_id);
CREATE INDEX IF NOT EXISTS idx_calls_agent         ON calls(agent_name);
CREATE INDEX IF NOT EXISTS idx_calls_called        ON calls(called_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_lead             ON ai_analysis(lead_id);
CREATE INDEX IF NOT EXISTS idx_ai_analyzed         ON ai_analysis(analyzed_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved     ON alerts(resolved);
CREATE INDEX IF NOT EXISTS idx_alerts_severity     ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_lead         ON alerts(lead_id);
