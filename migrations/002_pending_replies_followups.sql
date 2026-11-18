-- Supervisor follow-ups for customers who stay unanswered after escalation
-- (default: 1 day and 3 days after the first escalation).
-- Run once in Supabase → SQL Editor BEFORE deploying the code that uses it. Safe to re-run.

ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS escalation_count  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pending_replies ADD COLUMN IF NOT EXISTS last_escalated_at TIMESTAMPTZ;

-- Rows escalated before this migration count as "first escalation sent".
UPDATE pending_replies
   SET escalation_count = 1, last_escalated_at = escalated_at
 WHERE escalated_at IS NOT NULL AND escalation_count = 0;
