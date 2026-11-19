-- Less disk reading (Supabase "Disk IO budget"): indexes on the columns the
-- dashboard, reports and reminders filter by. Without them Postgres reads
-- whole tables from disk for every "messages of today" / "calls of this week".
--
-- Creates an index ONLY where none exists yet on that column (no duplicates).
-- Safe to re-run. Tables are small (tens of thousands of rows): seconds.
-- Run in Supabase → SQL Editor; the "Messages" tab lists what was created.

DO $$
DECLARE
  item record;
BEGIN
  FOR item IN
    SELECT * FROM (VALUES
      ('messages',                'sent_at'),
      ('messages',                'lead_id'),
      ('calls',                   'called_at'),
      ('calls',                   'lead_id'),
      ('alerts',                  'created_at'),
      ('alerts',                  'lead_id'),
      ('ai_analysis',             'analyzed_at'),
      ('ai_analysis',             'lead_id'),
      ('leads',                   'last_message_at'),
      ('leads',                   'phone'),
      ('leads',                   'created_at'),
      ('pending_replies',         'status'),
      ('pending_replies',         'updated_at'),
      ('notification_deliveries', 'sent_at')
    ) AS wanted(tbl, col)
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = item.tbl AND column_name = item.col)
       AND NOT EXISTS (SELECT 1 FROM pg_indexes
                       WHERE schemaname = 'public' AND tablename = item.tbl
                         AND indexdef ~ ('\(' || item.col || '[ ,)]'))   -- an index starting with this column
    THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON public.%I (%I)', 'idx_' || item.tbl || '_' || item.col, item.tbl, item.col);
      RAISE NOTICE 'created index on %.%', item.tbl, item.col;
    ELSE
      RAISE NOTICE 'ok: %.% (already indexed or no such column)', item.tbl, item.col;
    END IF;
  END LOOP;
END $$;
