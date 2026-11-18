"""Helpers for dashboard periods that lie completely in the past."""
from __future__ import annotations

import os

from dotenv import load_dotenv
from supabase import create_client

from pipeline.swr_cache import SWRCache
from pipeline.time_ranges import is_closed, since_iso, until_iso

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
_ACTIVE = SWRCache("active-leads", 3600)


def _active_lead_ids(range_: str) -> set[str]:
    since, until = since_iso(range_), until_iso(range_)
    ids: set[str] = set()
    offset = 0
    while True:
        rows = (
            supabase.table("messages").select("lead_id")
            .gte("sent_at", since).lt("sent_at", until)
            .range(offset, offset + 999).execute().data or []
        )
        ids.update(r["lead_id"] for r in rows if r.get("lead_id"))
        if len(rows) < 1000:
            return ids
        offset += 1000


def only_active(rows: list[dict], range_: str) -> list[dict]:
    """leads.last_message_at only stores the LATEST activity. For a past period
    ("yesterday", a custom range) keep the leads that had messages in that period."""
    if not is_closed(range_):
        return rows
    ids = _ACTIVE.get(range_, lambda: _active_lead_ids(range_))
    return [row for row in rows if row.get("id") in ids]
