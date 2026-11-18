"""
One-time data fix: set leads.created_at to the date of the lead's first call.

Background: all Maqsam leads were backfilled with created_at=NOW() (insertion time).
This makes date filters useless — everything looks like it happened today.
This script sets each maqsam lead's created_at to min(called_at) from its calls.

Usage:  python fix_leads_created_at.py
"""
import asyncio
import json as _json
import logging
import os
import sys
from collections import defaultdict

import httpx
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fix_leads")

SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
SB_H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}


async def paginate(c, path, params):
    rows, offset = [], 0
    while True:
        r = await c.get(f"{SB}/rest/v1/{path}", headers=SB_H,
                        params={**params, "limit": 1000, "offset": offset})
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


async def main():
    async with httpx.AsyncClient(timeout=60) as c:
        log.info("Fetching all calls with lead_id and called_at...")
        calls = await paginate(c, "calls",
                               {"select": "lead_id,called_at",
                                "lead_id": "not.is.null",
                                "called_at": "not.is.null"})
        log.info(f"  {len(calls)} calls fetched")

        # Build lead_id → min(called_at)
        min_called: dict[str, str] = {}
        for call in calls:
            lid = call["lead_id"]
            ca = call["called_at"]
            if ca and (lid not in min_called or ca < min_called[lid]):
                min_called[lid] = ca

        log.info(f"  {len(min_called)} unique leads have calls")

        # Group lead_ids by first-call DATE (YYYY-MM-DD) for batch updates
        by_date: dict[str, list[str]] = defaultdict(list)
        for lid, ca in min_called.items():
            date_str = ca[:10]  # YYYY-MM-DD
            by_date[date_str].append(lid)

        log.info(f"  {len(by_date)} distinct first-call dates")
        total_updated = 0

        # Batch PATCH: all leads with the same first-call date get the same created_at
        for date_str, lids in sorted(by_date.items()):
            created_at_val = date_str + "T00:00:00+00:00"
            # Process in chunks of 100 (URL length limit for IN clause)
            for i in range(0, len(lids), 100):
                chunk = lids[i:i + 100]
                ids_csv = ",".join(chunk)
                r = await c.patch(
                    f"{SB}/rest/v1/leads",
                    headers={**SB_H, "Prefer": "return=minimal"},
                    params={"id": f"in.({ids_csv})"},
                    content=_json.dumps({"created_at": created_at_val}),
                )
                if r.status_code not in (200, 204):
                    log.error(f"  PATCH failed {r.status_code}: {r.text[:200]}")
                else:
                    total_updated += len(chunk)

            log.info(f"  {date_str}: updated {len(lids)} leads  (total: {total_updated})")

        log.info(f"Done. {total_updated} leads updated.")

        # Show distribution after fix
        log.info("\n--- Verifying date distribution ---")
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        for label, days in [("today (midnight)", 0), ("7d", 7), ("30d", 30), ("90d", 90)]:
            if days == 0:
                since = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            else:
                since = (now - timedelta(days=days)).isoformat()
            r = await c.get(f"{SB}/rest/v1/leads",
                            headers={**SB_H, "Prefer": "count=exact"},
                            params={"created_at": f"gte.{since}", "select": "id", "limit": 1})
            log.info(f"  leads {label}: {r.headers.get('content-range', '?')}")


if __name__ == "__main__":
    asyncio.run(main())
