"""
Maqsam backfill: fetch all calls for the last 90 days with unlimited pagination.
Processes and upserts incrementally per page — no large memory buffer.

Usage:  python backfill_maqsam.py
"""
import asyncio
import base64
import json as _json
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("maqsam_backfill")

SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
SB_H = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

MQ_KEY = os.getenv("MAQSAM_ACCESS_KEY")
MQ_SEC = os.getenv("MAQSAM_ACCESS_SECRET")
tok = base64.b64encode(f"{MQ_KEY}:{MQ_SEC}".encode()).decode()
MQ_H = {"Authorization": f"Basic {tok}", "Content-Type": "application/json"}


def _extract(data):
    if isinstance(data, list):
        return data
    return data.get("message", data.get("data", []))


def _agent(call):
    agents = call.get("agents", [])
    if agents and isinstance(agents, list):
        return agents[0].get("name")
    return None


def _phone(call):
    direction = call.get("direction", "outbound")
    return call.get("calleeNumber") if direction == "outbound" else call.get("callerNumber")


async def main():
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    date_from = (now - timedelta(days=90)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    log.info(f"Fetching Maqsam calls {date_from} → {date_to} (incremental, unlimited pages)")

    total_calls = 0
    async with httpx.AsyncClient(timeout=60) as c:
        page = 1
        while True:
            r = await c.get("https://api.maqsam.com/v2/calls", headers=MQ_H,
                            params={"date_from": date_from, "date_to": date_to, "page": page})
            r.raise_for_status()
            batch = _extract(r.json())
            if not batch:
                break

            total_calls += len(batch)
            log.info(f"  page {page:4d}: {len(batch)} calls  (cumulative: {total_calls})")

            # ── Upsert leads for phones in this batch ───────────────────────
            phones = {p for call in batch if (p := _phone(call))}
            if phones:
                lead_rows = [
                    {"wa_contact_id": p, "phone": p, "channel": "maqsam", "updated_at": now_iso}
                    for p in phones
                ]
                rl = await c.post(f"{SB}/rest/v1/leads?on_conflict=wa_contact_id",
                                  headers=SB_H, content=_json.dumps(lead_rows))
                rl.raise_for_status()

                # ── Fetch lead IDs for the phones we just upserted ──────────
                phones_csv = ",".join(phones)
                rm = await c.get(f"{SB}/rest/v1/leads",
                                 headers={**SB_H, "Prefer": ""},
                                 params={"wa_contact_id": f"in.({phones_csv})",
                                         "select": "id,wa_contact_id", "limit": len(phones)})
                phone_to_lead = {row["wa_contact_id"]: row["id"] for row in rm.json()}
            else:
                phone_to_lead = {}

            # ── Upsert calls for this batch ──────────────────────────────────
            call_rows = []
            for call in batch:
                phone = _phone(call)
                ts = call.get("timestamp")
                called_at = (datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
                             if ts else None)
                call_rows.append({
                    "maqsam_id": str(call.get("id")),
                    "lead_id": phone_to_lead.get(phone or ""),
                    "agent_name": _agent(call),
                    "duration_seconds": call.get("duration"),
                    "outcome": call.get("state"),
                    "recording_url": call.get("recording_url"),
                    "called_at": called_at,
                })

            rc = await c.post(f"{SB}/rest/v1/calls?on_conflict=maqsam_id",
                              headers=SB_H, content=_json.dumps(call_rows))
            rc.raise_for_status()

            if len(batch) < 100:
                break
            page += 1

        log.info(f"Backfill complete — {total_calls} calls processed across {page} pages")

        # ── Final DB count ────────────────────────────────────────────────────
        r = await c.get(f"{SB}/rest/v1/calls",
                        headers={**SB_H, "Prefer": "count=exact"},
                        params={"select": "id", "limit": 1})
        log.info(f"DB calls count after backfill: {r.headers.get('content-range', '?')}")


if __name__ == "__main__":
    asyncio.run(main())
