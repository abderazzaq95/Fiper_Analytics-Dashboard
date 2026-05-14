"""
One-time Maqsam backfill: fetch ALL calls for the last 90 days with
unlimited pagination, upsert into Supabase calls table.

Usage:  python backfill_maqsam.py
"""
import asyncio
import base64
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

import json as _json


def _extract(data):
    if isinstance(data, list):
        return data
    return data.get("message", data.get("data", []))


def _agent(call):
    agents = call.get("agents", [])
    if agents and isinstance(agents, list):
        return agents[0].get("name")
    return None


async def main():
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=90)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    log.info(f"Fetching Maqsam calls {date_from} → {date_to} (unlimited pages)")

    async with httpx.AsyncClient(timeout=60) as c:
        # ── Phase 1: paginate all calls from Maqsam ──────────────────────────
        all_calls = []
        page = 1
        while True:
            r = await c.get("https://api.maqsam.com/v2/calls", headers=MQ_H,
                            params={"date_from": date_from, "date_to": date_to, "page": page})
            r.raise_for_status()
            batch = _extract(r.json())
            log.info(f"  page {page:3d}: {len(batch)} calls  (total so far: {len(all_calls) + len(batch)})")
            all_calls.extend(batch)
            if len(batch) < 100:
                break
            page += 1

        log.info(f"Maqsam fetch complete — {len(all_calls)} total calls")

        # ── Phase 2: collect unique phone numbers, upsert leads ───────────────
        now_iso = now.isoformat()
        unique_phones: dict[str, None] = {}
        for call in all_calls:
            direction = call.get("direction", "outbound")
            phone = call.get("calleeNumber") if direction == "outbound" else call.get("callerNumber")
            if phone:
                unique_phones[phone] = None

        log.info(f"Upserting {len(unique_phones)} unique phone leads...")
        lead_rows = [
            {"wa_contact_id": p, "phone": p, "channel": "maqsam", "updated_at": now_iso}
            for p in unique_phones
        ]
        chunk = 200
        for i in range(0, len(lead_rows), chunk):
            r = await c.post(f"{SB}/rest/v1/leads?on_conflict=wa_contact_id",
                             headers=SB_H, content=_json.dumps(lead_rows[i:i+chunk]))
            r.raise_for_status()

        # ── Phase 3: build phone → lead_id map ───────────────────────────────
        phone_to_lead: dict[str, str] = {}
        for i in range(0, len(unique_phones), 500):
            batch_phones = list(unique_phones.keys())[i:i+500]
            phones_csv = ",".join(batch_phones)
            r = await c.get(f"{SB}/rest/v1/leads", headers={**SB_H, "Prefer": ""},
                            params={"wa_contact_id": f"in.({phones_csv})",
                                    "select": "id,wa_contact_id", "limit": 500})
            for row in r.json():
                phone_to_lead[row["wa_contact_id"]] = row["id"]

        log.info(f"Lead map built: {len(phone_to_lead)} entries")

        # ── Phase 4: upsert call rows ─────────────────────────────────────────
        call_rows = []
        for call in all_calls:
            direction = call.get("direction", "outbound")
            phone = call.get("calleeNumber") if direction == "outbound" else call.get("callerNumber")
            ts = call.get("timestamp")
            called_at = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat() if ts else None
            call_rows.append({
                "maqsam_id": str(call.get("id")),
                "lead_id": phone_to_lead.get(phone or ""),
                "agent_name": _agent(call),
                "duration_seconds": call.get("duration"),
                "outcome": call.get("state"),
                "recording_url": call.get("recording_url"),
                "called_at": called_at,
            })

        log.info(f"Upserting {len(call_rows)} call rows in chunks of 200...")
        for i in range(0, len(call_rows), 200):
            r = await c.post(f"{SB}/rest/v1/calls?on_conflict=maqsam_id",
                             headers=SB_H, content=_json.dumps(call_rows[i:i+200]))
            r.raise_for_status()
            log.info(f"  upserted calls {i}–{min(i+200, len(call_rows))}")

        # ── Phase 5: final count ──────────────────────────────────────────────
        r = await c.get(f"{SB}/rest/v1/calls", headers={**SB_H, "Prefer": "count=exact"},
                        params={"select": "id", "limit": 1})
        log.info(f"DB calls count after backfill: {r.headers.get('content-range', '?')}")


if __name__ == "__main__":
    asyncio.run(main())
