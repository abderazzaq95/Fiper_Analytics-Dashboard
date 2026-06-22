"""
Backfill whatsapp_business_number on leads + messages using the ManyContacts
contact LIST from each account.

Strategy: each account owns exactly one WhatsApp number. Fetching the contact
list (IDs only) from Account A tells us every wa_contact_id that belongs to
line A — no per-contact detail call needed.

Requires in .env (or exported env vars):
  MC_API_KEY          — existing key (Oman, +96897245526)
  MC_API_KEY_TURKEY   — Turkey account key (+905318880855)

Usage:
  python backfill_wa_line_v2.py            # dry-run (counts only)
  python backfill_wa_line_v2.py --apply    # write to DB
"""

import argparse
import asyncio
import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_v2")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
MC_BASE_URL = os.getenv("MC_BASE_URL", "https://api.manycontacts.com/v1")

ACCOUNTS = [
    {
        "api_key": os.getenv("MC_API_KEY", ""),
        "line":    "96897245526",
        "label":   "Oman (+96897245526)",
    },
    {
        "api_key": os.getenv("MC_API_KEY_TURKEY", ""),
        "line":    "905318880855",
        "label":   "Turkey (+905318880855)",
    },
]

SB_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


async def fetch_all_contact_ids(api_key: str) -> set[str]:
    """Page through /v1/contacts for the given account; return all contact IDs."""
    headers = {"apikey": api_key}
    ids: set[str] = set()
    page: int | None = None

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            params: dict = {}
            if page is not None:
                params["page"] = page
            try:
                resp = await client.get(f"{MC_BASE_URL}/contacts", headers=headers, params=params)
                if resp.status_code == 429:
                    log.info(f"  rate limited at page={page}, waiting 5s ...")
                    await asyncio.sleep(5)
                    resp = await client.get(f"{MC_BASE_URL}/contacts", headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"  fetch_contacts page={page}: {e}")
                break

            if not isinstance(data, list) or not data:
                break

            for c in data:
                if isinstance(c, dict) and c.get("id"):
                    ids.add(str(c["id"]))

            if len(data) < 50:
                break
            page = 1 if page is None else page + 1
            # Gentle rate limit: pause every 20 pages
            if page and page % 20 == 0:
                await asyncio.sleep(1)

    return ids


async def fetch_leads_missing_line(client: httpx.AsyncClient) -> list[dict]:
    """Return all WA leads where whatsapp_business_number IS NULL."""
    rows: list[dict] = []
    offset = 0
    while True:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/leads",
            headers=SB_HEADERS,
            params={
                "select": "id,wa_contact_id,phone",
                "channel": "eq.whatsapp",
                "whatsapp_business_number": "is.null",
                "limit": 1000,
                "offset": offset,
            },
        )
        resp.raise_for_status()
        batch = resp.json()
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


async def update_lead(client: httpx.AsyncClient, lead_id: str, line: str) -> bool:
    try:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/leads",
            headers=SB_HEADERS,
            params={"id": f"eq.{lead_id}"},
            json={"whatsapp_business_number": line},
        )
        return r.status_code in (200, 204)
    except Exception as e:
        log.error(f"  update lead {lead_id}: {e}")
        return False


async def update_messages(client: httpx.AsyncClient, lead_id: str, line: str) -> bool:
    try:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/messages",
            headers=SB_HEADERS,
            params={"lead_id": f"eq.{lead_id}", "whatsapp_business_number": "is.null"},
            json={"whatsapp_business_number": line},
        )
        return r.status_code in (200, 204)
    except Exception as e:
        log.error(f"  update messages for {lead_id}: {e}")
        return False


async def run(apply: bool) -> None:
    log.info(f"=== WA line backfill v2 ({'APPLY' if apply else 'DRY RUN'}) ===")

    async with httpx.AsyncClient(timeout=30) as client:
        # ── 1. Fetch all null-line leads ──────────────────────────────────────
        log.info("Fetching WA leads with null whatsapp_business_number ...")
        null_leads = await fetch_leads_missing_line(client)
        log.info(f"  Found {len(null_leads)} leads to attribute")

        if not null_leads:
            log.info("Nothing to do.")
            return

        # Build lookup: wa_contact_id → lead_id
        contact_id_map: dict[str, str] = {
            l["wa_contact_id"]: l["id"]
            for l in null_leads
            if l.get("wa_contact_id")
        }
        log.info(f"  {len(contact_id_map)} have a wa_contact_id for matching")

        # ── 2. Process each account ───────────────────────────────────────────
        total_attributed = 0
        total_unmatched = len(null_leads)

        for account in ACCOUNTS:
            if not account["api_key"]:
                log.warning(f"  Skipping {account['label']} — no API key configured")
                continue

            log.info(f"\nFetching contacts from {account['label']} ...")
            mc_ids = await fetch_all_contact_ids(account["api_key"])
            log.info(f"  {len(mc_ids)} contacts found in this account")

            matched_lead_ids = [
                contact_id_map[cid]
                for cid in mc_ids
                if cid in contact_id_map
            ]
            log.info(f"  {len(matched_lead_ids)} leads matched → will be attributed to {account['line']}")

            if not matched_lead_ids:
                continue

            if not apply:
                total_attributed += len(matched_lead_ids)
                total_unmatched -= len(matched_lead_ids)
                continue

            ok = 0
            for i, lead_id in enumerate(matched_lead_ids):
                l_ok = await update_lead(client, lead_id, account["line"])
                m_ok = await update_messages(client, lead_id, account["line"])
                if l_ok:
                    ok += 1
                if (i + 1) % 100 == 0:
                    log.info(f"    ... {i+1}/{len(matched_lead_ids)} done")
                # Gentle rate limit
                if (i + 1) % 50 == 0:
                    await asyncio.sleep(0.3)

            log.info(f"  Updated {ok}/{len(matched_lead_ids)} leads for {account['label']}")
            total_attributed += ok
            total_unmatched -= ok

        # ── 3. Report ─────────────────────────────────────────────────────────
        log.info(f"\n{'=' * 55}")
        if apply:
            log.info("APPLY COMPLETE")
        else:
            log.info("DRY RUN COMPLETE — re-run with --apply to write")
        log.info(f"  Total null leads:    {len(null_leads)}")
        log.info(f"  Attributed:          {total_attributed}")
        log.info(f"  Still unmatched:     {total_unmatched}  (no wa_contact_id or not in either account)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write updates to DB (default: dry run)")
    args = parser.parse_args()
    asyncio.run(run(apply=args.apply))
