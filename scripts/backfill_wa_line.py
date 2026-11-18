"""
WhatsApp business-line backfill probe.

Phase 1 (always runs): samples up to PROBE_LIMIT ManyContacts contacts
  and reports which fields contain the business phone number, how many
  contacts have it, and how many DB rows can be recovered.

Phase 2 (only with --apply): updates leads and messages rows that have
  whatsapp_business_number IS NULL, using the value fetched from the
  ManyContacts /v1/contact/{id} endpoint.

Usage:
  python backfill_wa_line.py              # dry-run probe only
  python backfill_wa_line.py --apply      # apply updates to DB
  python backfill_wa_line.py --probe-only --probe-limit 20  # quick sample
"""

import argparse
import asyncio
import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_wa_line")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
MC_BASE_URL = os.getenv("MC_BASE_URL", "https://api.manycontacts.com/v1")
MC_API_KEY = os.getenv("MC_API_KEY", "")

SB_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}
MC_HEADERS = {"apikey": MC_API_KEY}

# Known business phone numbers (digits only)
BUSINESS_NUMBERS = {"96897245526", "905318880855"}


def _normalize(phone: str | None) -> str:
    return "".join(c for c in str(phone or "") if c.isdigit())


def _extract_business_line(contact: dict) -> tuple[str | None, str | None]:
    """
    Try to find the business phone number in a ManyContacts contact dict.
    Returns (normalized_number, key_that_matched) or (None, None).
    """
    # Walk the same structure _extract_whatsapp_business_number uses
    def _first_dict(*args):
        for a in args:
            if isinstance(a, dict):
                return a
        return {}

    root = _first_dict(contact.get("data"), contact.get("payload"), contact)
    delta = _first_dict(root.get("delta"), root.get("eventData"), root.get("data"))
    value = _first_dict(root.get("value"), delta.get("value"))
    contact_sub = _first_dict(root.get("contact"), delta.get("contact"), root.get("chat"))
    metadata = _first_dict(
        root.get("metadata"), delta.get("metadata"),
        value.get("metadata"), contact_sub.get("metadata"),
    )

    CANDIDATE_KEYS = (
        "display_phone_number", "phone_number", "business_number",
        "business_phone_number", "phone_number_id", "number",
        "line_number", "recipient_number", "to", "recipient",
        # Extra ManyContacts-specific keys to probe
        "whatsappPhoneNumber", "businessPhone", "inbox_phone",
        "wabaPhoneNumber", "wa_phone", "channel_phone", "assignedPhone",
    )

    for obj_name, obj in [
        ("root", root), ("delta", delta), ("value", value),
        ("metadata", metadata), ("contact_sub", contact_sub),
        ("top", contact),
    ]:
        if not isinstance(obj, dict):
            continue
        for key in CANDIDATE_KEYS:
            candidate = obj.get(key)
            if not candidate:
                continue
            normalized = _normalize(str(candidate))
            if normalized in BUSINESS_NUMBERS:
                return normalized, f"{obj_name}.{key}"

    return None, None


def _all_keys(contact: dict) -> list[str]:
    """Collect all keys from the top-level and one level deep."""
    keys = set(contact.keys())
    for v in contact.values():
        if isinstance(v, dict):
            keys.update(v.keys())
    return sorted(keys)


async def fetch_leads_missing_line(client: httpx.AsyncClient, limit: int = 10_000) -> list[dict]:
    """Fetch WA leads where whatsapp_business_number IS NULL."""
    rows = []
    offset = 0
    while True:
        resp = await client.get(
            f"{SUPABASE_URL}/rest/v1/leads",
            headers=SB_HEADERS,
            params={
                "select": "id,wa_contact_id,phone,whatsapp_business_number",
                "channel": "eq.whatsapp",
                "whatsapp_business_number": "is.null",
                "limit": min(1000, limit - len(rows)),
                "offset": offset,
            },
        )
        resp.raise_for_status()
        batch = resp.json()
        rows.extend(batch)
        if len(batch) < 1000 or len(rows) >= limit:
            break
        offset += 1000
    return rows


async def fetch_contact(client: httpx.AsyncClient, mc_id: str) -> dict | None:
    """Fetch a single ManyContacts contact."""
    try:
        resp = await client.get(
            f"{MC_BASE_URL}/contact/{mc_id}",
            headers=MC_HEADERS,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
        log.debug(f"[mc] /contact/{mc_id} → {resp.status_code}")
        return None
    except Exception as e:
        log.debug(f"[mc] /contact/{mc_id} error: {e}")
        return None


async def update_lead_line(client: httpx.AsyncClient, lead_id: str, line: str) -> bool:
    try:
        resp = await client.patch(
            f"{SUPABASE_URL}/rest/v1/leads",
            headers={**SB_HEADERS, "Prefer": "return=minimal"},
            params={"id": f"eq.{lead_id}"},
            content=json.dumps({"whatsapp_business_number": line}),
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        log.error(f"update lead {lead_id}: {e}")
        return False


async def update_messages_line(client: httpx.AsyncClient, lead_id: str, line: str) -> int:
    """Update all messages for this lead that have no business line set."""
    try:
        resp = await client.patch(
            f"{SUPABASE_URL}/rest/v1/messages",
            headers={**SB_HEADERS, "Prefer": "return=minimal"},
            params={"lead_id": f"eq.{lead_id}", "whatsapp_business_number": "is.null"},
            content=json.dumps({"whatsapp_business_number": line}),
        )
        return 1 if resp.status_code in (200, 204) else 0
    except Exception as e:
        log.error(f"update messages for lead {lead_id}: {e}")
        return 0


async def run(apply: bool, probe_limit: int):
    log.info(f"=== WhatsApp line backfill {'(APPLY)' if apply else '(DRY RUN)'} ===")
    log.info(f"Known business numbers: {BUSINESS_NUMBERS}")

    async with httpx.AsyncClient(timeout=30) as client:
        # ── Phase 1: fetch all null-line WA leads ──────────────────────────────
        log.info("Fetching WA leads with whatsapp_business_number IS NULL ...")
        null_leads = await fetch_leads_missing_line(client)
        log.info(f"Found {len(null_leads)} leads with null business line")

        if not null_leads:
            log.info("Nothing to backfill — all leads already have a business line.")
            return

        # ── Phase 2: probe a sample from ManyContacts ─────────────────────────
        probe_leads = null_leads[:probe_limit]
        log.info(f"Probing {len(probe_leads)} contacts via ManyContacts API ...")

        hit_count = 0
        miss_count = 0
        api_error_count = 0
        key_counter: Counter = Counter()
        all_seen_keys: set[str] = set()
        sample_hit: dict | None = None   # first contact where we found a line
        sample_miss: dict | None = None  # first contact where we did NOT find a line

        for i, lead in enumerate(probe_leads):
            mc_id = lead.get("wa_contact_id") or lead.get("phone")
            if not mc_id:
                miss_count += 1
                continue

            contact = await fetch_contact(client, mc_id)
            if contact is None:
                api_error_count += 1
                continue

            all_seen_keys.update(_all_keys(contact))
            line, matched_key = _extract_business_line(contact)

            if line:
                hit_count += 1
                key_counter[matched_key] += 1
                if sample_hit is None:
                    sample_hit = {"lead_id": lead["id"], "mc_id": mc_id, "line": line, "key": matched_key}
                log.debug(f"  [{i+1}] FOUND {line!r} via {matched_key!r} for {mc_id}")
            else:
                miss_count += 1
                if sample_miss is None:
                    sample_miss = {"lead_id": lead["id"], "mc_id": mc_id, "keys": _all_keys(contact)}
                log.debug(f"  [{i+1}] NOT FOUND for {mc_id}")

            # Gentle rate limit
            if (i + 1) % 20 == 0:
                await asyncio.sleep(0.5)
                log.info(f"  ... probed {i+1}/{len(probe_leads)} — hits so far: {hit_count}")

        # ── Phase 3: report ────────────────────────────────────────────────────
        log.info("\n" + "=" * 60)
        log.info("PROBE RESULTS")
        log.info(f"  Leads with null line:   {len(null_leads)}")
        log.info(f"  Probed:                 {len(probe_leads)}")
        log.info(f"  Line found (hits):      {hit_count}")
        log.info(f"  Line not found (miss):  {miss_count}")
        log.info(f"  API errors / no mc_id:  {api_error_count}")

        if hit_count:
            log.info(f"\n  Matched keys (field names that contained the business number):")
            for key, count in key_counter.most_common():
                log.info(f"    {key!r}: {count} times")
            if sample_hit:
                log.info(f"\n  Sample hit: lead={sample_hit['lead_id']} mc_id={sample_hit['mc_id']}")
                log.info(f"    → line={sample_hit['line']!r} via key={sample_hit['key']!r}")
            estimated_recoverable = int(hit_count / len(probe_leads) * len(null_leads))
            log.info(f"\n  Estimated recoverable rows: ~{estimated_recoverable} / {len(null_leads)}")
        else:
            log.info("\n  !! No business line field found in any probed contact.")
            log.info("  All keys seen across probed contacts:")
            for k in sorted(all_seen_keys):
                log.info(f"    {k}")
            if sample_miss:
                log.info(f"\n  Sample miss contact keys: {sample_miss['keys']}")
            log.info("\n  CONCLUSION: ManyContacts contact API does not expose the business")
            log.info("  phone number. True historical backfill is NOT possible from this source.")
            log.info("  Best alternative: treat all rows with null line as pre-split historical")
            log.info("  data, visible only in the 'All' view.")
            return

        if not apply:
            log.info(f"\n  Run with --apply to write updates to DB.")
            return

        # ── Phase 4: apply updates ─────────────────────────────────────────────
        log.info(f"\n{'=' * 60}")
        log.info(f"APPLYING updates to {len(null_leads)} leads ...")
        leads_updated = 0
        leads_failed = 0
        msg_batches_updated = 0

        for i, lead in enumerate(null_leads):
            mc_id = lead.get("wa_contact_id") or lead.get("phone")
            if not mc_id:
                leads_failed += 1
                continue

            contact = await fetch_contact(client, mc_id)
            if contact is None:
                leads_failed += 1
                continue

            line, _ = _extract_business_line(contact)
            if not line:
                leads_failed += 1
                continue

            ok = await update_lead_line(client, lead["id"], line)
            if ok:
                leads_updated += 1
                msg_batches_updated += await update_messages_line(client, lead["id"], line)
            else:
                leads_failed += 1

            if (i + 1) % 20 == 0:
                await asyncio.sleep(0.5)
                log.info(f"  ... {i+1}/{len(null_leads)} — updated: {leads_updated}, failed: {leads_failed}")

        log.info(f"\n{'=' * 60}")
        log.info("APPLY COMPLETE")
        log.info(f"  Leads updated:          {leads_updated}")
        log.info(f"  Leads failed/no line:   {leads_failed}")
        log.info(f"  Message batches patched:{msg_batches_updated}")
        log.info(f"  Remaining null leads:   {leads_failed} (no line found in MC API)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill whatsapp_business_number from ManyContacts API")
    parser.add_argument("--apply", action="store_true", help="Write updates to DB (default: dry run)")
    parser.add_argument("--probe-limit", type=int, default=50,
                        help="How many contacts to probe (default 50). Use 0 for all.")
    args = parser.parse_args()

    probe_limit = args.probe_limit if args.probe_limit > 0 else 10_000
    asyncio.run(run(apply=args.apply, probe_limit=probe_limit))
