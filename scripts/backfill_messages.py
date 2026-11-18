"""
One-time backfill: fetch full message history for all WhatsApp leads from
ManyContacts API, store in messages table, then re-run AI analysis.

Uses raw httpx + Supabase REST to avoid storage3/pyiceberg build issues on Python 3.14.

Usage:
    python backfill_messages.py
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from collections import defaultdict

import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
MC_BASE_URL = os.getenv("MC_BASE_URL", "https://api.manycontacts.com/v1")
MC_API_KEY = os.getenv("MC_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

SB_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}
MC_HEADERS = {"apikey": MC_API_KEY}

# Agent cache: user_id -> name
_agent_cache: dict = {}


def sb_url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


async def sb_get(client: httpx.AsyncClient, table: str, params: dict = None) -> list:
    resp = await client.get(sb_url(table), headers=SB_HEADERS, params=params or {})
    resp.raise_for_status()
    return resp.json()


async def sb_upsert(client: httpx.AsyncClient, table: str, rows: list, on_conflict: str):
    headers = {**SB_HEADERS, "Prefer": f"resolution=merge-duplicates,return=minimal"}
    resp = await client.post(
        f"{sb_url(table)}?on_conflict={on_conflict}",
        headers=headers,
        content=json.dumps(rows),
    )
    resp.raise_for_status()


async def sb_update(client: httpx.AsyncClient, table: str, eq_col: str, eq_val: str, data: dict):
    resp = await client.patch(
        f"{sb_url(table)}?{eq_col}=eq.{eq_val}",
        headers=SB_HEADERS,
        content=json.dumps(data),
    )
    resp.raise_for_status()


async def load_agent_cache(client: httpx.AsyncClient):
    global _agent_cache
    try:
        resp = await client.get(f"{MC_BASE_URL}/users", headers=MC_HEADERS, timeout=15)
        resp.raise_for_status()
        users = resp.json()
        _agent_cache = {str(u["id"]): u["name"] for u in users if isinstance(u, dict)}
        log.info(f"Agent cache loaded: {len(_agent_cache)} agents")
    except Exception as e:
        log.warning(f"Could not load agent cache: {e}")


def resolve_agent(user_id) -> str | None:
    if not user_id:
        return None
    return _agent_cache.get(str(user_id), str(user_id))


async def fetch_contact_messages(client: httpx.AsyncClient, mc_id: str) -> list:
    try:
        resp = await client.get(
            f"{MC_BASE_URL}/contact/{mc_id}/messages",
            headers=MC_HEADERS,
            timeout=30,
        )
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.debug(f"fetch_contact_messages({mc_id}) error: {e}")
        return []


def parse_messages(lead_id: str, mc_id: str, raw_messages: list) -> list:
    rows = []
    for m in raw_messages:
        msg_id = str(m.get("id", ""))
        if not msg_id:
            continue

        msg_type = (m.get("type") or "INBOUND").upper()
        direction = "inbound" if msg_type == "INBOUND" else "outbound"

        body = m.get("text") or m.get("message") or m.get("body") or f"[{msg_type.lower()}]"

        raw_ts = m.get("timestamp") or m.get("created_at") or m.get("createdAt")
        sent_at = None
        if raw_ts:
            try:
                sent_at = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc).isoformat()
            except (ValueError, TypeError):
                sent_at = str(raw_ts) if raw_ts else None

        user_id = m.get("user_id") or m.get("userId")
        agent_name = resolve_agent(user_id) if direction == "outbound" else None

        rows.append({
            "wa_message_id": f"mc_{mc_id}_{msg_id}",
            "lead_id": lead_id,
            "direction": direction,
            "body": body,
            "agent_name": agent_name,
            "sent_at": sent_at,
        })
    return rows


def analyze_conversation(messages: list) -> dict:
    """Call Claude API directly via httpx (sync-style using requests to avoid event loop issues)."""
    import urllib.request

    lines = []
    for m in messages:
        role = "Agent" if m.get("direction") == "outbound" else "Lead"
        lines.append(f"[{m.get('sent_at', '')}] {role}: {m.get('body', '')}")
    conversation_text = "\n".join(lines)

    if not conversation_text.strip():
        return _empty_result()

    system = (
        "You are an analytics engine for Fiper, a trading broker in Arabic-speaking markets. "
        "Analyze the conversation between a Fiper agent and a lead. "
        "Respond ONLY in valid JSON. No explanation, no markdown, no extra text.\n\n"
        '{"sentiment":"positive"|"neutral"|"negative","score":0-100,"topics":[],'
        '"outcome":"converted"|"callback"|"not_interested"|"no_answer"|"ongoing",'
        '"follow_up_needed":true|false,"risk_flags":[],"treatment_score":0-100,'
        '"summary":"Max 2 sentences. Arabic if conversation is Arabic, English if English."}\n\n'
        "topics options: pricing|product_fit|competitor|technical|follow_up|not_decision_maker|trading_education|account_info|greetings|profit_expectations\n"
        "risk_flags options: unanswered|profit_expectations|beginner_risk|stale_callback|negative_sentiment|slow_response"
    )

    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 512,
        "system": system,
        "messages": [{"role": "user", "content": f"Analyze this conversation:\n\n{conversation_text}"}],
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    raw = data["content"][0]["text"].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        return json.loads(raw[start:end]) if start != -1 else _empty_result()


def _empty_result() -> dict:
    return {
        "sentiment": "neutral", "score": 0, "topics": [], "outcome": "ongoing",
        "follow_up_needed": False, "risk_flags": [], "treatment_score": 50,
        "summary": "No conversation data available.",
    }


async def main():
    log.info("=== Backfill started ===")

    async with httpx.AsyncClient(timeout=60) as client:
        # Load agent cache
        await load_agent_cache(client)

        # All WhatsApp leads (fetch in pages of 1000)
        all_leads = []
        offset = 0
        while True:
            page = await sb_get(client, "leads", {
                "channel": "eq.whatsapp",
                "select": "id,wa_contact_id,phone",
                "limit": 1000,
                "offset": offset,
            })
            all_leads.extend(page)
            if len(page) < 1000:
                break
            offset += 1000

        log.info(f"Found {len(all_leads)} WhatsApp leads")

        # Existing analyzed lead_ids
        analyzed_rows = await sb_get(client, "ai_analysis", {"select": "lead_id"})
        analyzed_ids = {r["lead_id"] for r in analyzed_rows}
        log.info(f"Already analyzed: {len(analyzed_ids)} leads")

        # === Phase 1: Fetch & store messages ===
        total_fetched = 0
        total_stored = 0
        failed = 0
        leads_with_messages = 0

        for i, lead in enumerate(all_leads):
            lead_id = lead["id"]
            mc_id = lead.get("wa_contact_id") or lead.get("phone")
            if not mc_id:
                continue

            raw_msgs = await fetch_contact_messages(client, mc_id)

            total_fetched += len(raw_msgs)
            if raw_msgs:
                rows = parse_messages(lead_id, mc_id, raw_msgs)
                if rows:
                    try:
                        chunk_size = 200
                        for j in range(0, len(rows), chunk_size):
                            await sb_upsert(client, "messages", rows[j:j+chunk_size], "wa_message_id")
                        total_stored += len(rows)
                        leads_with_messages += 1
                        log.info(f"[{i+1}/{len(all_leads)}] {mc_id}: {len(raw_msgs)} msgs fetched, {len(rows)} stored")
                    except Exception as e:
                        log.error(f"[{i+1}/{len(all_leads)}] store failed for {mc_id}: {e}")
                        failed += 1
            else:
                if (i + 1) % 50 == 0:
                    log.info(f"[{i+1}/{len(all_leads)}] processed so far...")

            # Gentle rate limit
            if (i + 1) % 20 == 0:
                await asyncio.sleep(0.3)

        log.info(
            f"\n=== Message fetch complete ===\n"
            f"  Leads processed:      {len(all_leads)}\n"
            f"  Leads with messages:  {leads_with_messages}\n"
            f"  Total messages found: {total_fetched}\n"
            f"  Messages stored:      {total_stored}\n"
            f"  Failed:               {failed}"
        )

        if total_stored == 0 and total_fetched == 0:
            log.warning("No messages fetched — check if /v1/contact/{id}/messages endpoint exists in your ManyContacts plan")

        # === Phase 2: AI analysis on leads with messages but no analysis ===
        log.info("\n=== Running AI analysis on newly populated leads ===")

        # Fetch all messages in bulk
        all_msgs_raw = []
        offset = 0
        while True:
            page = await sb_get(client, "messages", {
                "select": "lead_id,direction,body,sent_at",
                "limit": 1000,
                "offset": offset,
            })
            all_msgs_raw.extend(page)
            if len(page) < 1000:
                break
            offset += 1000

        msgs_by_lead: dict = defaultdict(list)
        for m in all_msgs_raw:
            msgs_by_lead[m["lead_id"]].append(m)

        ai_done = 0
        ai_skipped = 0
        ai_failed = 0

        for lead in all_leads:
            lead_id = lead["id"]
            if lead_id in analyzed_ids:
                ai_skipped += 1
                continue

            msgs = sorted(msgs_by_lead.get(lead_id, []), key=lambda m: m.get("sent_at") or "")
            if not msgs:
                continue

            try:
                result = analyze_conversation(msgs)
                # ai_analysis has no unique constraint on lead_id — use plain INSERT
                insert_headers = {**SB_HEADERS, "Prefer": "return=minimal"}
                ins_resp = await client.post(
                    sb_url("ai_analysis"),
                    headers=insert_headers,
                    content=json.dumps({"lead_id": lead_id, "source": "whatsapp", **result}),
                )
                ins_resp.raise_for_status()
                await sb_update(client, "leads", "id", lead_id, {"score": result.get("score")})
                ai_done += 1
                log.info(
                    f"  AI [{ai_done}] lead={lead_id} "
                    f"score={result.get('score')} sentiment={result.get('sentiment')}"
                )
            except Exception as e:
                log.error(f"  AI failed for lead {lead_id}: {e}")
                ai_failed += 1

        log.info(
            f"\n=== AI analysis complete ===\n"
            f"  Newly analyzed:              {ai_done}\n"
            f"  Skipped (already analyzed):  {ai_skipped}\n"
            f"  Failed:                      {ai_failed}"
        )

    log.info("=== Backfill done ===")


if __name__ == "__main__":
    asyncio.run(main())
