import os
import json
import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from typing import AsyncGenerator
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client
from dotenv import load_dotenv

from pipeline import whatsapp, maqsam, ai_analyzer, alert_engine, email_notifications
from api import overview, agents, leads, channels, quality, journey, journey_v2

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fiper")

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
WA_VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN", "")
scheduler = AsyncIOScheduler()
_last_pipeline_run: datetime | None = None
_last_maqsam_sync: datetime | None = None
_last_webhook_payloads: list[dict] = []  # ring buffer — last 5 raw webhook bodies

# SSE client queues — one per connected dashboard tab
_sse_clients: set[asyncio.Queue] = set()


async def _broadcast(event_type: str, payload: dict | None = None):
    """Push a Server-Sent Event to every connected dashboard client."""
    msg = f"event: {event_type}\ndata: {json.dumps(payload or {})}\n\n"
    dead: set[asyncio.Queue] = set()
    for q in _sse_clients:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.add(q)
    for q in dead:
        _sse_clients.discard(q)

_GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
_GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{_GEMINI_MODEL}:generateContent"

_AI_SYSTEM_PROMPT = """You are a quality assurance analyst for Fiper, a trading broker in Arabic-speaking markets.
Analyze the call transcript(s) between Fiper agents and leads.
Maqsam has already determined the sentiment — do NOT re-evaluate it; it is passed to you separately.

IMPORTANT — ROLES: The AGENT is the Fiper employee making the outbound call. The CUSTOMER or LEAD is the person being called by Fiper. Always evaluate agent quality from the Fiper employee's perspective.
IMPORTANT — COMPANY NAME: The company is called FIPER (فايبر in Arabic). It is a trading broker.
Always write "Fiper" in the summary. Never write "Viber", "Fighter", "Faiber", or "financial brokerage company". Never say "financial brokerage" — always say "Fiper".
Write the summary in the same language as the conversation: Arabic if the conversation is in Arabic, English if in English.

Respond ONLY in valid JSON. No explanation, no markdown, no extra text.

{
  "score": 0-100,
  "topics": [],
  "outcome": "converted"|"callback"|"not_interested"|"no_answer"|"ongoing",
  "follow_up_needed": true|false,
  "risk_flags": [],
  "treatment_score": 0-100,
  "summary": "Legacy field. Use the conversation language. Use the company name Fiper.",
  "summary_en": "Max 2 concise sentences in English. Use the company name Fiper.",
  "summary_ar": "Max 2 concise sentences in Arabic. Use the company name Fiper."
}

topics: pricing|product_fit|competitor|technical|follow_up|not_decision_maker|trading_education|account_info|greetings|profit_expectations
risk_flags: unanswered|profit_expectations|beginner_risk|stale_callback|negative_sentiment|slow_response
treatment_score: 90-100 excellent, 70-89 good, 50-69 average, 30-49 poor, 0-29 very poor"""

_BAD_FIPER_NAMES = re.compile(
    r"\b(Fiverr|Viber|Faiber|Fiber|Fighter|financial brokerage company|financial brokerage)\b",
    re.IGNORECASE,
)

_CALL_SUMMARY_SYSTEM_PROMPT = """You summarize Fiper call transcripts for an analytics dashboard.
Always write the company name exactly as "Fiper". Never write Fiverr, Viber, Faiber, Fiber, Fighter, financial brokerage, or financial brokerage company.
Return ONLY valid JSON with this shape:
{
  "summary_en": "Max 2 concise sentences in English.",
  "summary_ar": "Max 2 concise sentences in Arabic."
}
The English summary must be English. The Arabic summary must be Arabic."""


# ---------------------------------------------------------------------------
# Ingestion pipeline
# ---------------------------------------------------------------------------

def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _format_maqsam_transcript(call: dict) -> str:
    """Convert Maqsam transcription segments into a compact QA-friendly text."""
    raw_transcript = call.get("callTranscription") or []
    transcript_lines = []
    for seg in raw_transcript:
        party = "Agent" if seg.get("party") == "agent" else "Customer"
        content = (seg.get("content") or "").strip()
        if content:
            transcript_lines.append(f"{party}: {content}")
    return "\n".join(transcript_lines)


def _maqsam_phone(call: dict) -> str | None:
    direction = call.get("direction", "outbound")
    return call.get("calleeNumber") if direction == "outbound" else call.get("callerNumber")


def _maqsam_called_at(call: dict) -> str | None:
    ts = call.get("timestamp")
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat() if ts else None


def _maqsam_row(call: dict, phone_to_lead: dict[str, str], existing: dict[str, dict]) -> dict:
    """Build a calls row while preserving previously stored AI fields.

    Maqsam sometimes returns a call before transcription/summary is ready. Because
    upsert uses merge semantics, sending None for those fields would erase data
    captured by a later/earlier sync. Keep old values when the fresh payload is
    empty.
    """
    phone = _maqsam_phone(call)
    maqsam_id = str(call.get("id"))
    prev = existing.get(maqsam_id) or {}

    summary_obj = call.get("summary") or {}
    transcript_text = _format_maqsam_transcript(call)
    auto_tags = call.get("callAutoTags") or None

    return {
        "maqsam_id": maqsam_id,
        "lead_id": phone_to_lead.get(phone or ""),
        "agent_name": maqsam.extract_agent_name(call),
        "duration_seconds": call.get("duration"),
        "outcome": call.get("state"),
        "called_at": _maqsam_called_at(call),
        "transcript": transcript_text or prev.get("transcript"),
        "summary_en": summary_obj.get("en") or prev.get("summary_en"),
        "summary_ar": summary_obj.get("ar") or prev.get("summary_ar"),
        "maqsam_sentiment": call.get("sentiment") or prev.get("maqsam_sentiment"),
        "auto_tags": auto_tags or prev.get("auto_tags"),
    }


def _latest_maqsam_called_at() -> datetime | None:
    rows = (
        supabase.table("calls")
        .select("called_at")
        .order("called_at", desc=True)
        .limit(1)
        .execute().data or []
    )
    if not rows or not rows[0].get("called_at"):
        return None
    try:
        return datetime.fromisoformat(rows[0]["called_at"].replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

async def ingest_manycontacts(hours_back: int = 2) -> dict:
    """Poll ManyContacts for contacts updated in the last N hours.

    Saves all messages (inbound + outbound) to the messages table.
    Returns stats dict: {contacts, messages_total, messages_outbound}.
    """
    log.info(f"ManyContacts ingestion started (hours_back={hours_back})")
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(hours=hours_back)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    # Message cutoff: only save messages newer than this (generous window for backfill)
    msg_cutoff = (now - timedelta(hours=max(hours_back * 4, 48))).isoformat()
    now_iso = now.isoformat()

    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.error(f"fetch_users failed: {e}")

    try:
        contacts = await whatsapp.fetch_contacts(date_from=date_from, date_to=date_to)
    except Exception as e:
        log.error(f"ManyContacts fetch_contacts failed: {e}")
        return {"contacts": 0, "messages_total": 0, "messages_outbound": 0}

    msg_total = 0
    msg_outbound = 0

    for contact in contacts:
        mc_id = contact.get("id")
        phone = contact.get("number", "")
        name = contact.get("name")
        open_status = contact.get("open", 1)
        last_user_id = contact.get("last_user_id")
        agent_name = whatsapp.resolve_agent_name(last_user_id)
        updated_at = contact.get("updatedAt")
        status = "engaged" if open_status == 1 else "lost"

        lead_res = supabase.table("leads").upsert({
            "wa_contact_id": mc_id,
            "phone": phone,
            "name": name,
            "channel": "whatsapp",
            "status": status,
            "assigned_agent": agent_name,
            "last_message_at": updated_at,
            "updated_at": now_iso,
        }, on_conflict="wa_contact_id").execute()
        lead_id = lead_res.data[0]["id"] if lead_res.data else None

        if not (lead_id and mc_id):
            continue

        # ── Fetch + save full message history ────────────────────────────────
        raw_msgs = await whatsapp.fetch_contact_messages(mc_id, phone=phone)
        msg_rows = []
        for msg in raw_msgs:
            sent_at = _parse_mc_ts(
                msg.get("timestamp") or msg.get("createdAt") or msg.get("created_at")
            )
            if not sent_at:
                continue
            # Skip messages older than msg_cutoff
            if sent_at < msg_cutoff:
                continue

            direction = (
                "outbound"
                if (msg.get("type") or msg.get("direction") or "").upper() in ("OUTBOUND", "OUT")
                else "inbound"
            )
            body_text = (
                msg.get("text") or msg.get("message") or msg.get("body")
                or msg.get("content") or f"[{(msg.get('type') or 'message').lower()}]"
            )
            msg_id = str(msg.get("id") or msg.get("wamid") or "")
            if not msg_id:
                continue

            # For outbound: agent comes from message's user_id, fall back to contact's assigned agent
            msg_agent = None
            if direction == "outbound":
                msg_agent = whatsapp.resolve_agent_name(msg.get("user_id")) or agent_name
                msg_outbound += 1

            row = {
                "wa_message_id": msg_id,
                "lead_id": lead_id,
                "direction": direction,
                "body": str(body_text)[:2000],
                "sent_at": sent_at,
            }
            if msg_agent:
                row["agent_name"] = msg_agent
            msg_rows.append(row)
            msg_total += 1

        # Upsert in chunks of 100 to stay within Supabase request limits
        for i in range(0, len(msg_rows), 100):
            supabase.table("messages").upsert(
                msg_rows[i:i + 100], on_conflict="wa_message_id"
            ).execute()

    log.info(
        f"ManyContacts ingestion done — {len(contacts)} contacts | "
        f"{msg_total} messages ({msg_outbound} outbound)"
    )
    return {"contacts": len(contacts), "messages_total": msg_total, "messages_outbound": msg_outbound}


async def ingest_manycontacts_activity(days_back: int = 1) -> dict:
    """Sync ManyContacts conversation activity without message bodies.

    ManyContacts does not expose conversation message history via API, but the
    contacts endpoint does expose recently updated conversations and last_user_id.
    This keeps dashboard WhatsApp activity current while message body capture
    still depends on webhooks.
    """
    log.info("ManyContacts activity sync started")
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    now_iso = now.isoformat()

    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.warning(f"ManyContacts users sync failed: {e}")

    try:
        contacts = await whatsapp.fetch_contacts(date_from=date_from, date_to=date_to)
    except Exception as e:
        log.error(f"ManyContacts activity fetch failed: {e}")
        return {"contacts_seen": 0, "contacts_upserted": 0, "error": str(e)}

    rows = []
    for contact in contacts:
        mc_id = contact.get("id")
        phone = contact.get("number") or contact.get("phone")
        if not (mc_id or phone):
            continue
        updated_at = (
            contact.get("updatedAt") or contact.get("updated_at")
            or contact.get("updated") or now_iso
        )
        last_user_id = contact.get("last_user_id")
        agent_name = whatsapp.resolve_agent_name(last_user_id) if last_user_id else None
        open_status = contact.get("open", 1)
        rows.append({
            "wa_contact_id": mc_id or phone,
            "phone": phone,
            "name": contact.get("name"),
            "channel": "whatsapp",
            "status": "engaged" if open_status == 1 else "lost",
            "last_message_at": updated_at,
            "updated_at": now_iso,
            **({"assigned_agent": agent_name} if agent_name else {}),
        })

    for chunk in _chunks(rows, 200):
        supabase.table("leads").upsert(chunk, on_conflict="wa_contact_id").execute()

    log.info(f"ManyContacts activity sync done — {len(rows)} contacts")
    return {"contacts_seen": len(contacts), "contacts_upserted": len(rows)}


async def ingest_maqsam(days_back: int = 3, overlap_minutes: int = 30) -> dict:
    """Fetch Maqsam calls incrementally.

    A frequent sync should not re-read days of calls on every run. We use the
    latest stored call as the cursor, then step back a little so calls that later
    gain transcript/summary data are updated.
    """
    log.info("Maqsam ingestion started")
    now = datetime.now(timezone.utc)
    latest_called_at = _latest_maqsam_called_at()
    if latest_called_at:
        since_dt = latest_called_at - timedelta(minutes=overlap_minutes)
    else:
        since_dt = now - timedelta(days=days_back)
    min_since_dt = now - timedelta(days=days_back)
    if since_dt < min_since_dt:
        since_dt = min_since_dt

    since_ts = int(since_dt.timestamp())
    date_from = since_dt.strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    try:
        calls = await maqsam.fetch_calls(date_from, date_to, since_ts=since_ts)
    except Exception as e:
        log.error(f"Maqsam fetch_calls failed: {e}")
        return {"calls_seen": 0, "calls_upserted": 0, "since": since_dt.isoformat(), "error": str(e)}

    now_iso = datetime.now(timezone.utc).isoformat()
    # Batch-upsert unique leads first, then calls
    unique_phones: dict[str, None] = {}
    for call in calls:
        phone = _maqsam_phone(call)
        if phone:
            unique_phones[phone] = None

    if unique_phones:
        lead_rows = [
            {"wa_contact_id": p, "phone": p, "channel": "maqsam", "updated_at": now_iso}
            for p in unique_phones
        ]
        for chunk in _chunks(lead_rows, 200):
            supabase.table("leads").upsert(chunk, on_conflict="wa_contact_id").execute()

    # Build phone → lead_id map
    phone_to_lead: dict[str, str] = {}
    if unique_phones:
        for phones in _chunks(list(unique_phones.keys()), 500):
            result = (
                supabase.table("leads")
                .select("id,wa_contact_id")
                .in_("wa_contact_id", phones)
                .execute()
            )
            phone_to_lead.update({r["wa_contact_id"]: r["id"] for r in (result.data or [])})

    # Preserve transcript/summary data if the fresh Maqsam row does not contain it.
    maqsam_ids = list({str(call.get("id")) for call in calls if call.get("id")})
    existing_calls: dict[str, dict] = {}
    for ids in _chunks(maqsam_ids, 500):
        rows = (
            supabase.table("calls")
            .select("maqsam_id,transcript,summary_en,summary_ar,maqsam_sentiment,auto_tags")
            .in_("maqsam_id", ids)
            .execute().data or []
        )
        existing_calls.update({r["maqsam_id"]: r for r in rows})

    # Batch-upsert calls in chunks of 200
    call_rows = []
    for call in calls:
        if call.get("id"):
            call_rows.append(_maqsam_row(call, phone_to_lead, existing_calls))

    # Deduplicate by maqsam_id (Maqsam can return duplicate IDs across pages)
    call_rows = list({r["maqsam_id"]: r for r in call_rows}.values())

    # Normalize all rows to identical key set — PostgREST PGRST102 requires this
    ALL_CALL_KEYS = {
        "maqsam_id", "lead_id", "agent_name", "duration_seconds", "outcome", "called_at",
        "transcript", "summary_en", "summary_ar", "maqsam_sentiment", "auto_tags",
    }
    for row in call_rows:
        for k in ALL_CALL_KEYS:
            if k not in row:
                row[k] = None

    chunk_size = 200
    for i in range(0, len(call_rows), chunk_size):
        supabase.table("calls").upsert(call_rows[i:i + chunk_size], on_conflict="maqsam_id").execute()

    transcript_count = sum(1 for r in call_rows if r.get("transcript"))
    log.info(
        f"Maqsam ingestion done — {len(call_rows)} calls "
        f"({transcript_count} with transcripts), since={since_dt.isoformat()}"
    )
    return {
        "calls_seen": len(calls),
        "calls_upserted": len(call_rows),
        "with_transcripts": transcript_count,
        "since": since_dt.isoformat(),
    }


def _parse_ai_json(raw: str, fallback: dict | None = None) -> dict:
    import json as _json

    fallback = fallback or {
        "score": 0,
        "topics": [],
        "outcome": "ongoing",
        "follow_up_needed": False,
        "risk_flags": [],
        "treatment_score": 50,
        "summary": "Parse error.",
    }
    raw = (raw or "").strip()
    try:
        return _json.loads(raw)
    except _json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}") + 1
        if start != -1 and end > start:
            return _json.loads(raw[start:end])
        return fallback


async def _call_gemini_async(user_message: str) -> dict:
    """Call Gemini via async httpx. Returns parsed JSON result dict."""
    import httpx as _httpx
    import json as _json

    if not _GEMINI_KEY:
        raise RuntimeError("GEMINI_API_KEY is missing")

    payload = {
        "systemInstruction": {"parts": [{"text": _AI_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": user_message}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 700,
            "responseMimeType": "application/json",
        },
    }
    async with _httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{_GEMINI_URL}?key={_GEMINI_KEY}",
            headers={"content-type": "application/json"},
            content=_json.dumps(payload),
        )
        resp.raise_for_status()
        raw = (
            resp.json()
            .get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )

    return _parse_ai_json(raw)


def _clean_fiper_text(text: str | None) -> str:
    if not text:
        return ""
    return _BAD_FIPER_NAMES.sub("Fiper", text)


def _has_arabic(text: str | None) -> bool:
    return bool(text and re.search(r"[\u0600-\u06FF]", text))


def _normalize_ai_result(result: dict) -> dict:
    """Keep legacy summary plus bilingual display summaries when the AI returns them."""
    result = dict(result)
    legacy = _clean_fiper_text(result.get("summary") or "")
    summary_en = _clean_fiper_text(result.get("summary_en") or "")
    summary_ar = _clean_fiper_text(result.get("summary_ar") or "")

    if legacy and not summary_ar and _has_arabic(legacy):
        summary_ar = legacy
    if legacy and not summary_en and not _has_arabic(legacy):
        summary_en = legacy

    result["summary_en"] = summary_en
    result["summary_ar"] = summary_ar
    result["summary"] = summary_ar or summary_en or legacy
    return result


def _legacy_ai_result(result: dict) -> dict:
    return {k: v for k, v in result.items() if k not in {"summary_en", "summary_ar"}}


def _save_ai_analysis(lead_id: str, source: str, result: dict):
    existing = (
        supabase.table("ai_analysis")
        .select("id")
        .eq("lead_id", lead_id)
        .execute().data or []
    )
    try:
        if existing:
            supabase.table("ai_analysis").update(result).eq("lead_id", lead_id).execute()
        else:
            supabase.table("ai_analysis").insert(
                {"lead_id": lead_id, "source": source, **result}
            ).execute()
    except Exception as exc:
        log.warning(f"AI analysis bilingual columns unavailable; using legacy summary only: {exc}")
        legacy = _legacy_ai_result(result)
        if existing:
            supabase.table("ai_analysis").update(legacy).eq("lead_id", lead_id).execute()
        else:
            supabase.table("ai_analysis").insert(
                {"lead_id": lead_id, "source": source, **legacy}
            ).execute()


async def _call_gemini_call_summary_async(call: dict) -> dict:
    """Generate bilingual call summaries for one Fiper call transcript."""
    import httpx as _httpx
    import json as _json

    if not _GEMINI_KEY:
        raise RuntimeError("GEMINI_API_KEY is missing")

    transcript = (call.get("transcript") or "").strip()
    user_message = (
        f"Duration: {call.get('duration_seconds')}s\n"
        f"Outcome: {call.get('outcome') or 'unknown'}\n"
        f"Agent: {call.get('agent_name') or 'unknown'}\n\n"
        f"Transcript:\n{transcript[:5000]}"
    )
    payload = {
        "systemInstruction": {"parts": [{"text": _CALL_SUMMARY_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": user_message}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 500,
            "responseMimeType": "application/json",
        },
    }
    async with _httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{_GEMINI_URL}?key={_GEMINI_KEY}",
            headers={"content-type": "application/json"},
            content=_json.dumps(payload),
        )
        resp.raise_for_status()
        raw = (
            resp.json()
            .get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )

    parsed = _parse_ai_json(raw, fallback={})

    return {
        "summary_en": _clean_fiper_text(parsed.get("summary_en") or ""),
        "summary_ar": _clean_fiper_text(parsed.get("summary_ar") or ""),
    }


async def backfill_call_summaries(hours_back: int = 6, limit: int = 30) -> dict:
    """Fill missing summaries for calls over 30s when a transcript exists."""
    if not _GEMINI_KEY:
        log.info("Call summary backfill skipped: GEMINI_API_KEY is missing")
        return {"summarized": 0, "errors": 0, "skipped": 0}

    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = (
        supabase.table("calls")
        .select("id,maqsam_id,agent_name,duration_seconds,outcome,called_at,transcript,summary_en,summary_ar")
        .gte("called_at", cutoff_iso)
        .gt("duration_seconds", 30)
        .not_.is_("transcript", "null")
        .order("called_at", desc=True)
        .limit(limit * 3)
        .execute().data or []
    )
    candidates = [
        r for r in rows
        if (r.get("transcript") or "").strip() and not (r.get("summary_en") and r.get("summary_ar"))
    ][:limit]

    ok = err = skipped = 0
    for call in candidates:
        try:
            summary = await _call_gemini_call_summary_async(call)
            update = {
                k: v for k, v in summary.items()
                if v and not call.get(k)
            }
            if not update:
                skipped += 1
                continue
            supabase.table("calls").update(update).eq("id", call["id"]).execute()
            ok += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            log.error(f"Call summary backfill failed for call {call.get('maqsam_id') or call.get('id')}: {e}")
            err += 1

    log.info(f"Call summary backfill done — {ok} summarized, {err} errors, {skipped} skipped")
    return {"summarized": ok, "errors": err, "skipped": skipped}


async def run_ai_analysis(hours_back: int = 3):
    """Analyze leads that have no ai_analysis in the selected recent window.

    Maqsam leads: uses transcript + maqsam_sentiment from calls table when available.
    WhatsApp leads: uses message bodies as conversation text.
    Skips leads with no transcript/messages or whose calls are all 0s duration.
    """
    from collections import defaultdict

    now = datetime.now(timezone.utc)
    # Default 3h window has a buffer over the 2h full pipeline schedule. Manual
    # triggers can pass a wider window after a Maqsam catch-up sync.
    cutoff_iso = (now - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_stats = await backfill_call_summaries(hours_back=max(hours_back, 6), limit=30)

    # ── Leads already analyzed in this window ─────────────────────────────────
    recently_analyzed = {
        r["lead_id"]
        for r in (
            supabase.table("ai_analysis")
            .select("lead_id")
            .gte("analyzed_at", cutoff_iso)
            .execute().data or []
        )
    }

    # ── Leads active in the window ────────────────────────────────────────────
    all_leads = (
        supabase.table("leads")
        .select("id,channel")
        .gte("last_message_at", cutoff_iso)
        .execute().data or []
    )
    # Also include leads from recent calls
    recent_call_lead_ids = {
        r["lead_id"]
        for r in (
            supabase.table("calls")
            .select("lead_id")
            .gte("called_at", cutoff_iso)
            .not_.is_("lead_id", "null")
            .execute().data or []
        )
    }
    all_lead_ids = {l["id"] for l in all_leads} | recent_call_lead_ids
    to_analyze = all_lead_ids - recently_analyzed

    if not to_analyze:
        log.info(f"AI analysis: no new leads to analyze (hours_back={hours_back})")
        return {"analyzed": 0, "errors": 0, "skipped": 0, "call_summaries": summary_stats}

    log.info(f"AI analysis: {len(to_analyze)} leads to analyze (hours_back={hours_back})")

    # ── Bulk-fetch calls and messages for those leads ─────────────────────────
    lead_ids_list = list(to_analyze)

    # Fetch in chunks of 500 to stay within PostgREST IN() limits
    all_calls_raw = []
    all_msgs_raw  = []
    chunk = 500
    for i in range(0, len(lead_ids_list), chunk):
        batch_ids = lead_ids_list[i:i + chunk]
        all_calls_raw += (
            supabase.table("calls")
            .select("lead_id,duration_seconds,outcome,agent_name,called_at,"
                    "transcript,maqsam_sentiment,summary_en")
            .in_("lead_id", batch_ids)
            .gt("duration_seconds", 0)
            .execute().data or []
        )
        all_msgs_raw += (
            supabase.table("messages")
            .select("lead_id,direction,body,sent_at,agent_name")
            .in_("lead_id", batch_ids)
            .execute().data or []
        )

    # Also need channel per lead
    lead_channel = {}
    for batch_start in range(0, len(lead_ids_list), chunk):
        batch_ids = lead_ids_list[batch_start:batch_start + chunk]
        rows = (
            supabase.table("leads")
            .select("id,channel")
            .in_("id", batch_ids)
            .execute().data or []
        )
        for r in rows:
            lead_channel[r["id"]] = r.get("channel", "")

    calls_by_lead: dict[str, list] = defaultdict(list)
    for c in all_calls_raw:
        if c.get("lead_id"):
            calls_by_lead[c["lead_id"]].append(c)

    msgs_by_lead: dict[str, list] = defaultdict(list)
    for m in all_msgs_raw:
        if m.get("lead_id"):
            msgs_by_lead[m["lead_id"]].append(m)

    # ── Analyze each lead ─────────────────────────────────────────────────────
    ok = err = skipped = 0

    for lead_id in to_analyze:
        channel = lead_channel.get(lead_id, "")

        if channel == "maqsam":
            calls = sorted(calls_by_lead.get(lead_id, []),
                           key=lambda c: c.get("called_at") or "")
            if not calls:
                skipped += 1
                continue

            # Build prompt blocks from transcripts
            blocks = []
            sentiments = []
            for i, call in enumerate(calls, 1):
                transcript = (call.get("transcript") or "").strip()
                sentiment  = call.get("maqsam_sentiment")
                summary_en = call.get("summary_en") or ""
                if sentiment:
                    sentiments.append(sentiment)

                block = (
                    f"=== CALL {i} | Duration: {call.get('duration_seconds')}s | "
                    f"State: {call.get('outcome')} | Agent: {call.get('agent_name') or 'unknown'} ==="
                )
                if sentiment:
                    block += f"\nMaqsam Sentiment: {sentiment}"
                if summary_en:
                    block += f"\nSummary: {summary_en}"
                if transcript:
                    # Cap at 3000 chars to control token cost
                    block += f"\n\nTranscript:\n{transcript[:3000]}"
                blocks.append(block)

            if not any(call.get("transcript") for call in calls):
                skipped += 1
                continue

            mq_sentiment = (
                "negative" if "negative" in sentiments else
                "positive" if "positive" in sentiments else
                (sentiments[0] if sentiments else None)
            )
            user_msg = (
                f"Maqsam sentiment (pre-determined): {mq_sentiment or 'unknown'}\n\n"
                f"Analyze these calls:\n\n" + "\n\n".join(blocks)
            )
            source = "maqsam"

        elif channel == "whatsapp":
            msgs = sorted(msgs_by_lead.get(lead_id, []),
                          key=lambda m: m.get("sent_at") or "")
            if not msgs:
                skipped += 1
                continue

            lines = []
            for m in msgs:
                role = "Agent" if m.get("direction") == "outbound" else "Customer"
                lines.append(f"{role}: {(m.get('body') or '')[:300]}")
            user_msg = (
                "Maqsam sentiment (pre-determined): unknown\n\n"
                "Analyze this WhatsApp conversation:\n\n" + "\n".join(lines)
            )
            mq_sentiment = None
            source = "whatsapp"

        else:
            skipped += 1
            continue

        try:
            result = _normalize_ai_result(await _call_gemini_async(user_msg))
            if mq_sentiment:
                result["sentiment"] = mq_sentiment

            _save_ai_analysis(lead_id, source, result)

            supabase.table("leads").update({"score": result.get("score")}).eq("id", lead_id).execute()
            ok += 1
            await asyncio.sleep(0.3)  # stay within AI rate limits

        except Exception as e:
            log.error(f"AI analysis failed for lead {lead_id}: {e}")
            err += 1

    log.info(f"AI analysis done — {ok} analyzed, {err} errors, {skipped} skipped (no data)")
    return {"analyzed": ok, "errors": err, "skipped": skipped, "call_summaries": summary_stats}


async def run_pipeline():
    """Full pipeline: ingest → AI analysis → alerts. Runs every 2 hours."""
    global _last_pipeline_run
    log.info("Pipeline started")
    await ingest_manycontacts()
    await ingest_maqsam()
    await run_ai_analysis()
    try:
        alert_engine.run_all_checks()
    except Exception as e:
        log.error(f"Alert engine error: {e}")
    _last_pipeline_run = datetime.now(timezone.utc)
    log.info("Pipeline complete")
    await _broadcast("data_updated", {"source": "pipeline"})


async def run_maqsam_realtime_sync():
    """Lightweight high-frequency job for live call totals and transcripts."""
    global _last_maqsam_sync
    stats = await ingest_maqsam(days_back=1, overlap_minutes=45)
    _last_maqsam_sync = datetime.now(timezone.utc)
    await _broadcast("data_updated", {"source": "maqsam", **stats})


async def run_whatsapp_activity_sync():
    """Lightweight high-frequency job for WhatsApp conversation activity."""
    stats = await ingest_manycontacts_activity(days_back=1)
    await _broadcast("data_updated", {"source": "whatsapp_activity", **stats})


async def run_whatsapp_message_sync():
    """Full WhatsApp message sync — pulls message bodies for contacts active in the last hour.
    Runs every 5 minutes so in/out counts stay fresh without waiting for the 2-hour pipeline."""
    stats = await ingest_manycontacts(hours_back=1)
    await _broadcast("data_updated", {"source": "whatsapp_messages", **stats})


async def _force_reanalyze_top(limit: int = 20):
    """Re-run AI analysis on the top `limit` leads by score, bypassing the
    recency guard.  Used once to backfill corrected summaries after a prompt fix.
    """
    from collections import defaultdict as _dd
    log.info(f"[reanalyze] Starting force-reanalysis of top {limit} leads by score")

    leads = (
        supabase.table("leads")
        .select("id,phone,channel")
        .gt("score", 0)
        .order("score", desc=True)
        .limit(limit)
        .execute()
        .data or []
    )
    if not leads:
        log.info("[reanalyze] No scored leads found — nothing to do")
        return

    lead_ids = [l["id"] for l in leads]

    all_calls_raw = (
        supabase.table("calls")
        .select("lead_id,duration_seconds,outcome,agent_name,called_at,transcript,maqsam_sentiment,summary_en")
        .in_("lead_id", lead_ids)
        .gt("duration_seconds", 0)
        .execute()
        .data or []
    )
    all_msgs_raw = (
        supabase.table("messages")
        .select("lead_id,direction,body,sent_at,agent_name")
        .in_("lead_id", lead_ids)
        .execute()
        .data or []
    )

    # Cross-channel: same person may have calls under one lead_id, msgs under another
    phones = list({l.get("phone") for l in leads if l.get("phone")})
    if phones:
        cross = (
            supabase.table("leads").select("id,phone")
            .in_("phone", phones).execute().data or []
        )
        cross_ids = [c["id"] for c in cross if c.get("id")]
        cross_phone = {c["id"]: c.get("phone") for c in cross}
        if cross_ids:
            extra = (
                supabase.table("messages")
                .select("lead_id,direction,body,sent_at,agent_name")
                .in_("lead_id", cross_ids)
                .execute()
                .data or []
            )
            all_msgs_raw.extend(extra)

    calls_by: dict = _dd(list)
    for c in all_calls_raw:
        if c.get("lead_id"):
            calls_by[c["lead_id"]].append(c)

    lead_phone = {l["id"]: l.get("phone") for l in leads}
    msgs_by: dict = _dd(list)
    for m in all_msgs_raw:
        msg_lid = m.get("lead_id")
        if not msg_lid:
            continue
        # Direct match
        if msg_lid in lead_phone:
            msgs_by[msg_lid].append(m)
            continue
        # Cross-channel: map via phone
        phone = cross_phone.get(msg_lid) if phones else None
        if phone:
            for top_lid, top_phone in lead_phone.items():
                if top_phone == phone:
                    msgs_by[top_lid].append(m)

    ok = err = skipped = 0
    for lead in leads:
        lid     = lead["id"]
        channel = lead.get("channel", "")
        phone   = lead.get("phone") or lid[:8]

        try:
            if channel == "maqsam":
                calls = sorted(calls_by.get(lid, []), key=lambda c: c.get("called_at") or "")
                if not calls or not any(c.get("transcript") for c in calls):
                    log.info(f"[reanalyze] SKIP {phone} — no transcript")
                    skipped += 1
                    continue

                blocks, sentiments = [], []
                for i, c in enumerate(calls, 1):
                    transcript = (c.get("transcript") or "").strip()
                    sentiment  = c.get("maqsam_sentiment")
                    if sentiment:
                        sentiments.append(sentiment)
                    block = (f"=== CALL {i} | Duration: {c.get('duration_seconds')}s | "
                             f"State: {c.get('outcome')} | Agent: {c.get('agent_name') or 'unknown'} ===")
                    if sentiment:
                        block += f"\nMaqsam Sentiment: {sentiment}"
                    if c.get("summary_en"):
                        block += f"\nSummary: {c['summary_en']}"
                    if transcript:
                        block += f"\n\nTranscript:\n{transcript[:3000]}"
                    blocks.append(block)

                mq_sentiment = (
                    "negative" if "negative" in sentiments else
                    "positive" if "positive" in sentiments else
                    (sentiments[0] if sentiments else None)
                )
                user_msg = (f"Maqsam sentiment (pre-determined): {mq_sentiment or 'unknown'}\n\n"
                            f"Analyze these calls:\n\n" + "\n\n".join(blocks))
                source = "maqsam"

            elif channel == "whatsapp":
                msgs = sorted(msgs_by.get(lid, []), key=lambda m: m.get("sent_at") or "")
                if not msgs:
                    log.info(f"[reanalyze] SKIP {phone} — no messages")
                    skipped += 1
                    continue
                lines = []
                for m in msgs:
                    role = "Agent" if m.get("direction") == "outbound" else "Customer"
                    lines.append(f"{role}: {(m.get('body') or '')[:300]}")
                user_msg = ("Maqsam sentiment (pre-determined): unknown\n\n"
                            "Analyze this WhatsApp conversation:\n\n" + "\n".join(lines))
                mq_sentiment = None
                source = "whatsapp"
            else:
                skipped += 1
                continue

            result = _normalize_ai_result(await _call_gemini_async(user_msg))
            if mq_sentiment:
                result["sentiment"] = mq_sentiment

            _save_ai_analysis(lid, source, result)
            supabase.table("leads").update({"score": result.get("score")}).eq("id", lid).execute()

            summary = (result.get("summary") or "")[:100]
            log.info(f"[reanalyze] OK {phone} score={result.get('score')} — {summary!r}")
            ok += 1
            await asyncio.sleep(0.4)

        except Exception as e:
            log.error(f"[reanalyze] ERROR {phone}: {e}")
            err += 1

    log.info(f"[reanalyze] Done — {ok} updated, {err} errors, {skipped} skipped")


async def _ingest_mc_with_stats(hours_back: int) -> dict:
    """Wrapper that runs ingest_manycontacts and counts pre/post message rows."""
    before = (supabase.table("messages").select("id", count="exact")
              .eq("direction", "outbound").execute().count or 0)
    stats = await ingest_manycontacts(hours_back=hours_back)
    after = (supabase.table("messages").select("id", count="exact")
             .eq("direction", "outbound").execute().count or 0)
    stats["outbound_in_db_before"] = before
    stats["outbound_in_db_after"] = after
    stats["new_outbound_saved"] = after - before
    return stats


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

def _ping_self():
    """Hit /ping so Render free tier doesn't spin down between pipeline runs."""
    import urllib.request
    base = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000").rstrip("/")
    try:
        urllib.request.urlopen(f"{base}/ping", timeout=10)
        log.debug("Self-ping OK")
    except Exception as e:
        log.warning(f"Self-ping failed: {e}")


def send_sales_report(report_label: str):
    try:
        sent = email_notifications.send_supervisor_report(report_label)
        log.info(f"Sales report {report_label} sent={sent}")
    except Exception as e:
        log.error(f"Sales report {report_label} failed: {e}", exc_info=True)


def _require_email_debug_token(token: str):
    expected = os.getenv("EMAIL_DEBUG_TOKEN", "")
    if not expected or token != expected:
        raise HTTPException(status_code=403, detail="Invalid email debug token")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm agent cache on startup
    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.warning(f"Could not pre-load agent cache: {e}")

    scheduler.add_job(
        run_maqsam_realtime_sync,
        "interval",
        minutes=2,
        id="maqsam_realtime",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    scheduler.add_job(
        run_whatsapp_activity_sync,
        "interval",
        minutes=2,
        id="whatsapp_activity",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    scheduler.add_job(
        run_whatsapp_message_sync,
        "interval",
        minutes=5,
        id="whatsapp_messages",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    scheduler.add_job(
        run_ai_analysis,
        "interval",
        minutes=15,
        id="ai_analysis",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=3),
    )
    scheduler.add_job(
        run_pipeline,
        "interval",
        hours=2,
        id="pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(_ping_self, "interval", minutes=10, id="keepalive", replace_existing=True)
    scheduler.add_job(
        send_sales_report,
        "cron",
        hour=13,
        minute=0,
        timezone="UTC",
        args=["13:00 UTC"],
        id="sales_report_13",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        send_sales_report,
        "cron",
        hour=19,
        minute=0,
        timezone="UTC",
        args=["19:00 UTC"],
        id="sales_report_19",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    log.info(
        "Scheduler started — Maqsam every 2 min, WA activity every 2 min, "
        "WA messages every 5 min, AI every 15 min, full pipeline every 2 h"
    )

    yield
    scheduler.shutdown()


app = FastAPI(title="Fiper Intelligence API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(overview.router)
app.include_router(agents.router)
app.include_router(leads.router)
app.include_router(channels.router)
app.include_router(quality.router)
app.include_router(journey.router)
app.include_router(journey_v2.router)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def dashboard():
    return FileResponse("static/index.html")


# ---------------------------------------------------------------------------
# ManyContacts webhook (real-time messages)
# ---------------------------------------------------------------------------

def _verify_webhook_challenge(hub_mode: str | None, hub_verify_token: str | None, hub_challenge: str | None):
    if hub_mode == "subscribe" and hub_challenge:
        if not WA_VERIFY_TOKEN or hub_verify_token == WA_VERIFY_TOKEN:
            return int(hub_challenge)
    return {"status": "ok"}


@app.get("/webhook/manycontacts")
@app.get("/webhook/whatsapp")
@app.get("/webhook/meta")
@app.get("/webhook/wa")
async def webhook_manycontacts_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    """Meta webhook verification — echoes hub.challenge back when token matches."""
    log.info(f"[webhook/mc] GET | hub_mode={hub_mode!r} hub_challenge={hub_challenge!r}")
    return _verify_webhook_challenge(hub_mode, hub_verify_token, hub_challenge)


def _ts_to_iso(raw_ts, fallback: str) -> str:
    if raw_ts:
        try:
            return datetime.fromtimestamp(int(raw_ts), tz=timezone.utc).isoformat()
        except (ValueError, TypeError):
            pass
    return fallback


def _parse_mc_ts(ts) -> str | None:
    """Convert a ManyContacts message timestamp to ISO string.

    MC sends either a unix int/string or an ISO datetime string.
    Returns None if unparseable.
    """
    if not ts:
        return None
    # Try unix timestamp (int or numeric string)
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        pass
    # Try ISO string as-is
    if isinstance(ts, str):
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return ts
        except (ValueError, TypeError):
            pass
    return None


def _mc_message_direction(raw_type: str | None) -> str | None:
    msg_type = (raw_type or "").strip().lower()
    if msg_type in {"sent", "outbound", "out", "agent", "agent_message", "from_me", "fromme"}:
        return "outbound"
    if msg_type in {"received", "inbound", "in", "customer", "contact", "incoming"}:
        return "inbound"
    return None


def _first_dict(*values) -> dict:
    for value in values:
        if isinstance(value, dict):
            return value
    return {}


def _first_list_item(value):
    return value[0] if isinstance(value, list) and value else value


def _find_whatsapp_lead(contact_id: str | None, phone: str | None) -> dict | None:
    """Find an existing WhatsApp lead by ManyContacts id or phone."""
    lookups = []
    if contact_id:
        lookups.append(("wa_contact_id", contact_id))
    if phone:
        lookups.append(("wa_contact_id", phone))
        lookups.append(("phone", phone))

    seen = set()
    for col, val in lookups:
        key = (col, val)
        if key in seen:
            continue
        seen.add(key)
        rows = (
            supabase.table("leads")
            .select("id,wa_contact_id,phone")
            .eq(col, val)
            .eq("channel", "whatsapp")
            .limit(1)
            .execute().data or []
        )
        if rows:
            return rows[0]
    return None


def _message_duplicate_exists(
    lead_id: str | None,
    phone: str | None,
    direction: str,
    body_text: str,
    sent_at: str,
) -> bool:
    """Detect same WhatsApp message delivered by both Meta and ManyContacts.

    The two webhook formats can describe the same inbound message with different
    IDs (wamid vs ManyContacts UUID), so wa_message_id alone cannot dedupe it.
    """
    if not sent_at:
        return False

    lead_ids = set()
    if lead_id:
        lead_ids.add(lead_id)
    if phone:
        rows = (
            supabase.table("leads")
            .select("id")
            .eq("channel", "whatsapp")
            .eq("phone", phone)
            .execute().data or []
        )
        lead_ids.update(r["id"] for r in rows if r.get("id"))

    if not lead_ids:
        return False

    rows = (
        supabase.table("messages")
        .select("id")
        .in_("lead_id", list(lead_ids))
        .eq("direction", direction)
        .eq("sent_at", sent_at)
        .eq("body", str(body_text)[:2000])
        .limit(1)
        .execute().data or []
    )
    return bool(rows)


OUTBOUND_STATUS_PLACEHOLDER = "[outbound body unavailable from WhatsApp status webhook]"


def _nearby_outbound_rows(lead_id: str, sent_at: str, *, placeholder_only: bool = False) -> list[dict]:
    try:
        center = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return []
    start = (center - timedelta(seconds=10)).isoformat()
    end = (center + timedelta(seconds=10)).isoformat()
    query = (
        supabase.table("messages")
        .select("id,wa_message_id,body,status,sent_at")
        .eq("lead_id", lead_id)
        .eq("direction", "outbound")
        .gte("sent_at", start)
        .lte("sent_at", end)
        .order("sent_at", desc=True)
        .limit(5)
    )
    if placeholder_only:
        query = query.eq("body", OUTBOUND_STATUS_PLACEHOLDER)
    else:
        query = query.neq("body", OUTBOUND_STATUS_PLACEHOLDER)
    return query.execute().data or []


def _is_outbound_mc(body: dict) -> bool:
    """Return True if the payload looks like a ManyContacts outbound/agent message."""
    if body.get("direction") == "outbound":
        return True
    if body.get("type") == "agent_message":
        return True
    if body.get("fromMe") is True:
        return True
    sender = body.get("sender")
    if isinstance(sender, dict) and sender.get("type") == "agent":
        return True
    # Legacy explicit keys
    if "agent" in body or "user" in body:
        return True
    return False


def _extract_mc_outbound(body: dict) -> tuple[str | None, str | None, str, str, str | None]:
    """Parse ManyContacts native outbound webhook payload.

    Returns (phone, agent_name, body_text, sent_at, msg_id).
    Handles multiple known field-name patterns defensively.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # ── Agent name ───────────────────────────────────────────────────────────
    # Try: agent / user / sender (newer MC format)
    root = _first_dict(body.get("data"), body.get("payload"), body)
    delta = _first_dict(root.get("delta"), root.get("eventData"), root.get("data"))
    agent_obj = _first_dict(root.get("agent"), root.get("user"), root.get("sender"), delta.get("user"))
    if isinstance(agent_obj, dict):
        agent_name = (agent_obj.get("name") or agent_obj.get("fullName")
                      or agent_obj.get("displayName") or agent_obj.get("username")
                      or agent_obj.get("email"))
    else:
        agent_name = str(agent_obj) if agent_obj else None
    agent_id = delta.get("user_id") or delta.get("userId") or root.get("user_id") or root.get("userId")
    if not agent_name and agent_id:
        agent_name = whatsapp.resolve_agent_name(agent_id)

    # ── Contact phone ────────────────────────────────────────────────────────
    contact_obj = _first_dict(root.get("contact"), delta.get("contact"), root.get("chat"), delta.get("chat"))
    if isinstance(contact_obj, dict):
        phone = (contact_obj.get("number") or contact_obj.get("phone")
                 or contact_obj.get("wa_id") or contact_obj.get("id")
                 or contact_obj.get("jid", "").split("@")[0])  # WhatsApp JID format
    else:
        phone = str(contact_obj) if contact_obj else None
    # Fallback: top-level fields, or strip @s.whatsapp.net from "to" field
    to_raw = root.get("to") or body.get("to") or root.get("recipient") or body.get("recipient") or ""
    if isinstance(to_raw, str):
        to_raw = to_raw.split("@")[0]
    phone = (
        phone or delta.get("number") or delta.get("phone") or root.get("number")
        or root.get("phone") or body.get("number") or body.get("phone")
        or body.get("contact_number") or to_raw or None
    )

    # ── Message body ─────────────────────────────────────────────────────────
    msg_obj = _first_dict(root.get("message"), delta.get("message"))
    if isinstance(msg_obj, dict):
        body_text = (msg_obj.get("body") or msg_obj.get("text") or msg_obj.get("content")
                     or f"[{msg_obj.get('type', 'message')}]")
    else:
        body_text = str(msg_obj) if msg_obj else "[outbound]"
    body_text = (
        delta.get("body") or delta.get("text") or delta.get("content")
        or body_text or root.get("body") or root.get("text") or root.get("content")
        or body.get("body") or body.get("text") or body.get("content") or "[outbound]"
    )

    # ── Message ID (for dedup) ───────────────────────────────────────────────
    if isinstance(msg_obj, dict):
        msg_id = msg_obj.get("id") or msg_obj.get("wamid")
    else:
        msg_id = None
    msg_id = (
        msg_id or delta.get("message_id") or delta.get("messageId") or delta.get("id")
        or root.get("message_id") or root.get("messageId") or root.get("id")
        or body.get("message_id") or body.get("id") or body.get("wamid")
    )
    # Generate a synthetic stable ID if none provided
    if not msg_id and phone:
        ts_raw = (
            delta.get("timestamp") or root.get("timestamp") or body.get("timestamp")
            or body.get("createdAt") or body.get("created_at")
        )
        msg_id = f"mc_out_{phone}_{ts_raw or now_iso}_{str(body_text)[:64]}"

    # ── Timestamp ────────────────────────────────────────────────────────────
    ts_raw = (
        delta.get("timestamp") or delta.get("createdAt") or delta.get("created_at")
        or delta.get("time") or delta.get("sentAt") or root.get("timestamp")
        or body.get("timestamp") or body.get("createdAt") or body.get("created_at")
        or body.get("time") or body.get("sentAt")
    )
    if isinstance(msg_obj, dict):
        ts_raw = ts_raw or msg_obj.get("timestamp") or msg_obj.get("createdAt")
    sent_at = _ts_to_iso(ts_raw, now_iso)

    return phone, agent_name, body_text, sent_at, msg_id


async def _handle_meta_format(body: dict, now_iso: str) -> None:
    """Process Meta WhatsApp Cloud API webhook payload."""
    for entry in (body.get("entry") or []):
        for change in (entry.get("changes") or []):
            value = change.get("value") or {}

            statuses = value.get("statuses")
            if statuses:
                for s in statuses:
                    log.info(f"[webhook/mc] status | id={s.get('id')!r} status={s.get('status')!r}")
                    msg_id = s.get("id")
                    phone = s.get("recipient_id")
                    sent_at = _ts_to_iso(s.get("timestamp"), now_iso)
                    status = s.get("status")
                    if not (msg_id and phone):
                        continue

                    existing_lead = _find_whatsapp_lead(None, phone)
                    lead_row = {
                        "phone": phone,
                        "channel": "whatsapp",
                        "last_message_at": sent_at,
                        "updated_at": now_iso,
                    }
                    if existing_lead:
                        supabase.table("leads").update(lead_row).eq("id", existing_lead["id"]).execute()
                        lead_id = existing_lead["id"]
                    else:
                        lead_result = supabase.table("leads").upsert({
                            "wa_contact_id": phone,
                            **lead_row,
                        }, on_conflict="wa_contact_id").execute()
                        lead_id = lead_result.data[0]["id"] if lead_result.data else None
                    if not lead_id:
                        continue

                    existing = (
                        supabase.table("messages")
                        .select("id,body")
                        .eq("wa_message_id", msg_id)
                        .limit(1)
                        .execute()
                        .data or []
                    )
                    if existing:
                        supabase.table("messages").update({
                            "status": status,
                        }).eq("wa_message_id", msg_id).execute()
                    else:
                        body_rows = _nearby_outbound_rows(lead_id, sent_at, placeholder_only=False)
                        if body_rows:
                            supabase.table("messages").update({
                                "status": status,
                            }).eq("id", body_rows[0]["id"]).execute()
                            continue
                        supabase.table("messages").insert({
                            "wa_message_id": msg_id,
                            "lead_id": lead_id,
                            "direction": "outbound",
                            "body": OUTBOUND_STATUS_PLACEHOLDER,
                            "status": status,
                            "sent_at": sent_at,
                        }).execute()
                continue

            messages = value.get("messages")
            if not messages:
                continue

            contacts_meta = value.get("contacts") or []
            contact_profile = contacts_meta[0] if contacts_meta else {}

            for msg in messages:
                msg_id = msg.get("id")
                phone = msg.get("from", "")
                name = contact_profile.get("profile", {}).get("name")
                msg_type = (msg.get("type") or "text").lower()

                if msg_type == "text":
                    body_text = (msg.get("text") or {}).get("body") or "[text]"
                elif msg_type in ("image", "audio", "video", "document", "sticker"):
                    body_text = f"[{msg_type}]"
                else:
                    body_text = f"[{msg_type}]"

                sent_at = _ts_to_iso(msg.get("timestamp"), now_iso)

                log.info(
                    f"[webhook/mc] inbound | from={phone!r} msg_id={msg_id!r} "
                    f"type={msg_type!r} body={body_text[:80]!r}"
                )

                if not phone:
                    continue

                lead_result = supabase.table("leads").upsert({
                    "wa_contact_id": phone,
                    "phone": phone,
                    "name": name or None,
                    "channel": "whatsapp",
                    "last_message_at": sent_at,
                    "updated_at": now_iso,
                }, on_conflict="wa_contact_id").execute()
                lead_id = lead_result.data[0]["id"] if lead_result.data else None

                if msg_id and lead_id:
                    if _message_duplicate_exists(lead_id, phone, "inbound", body_text, sent_at):
                        log.info(
                            f"[webhook/mc] inbound duplicate skipped | from={phone!r} "
                            f"msg_id={msg_id!r} sent_at={sent_at!r}"
                        )
                        continue

                    supabase.table("messages").upsert({
                        "wa_message_id": msg_id,
                        "lead_id": lead_id,
                        "direction": "inbound",
                        "body": body_text,
                        "sent_at": sent_at,
                    }, on_conflict="wa_message_id").execute()

                    try:
                        alert_engine.check_no_reply()
                    except Exception as e:
                        log.error(f"no_reply check error: {e}")


async def _handle_mc_contact_created(body: dict, now_iso: str) -> None:
    """ManyContacts native event: new contact created.

    Payload: {'event': 'contact_created', 'contact': {id, name, number, last_user_id, open, ...}}
    Upserts the lead immediately — faster than waiting for the 2h polling cycle.
    """
    contact = body.get("contact") or {}
    mc_id = contact.get("id")
    phone = contact.get("number") or contact.get("phone")
    name = contact.get("name")
    last_user_id = contact.get("last_user_id")
    agent_name = whatsapp.resolve_agent_name(last_user_id) if last_user_id else None
    open_status = contact.get("open", 1)
    status = "engaged" if open_status == 1 else "lost"

    if not phone:
        log.warning(f"[webhook/mc] contact_created: no phone — contact={contact}")
        return

    supabase.table("leads").upsert({
        "wa_contact_id": mc_id or phone,
        "phone": phone,
        "name": name or None,
        "channel": "whatsapp",
        "status": status,
        **({"assigned_agent": agent_name} if agent_name else {}),
        "updated_at": now_iso,
    }, on_conflict="wa_contact_id").execute()

    log.info(f"[webhook/mc] contact_created | phone={phone!r} name={name!r} agent={agent_name!r}")


async def _handle_mc_message_new(body: dict, now_iso: str) -> None:
    """ManyContacts native event: a new conversation message.

    Documented shape:
    {
      "event": "message_new",
      "delta": {
        "contactId": "...",
        "lastUserId": "...",
        "message": {"id": "...", "text": "...", "type": "received", "metadata": {"time": "..."}}
      },
      "contact": {"id": "...", "number": "...", "last_user_id": "..."}
    }

    If ManyContacts sends outbound replies with type=sent/outbound/etc., this
    stores the full agent body, agent name, and timestamp.
    """
    root = _first_dict(body.get("data"), body.get("payload"), body)
    delta = _first_dict(root.get("delta"), root.get("eventData"), root.get("data"))
    message = _first_list_item(
        delta.get("message") or root.get("message") or root.get("messages")
    )
    message = _first_dict(message)
    contact = _first_dict(
        root.get("contact"),
        delta.get("contact"),
        message.get("contact"),
        root.get("conversation"),
        delta.get("conversation"),
        root.get("chat"),
        delta.get("chat"),
    )

    if not isinstance(delta, dict) or not isinstance(contact, dict) or not isinstance(message, dict):
        log.warning(f"[webhook/mc] message_new malformed — preview={str(body)[:500]}")
        return

    contact_id = (
        delta.get("contactId") or delta.get("contact_id")
        or root.get("contactId") or root.get("contact_id")
        or contact.get("id")
    )
    phone = (
        contact.get("number") or contact.get("phone") or contact.get("wa_id")
        or message.get("from") or message.get("to")
        or root.get("number") or root.get("phone")
    )
    if isinstance(phone, str) and "@" in phone:
        phone = phone.split("@")[0]
    name = contact.get("name")
    msg_id = message.get("id") or message.get("wamid")
    msg_type = (
        message.get("type") or message.get("direction")
        or delta.get("type") or delta.get("direction")
        or root.get("type") or root.get("direction")
    )
    direction = _mc_message_direction(msg_type)
    if not direction and (message.get("fromMe") is True or message.get("from_me") is True):
        direction = "outbound"
    if not direction and (message.get("fromMe") is False or message.get("from_me") is False):
        direction = "inbound"
    metadata = message.get("metadata") or {}
    sent_at = (
        _parse_mc_ts(metadata.get("time") or message.get("createdAt") or message.get("created_at")
                     or message.get("timestamp") or message.get("time")
                     or delta.get("lastInbound") or delta.get("timestamp") or root.get("timestamp"))
        or now_iso
    )

    text_obj = message.get("text")
    if isinstance(text_obj, dict):
        text_obj = text_obj.get("body") or text_obj.get("text")
    body_text = (
        text_obj or message.get("body") or message.get("content")
        or message.get("caption") or root.get("body") or root.get("text")
        or f"[{msg_type or 'message'}]"
    )

    if not direction:
        log.warning(
            f"[webhook/mc] message_new unknown type={msg_type!r} "
            f"contact={contact_id or phone!r} msg_id={msg_id!r} preview={str(message)[:300]}"
        )
        return

    if not (phone or contact_id):
        log.warning(f"[webhook/mc] message_new missing contact — keys={list(body.keys())}")
        return

    agent_obj = _first_dict(root.get("agent"), root.get("user"), message.get("user"), message.get("sender"))
    agent_id = (
        delta.get("lastUserId") or delta.get("last_user_id")
        or contact.get("last_user_id") or message.get("user_id")
        or agent_obj.get("id")
    )
    agent_name = (
        agent_obj.get("name") or agent_obj.get("fullName") or agent_obj.get("displayName")
        or (whatsapp.resolve_agent_name(agent_id) if agent_id else None)
    )

    lead_key = contact_id or phone
    lead_row = {
        "phone": phone,
        "name": name or None,
        "channel": "whatsapp",
        "last_message_at": sent_at,
        "updated_at": now_iso,
        **({"assigned_agent": agent_name} if agent_name else {}),
    }

    existing_lead = _find_whatsapp_lead(contact_id, phone)
    if existing_lead:
        lead_result = (
            supabase.table("leads")
            .update({k: v for k, v in lead_row.items() if v is not None})
            .eq("id", existing_lead["id"])
            .execute()
        )
        lead_id = existing_lead["id"]
    else:
        lead_result = supabase.table("leads").upsert({
            "wa_contact_id": lead_key,
            **lead_row,
        }, on_conflict="wa_contact_id").execute()
        lead_id = lead_result.data[0]["id"] if lead_result.data else None

    if not lead_id:
        return

    if not msg_id:
        msg_id = f"mc_{lead_key}_{sent_at}_{direction}"

    row = {
        "wa_message_id": str(msg_id),
        "lead_id": lead_id,
        "direction": direction,
        "body": str(body_text)[:2000],
        "sent_at": sent_at,
    }
    if direction == "outbound" and agent_name:
        row["agent_name"] = agent_name

    if _message_duplicate_exists(lead_id, phone, direction, body_text, sent_at):
        log.info(
            f"[webhook/mc] message_new duplicate skipped | direction={direction!r} "
            f"phone={phone!r} msg_id={msg_id!r} sent_at={sent_at!r}"
        )
        return

    supabase.table("messages").upsert(row, on_conflict="wa_message_id").execute()

    log.info(
        f"[webhook/mc] message_new | direction={direction!r} type={msg_type!r} "
        f"phone={phone!r} agent={agent_name!r} msg_id={msg_id!r} body={str(body_text)[:80]!r}"
    )

    if direction == "inbound":
        try:
            alert_engine.check_no_reply()
        except Exception as e:
            log.error(f"no_reply check error: {e}")


async def _handle_mc_outbound(body: dict, now_iso: str) -> None:
    """Process ManyContacts native outbound (agent-reply) webhook payload."""
    phone, agent_name, body_text, sent_at, msg_id = _extract_mc_outbound(body)

    log.info(
        f"[webhook/mc] outbound | phone={phone!r} agent={agent_name!r} "
        f"msg_id={msg_id!r} body={body_text[:80]!r}"
    )

    if not phone:
        log.warning(f"[webhook/mc] outbound: no phone found — keys={list(body.keys())}")
        return

    # Upsert lead (may already exist from inbound messages)
    lead_result = supabase.table("leads").upsert({
        "wa_contact_id": phone,
        "phone": phone,
        "channel": "whatsapp",
        "last_message_at": sent_at,
        "updated_at": now_iso,
        **({"assigned_agent": agent_name} if agent_name else {}),
    }, on_conflict="wa_contact_id").execute()
    lead_id = lead_result.data[0]["id"] if lead_result.data else None

    if not (msg_id and lead_id):
        return

    placeholder_rows = _nearby_outbound_rows(lead_id, sent_at, placeholder_only=True)
    if placeholder_rows:
        update_row = {
            "body": body_text,
            "sent_at": sent_at,
            **({"agent_name": agent_name} if agent_name else {}),
        }
        supabase.table("messages").update(update_row).eq("id", placeholder_rows[0]["id"]).execute()
    else:
        # Store outbound message
        supabase.table("messages").upsert({
            "wa_message_id": msg_id,
            "lead_id": lead_id,
            "direction": "outbound",
            "body": body_text,
            "sent_at": sent_at,
            **({"agent_name": agent_name} if agent_name else {}),
        }, on_conflict="wa_message_id").execute()

    # Compute and log response time (time since last inbound from this lead)
    try:
        last_in = (
            supabase.table("messages")
            .select("sent_at")
            .eq("lead_id", lead_id)
            .eq("direction", "inbound")
            .order("sent_at", desc=True)
            .limit(1)
            .execute()
        ).data
        if last_in:
            last_in_dt = datetime.fromisoformat(last_in[0]["sent_at"].replace("Z", "+00:00"))
            out_dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
            gap_min = round((out_dt - last_in_dt).total_seconds() / 60, 1)
            log.info(f"[webhook/mc] response_time={gap_min}m for lead {lead_id} by {agent_name!r}")
    except Exception as e:
        log.warning(f"[webhook/mc] response_time calc failed: {e}")


@app.post("/webhook/manycontacts")
@app.post("/webhook/whatsapp")
@app.post("/webhook/manycontacts")
@app.post("/webhook/meta")
@app.post("/webhook/wa")
@app.post("/webhook/manycontacts/message_new")
@app.post("/webhook/manycontacts/messages")
async def webhook_manycontacts(request: Request):
    raw = await request.body()
    ct = request.headers.get("content-type", "")
    log.info(f"[webhook/mc] POST {request.url.path} | size={len(raw)} | ct={ct!r}")

    try:
        body = json.loads(raw)
    except Exception:
        log.warning(f"[webhook/mc] non-JSON body: {raw[:500]!r}")
        _last_webhook_payloads.append({"at": datetime.now(timezone.utc).isoformat(), "raw": raw[:1000].decode("utf-8", "replace"), "parsed": False})
        if len(_last_webhook_payloads) > 5:
            _last_webhook_payloads.pop(0)
        return {"status": "ok"}

    # Store last 5 payloads for debugging
    _last_webhook_payloads.append({
        "at": datetime.now(timezone.utc).isoformat(),
        "path": request.url.path,
        "event": body.get("event"),
        "keys": list(body.keys()),
        "preview": str(body)[:500],
    })
    if len(_last_webhook_payloads) > 5:
        _last_webhook_payloads.pop(0)

    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        if "entry" in body:
            # Meta WhatsApp Cloud API — inbound messages + delivery statuses
            await _handle_meta_format(body, now_iso)
        elif body.get("event") == "contact_created":
            # ManyContacts native: new contact/lead notification
            await _handle_mc_contact_created(body, now_iso)
        elif body.get("event") == "message_new":
            # ManyContacts native: full message event, including outbound if enabled.
            await _handle_mc_message_new(body, now_iso)
        elif body.get("event") == "message_sent":
            # ManyContacts native: outbound agent reply with body/user/timestamp.
            await _handle_mc_outbound(body, now_iso)
        elif _is_outbound_mc(body):
            # ManyContacts native outbound (agent reply) — if ever enabled
            await _handle_mc_outbound(body, now_iso)
        else:
            log.info(
                f"[webhook/mc] unrecognised format — event={body.get('event')!r} "
                f"keys={list(body.keys())} preview={str(body)[:200]}"
            )
    except Exception as e:
        log.error(f"[webhook/mc] handler error: {e}", exc_info=True)

    asyncio.create_task(_broadcast("data_updated", {"source": "webhook"}))
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Utility endpoints
# ---------------------------------------------------------------------------

@app.get("/ping")
def ping():
    return {"status": "alive"}


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/status")
def pipeline_status():
    pipeline_job  = scheduler.get_job("pipeline")
    maqsam_job    = scheduler.get_job("maqsam_realtime")
    whatsapp_job  = scheduler.get_job("whatsapp_activity")
    wa_msg_job    = scheduler.get_job("whatsapp_messages")
    ai_job        = scheduler.get_job("ai_analysis")
    base_url      = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000").rstrip("/")
    return {
        "last_pipeline_run": _last_pipeline_run.isoformat() if _last_pipeline_run else None,
        "last_maqsam_sync":  _last_maqsam_sync.isoformat()  if _last_maqsam_sync  else None,
        "next_pipeline_run": (
            pipeline_job.next_run_time.isoformat()
            if pipeline_job and pipeline_job.next_run_time else None
        ),
        "next_maqsam_sync": (
            maqsam_job.next_run_time.isoformat()
            if maqsam_job and maqsam_job.next_run_time else None
        ),
        "next_whatsapp_message_sync": (
            wa_msg_job.next_run_time.isoformat()
            if wa_msg_job and wa_msg_job.next_run_time else None
        ),
        "next_whatsapp_activity_sync": (
            whatsapp_job.next_run_time.isoformat()
            if whatsapp_job and whatsapp_job.next_run_time else None
        ),
        "next_ai_analysis": (
            ai_job.next_run_time.isoformat()
            if ai_job and ai_job.next_run_time else None
        ),
        "webhook_url": f"{base_url}/webhook/manycontacts",
        "accepted_webhook_urls": [
            f"{base_url}/webhook/manycontacts",
            f"{base_url}/webhook/whatsapp",
            f"{base_url}/webhook/meta",
            f"{base_url}/webhook/wa",
            f"{base_url}/webhook/manycontacts/message_new",
            f"{base_url}/webhook/manycontacts/messages",
            f"{base_url}/webhook/n8n/messages",
        ],
    }


@app.get("/api/debug/webhooks")
def debug_webhooks():
    """Return the last 5 raw webhook payloads received — use to diagnose format issues."""
    return {"count": len(_last_webhook_payloads), "payloads": _last_webhook_payloads}


@app.post("/api/debug/email/supervisor-report")
def debug_send_supervisor_report(token: str = Query("")):
    _require_email_debug_token(token)
    sent = email_notifications.send_supervisor_report("manual test")
    return {"sent": sent}


@app.post("/api/debug/email/agent-alert")
def debug_send_agent_alert(agent: str = Query("Jehad Qasim"), token: str = Query("")):
    _require_email_debug_token(token)
    sent = email_notifications.notify_agent_alert({
        "lead_id": "test",
        "agent_name": agent,
        "severity": "HIGH",
        "type": "test_alert",
        "message": "Test alert notification from Fiper Analytics Dashboard.",
    })
    return {"agent": agent, "sent": sent}


@app.get("/api/debug/mc-probe")
async def debug_mc_probe(contact_id: str = Query(None)):
    """
    Probe ManyContacts API for a given contact ID (or the first contact if omitted).
    Returns raw HTTP responses from all known message URL patterns — use to discover
    which endpoint actually works.
    """
    cid = contact_id
    if not cid:
        try:
            contacts = await whatsapp.fetch_contacts()
            if contacts:
                cid = str(contacts[0].get("id") or contacts[0].get("number") or "")
        except Exception as e:
            return {"error": f"fetch_contacts failed: {e}"}
    if not cid:
        return {"error": "no contact_id and no contacts found"}
    result = await whatsapp.probe_contact(cid)
    return result


@app.post("/api/sync/whatsapp")
async def manual_whatsapp_sync(days_back: int = Query(7, ge=1, le=90)):
    """
    Manually trigger a full WhatsApp message backfill for the last N days.
    Runs synchronously and returns stats — may take up to a minute for large accounts.
    """
    log.info(f"[sync/whatsapp] manual trigger days_back={days_back}")
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")
    msg_cutoff = (now - timedelta(days=days_back)).isoformat()
    now_iso = now.isoformat()

    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.warning(f"[sync/whatsapp] fetch_users failed: {e}")

    try:
        contacts = await whatsapp.fetch_contacts(date_from=date_from, date_to=date_to)
    except Exception as e:
        return {"error": str(e), "contacts": 0, "messages_saved": 0}

    msg_total = 0
    msg_inbound = 0
    msg_outbound = 0
    contacts_with_messages = 0

    for contact in contacts:
        mc_id = contact.get("id")
        phone = contact.get("number", "")
        name = contact.get("name")
        last_user_id = contact.get("last_user_id")
        agent_name = whatsapp.resolve_agent_name(last_user_id)
        updated_at = contact.get("updatedAt")
        open_status = contact.get("open", 1)

        lead_res = supabase.table("leads").upsert({
            "wa_contact_id": mc_id,
            "phone": phone,
            "name": name,
            "channel": "whatsapp",
            "status": "engaged" if open_status == 1 else "lost",
            "assigned_agent": agent_name,
            "last_message_at": updated_at,
            "updated_at": now_iso,
        }, on_conflict="wa_contact_id").execute()
        lead_id = lead_res.data[0]["id"] if lead_res.data else None
        if not (lead_id and mc_id):
            continue

        raw_msgs = await whatsapp.fetch_contact_messages(mc_id, phone=phone)
        if not raw_msgs:
            continue
        contacts_with_messages += 1
        msg_rows = []
        for msg in raw_msgs:
            sent_at = _parse_mc_ts(
                msg.get("timestamp") or msg.get("createdAt") or msg.get("created_at")
            )
            if not sent_at or sent_at < msg_cutoff:
                continue
            direction = (
                "outbound"
                if (msg.get("type") or msg.get("direction") or "").upper() in ("OUTBOUND", "OUT")
                else "inbound"
            )
            body_text = (
                msg.get("text") or msg.get("message") or msg.get("body")
                or msg.get("content") or f"[{direction}]"
            )
            msg_id = str(msg.get("id") or msg.get("wamid") or "")
            if not msg_id:
                continue
            row = {"wa_message_id": msg_id, "lead_id": lead_id, "direction": direction,
                   "body": str(body_text)[:2000], "sent_at": sent_at}
            if direction == "outbound":
                row["agent_name"] = whatsapp.resolve_agent_name(msg.get("user_id")) or agent_name
                msg_outbound += 1
            else:
                msg_inbound += 1
            msg_rows.append(row)
            msg_total += 1

        for i in range(0, len(msg_rows), 100):
            supabase.table("messages").upsert(
                msg_rows[i:i + 100], on_conflict="wa_message_id"
            ).execute()

    await _broadcast("data_updated", {"source": "whatsapp_manual_sync"})
    return {
        "days_back": days_back,
        "contacts_scanned": len(contacts),
        "contacts_with_messages": contacts_with_messages,
        "messages_saved": msg_total,
        "inbound": msg_inbound,
        "outbound": msg_outbound,
    }


@app.post("/api/run-pipeline")
@app.post("/api/trigger-pipeline")
async def trigger_pipeline():
    """Manually trigger the full pipeline (ingest → AI analysis → alerts)."""
    asyncio.create_task(run_pipeline())
    return {"status": "pipeline triggered", "steps": ["ingest_manycontacts", "ingest_maqsam", "run_ai_analysis", "alert_engine"]}


@app.post("/api/trigger-ingest-maqsam")
async def trigger_ingest_maqsam(
    days_back: int = Query(1, ge=1, le=7),
    overlap_minutes: int = Query(45, ge=0, le=240),
):
    """Run the Maqsam sync now and return counts."""
    stats = await ingest_maqsam(days_back=days_back, overlap_minutes=overlap_minutes)
    global _last_maqsam_sync
    _last_maqsam_sync = datetime.now(timezone.utc)
    await _broadcast("data_updated", {"source": "maqsam_manual", **stats})
    return {"status": "done", **stats}


@app.post("/api/trigger-ai-analysis")
async def trigger_ai_analysis(hours_back: int = Query(24, ge=1, le=72)):
    """Run AI analysis now for recent leads with transcripts/messages."""
    stats = await run_ai_analysis(hours_back=hours_back)
    await _broadcast("data_updated", {"source": "ai_analysis_manual", "hours_back": hours_back, **stats})
    return {"status": "done", "hours_back": hours_back, **stats}


@app.get("/api/stream")
async def sse_stream(request: Request):
    """Server-Sent Events — pushes data_updated to all open dashboard tabs."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=20)
    _sse_clients.add(queue)

    async def generator() -> AsyncGenerator[str, None]:
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=25)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": ping\n\n"  # keepalive — prevents proxy/Render timeout
        finally:
            _sse_clients.discard(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.post("/api/trigger-ingest-mc")
async def trigger_ingest_mc(hours_back: int = Query(48)):
    """Run ManyContacts ingestion right now and return message counts.

    hours_back: how far back to fetch contacts (default 48h).
    Message cutoff is 4× hours_back (min 48h) so recent conversations are fully captured.
    """
    stats = await _ingest_mc_with_stats(hours_back)
    return {"status": "done", **stats}


# ---------------------------------------------------------------------------
# n8n push endpoint — receives conversation batches from n8n workflow
# ---------------------------------------------------------------------------

@app.post("/webhook/n8n/messages")
async def n8n_messages(request: Request):
    """Receive a conversation batch POSTed by n8n every 2 hours.

    Expected payload (one contact per request):
    {
      "contact_id": "mc-uuid",          // ManyContacts contact ID
      "phone":      "96897245526",
      "name":       "Ahmed Al-Sayed",   // optional
      "agent":      "Feras Zabalawi",   // assigned agent, optional
      "messages": [
        {
          "id":         "msg-123",
          "direction":  "outbound",      // "inbound" | "outbound"
          "body":       "Hello, how can I help?",
          "timestamp":  1778774807,      // unix seconds OR ISO string
          "agent_name": "Feras Zabalawi" // outbound only
        }
      ]
    }

    n8n can also POST an array of such objects in one call — both shapes accepted.
    """
    raw = await request.body()
    try:
        payload = json.loads(raw)
    except Exception:
        log.warning(f"[n8n] non-JSON body: {raw[:200]!r}")
        return {"status": "error", "detail": "invalid JSON"}

    # Accept both a single object and a list
    items = payload if isinstance(payload, list) else [payload]
    now_iso = datetime.now(timezone.utc).isoformat()

    contacts_seen = 0
    msgs_saved = 0
    msgs_outbound = 0

    for item in items:
        contact_id = item.get("contact_id") or item.get("id")
        phone      = str(item.get("phone") or item.get("number") or "").strip()
        name       = item.get("name")
        agent      = item.get("agent") or item.get("assigned_agent")
        messages   = item.get("messages") or []

        if not phone and not contact_id:
            log.warning(f"[n8n] skipping item — no phone or contact_id: {list(item.keys())}")
            continue

        # Upsert lead
        lead_row = {
            "phone":    phone,
            "channel":  "whatsapp",
            "updated_at": now_iso,
        }
        if contact_id:
            lead_row["wa_contact_id"] = contact_id
        if name:
            lead_row["name"] = name
        if agent:
            lead_row["assigned_agent"] = agent

        conflict_col = "wa_contact_id" if contact_id else "phone"
        lead_res = supabase.table("leads").upsert(
            lead_row, on_conflict=conflict_col
        ).execute()
        lead_id = lead_res.data[0]["id"] if lead_res.data else None
        if not lead_id:
            continue
        contacts_seen += 1

        # Upsert messages
        msg_rows = []
        for msg in messages:
            msg_id    = str(msg.get("id") or msg.get("wamid") or "")
            direction = (msg.get("direction") or msg.get("type") or "inbound").lower()
            direction = "outbound" if direction in ("outbound", "out", "agent") else "inbound"
            body_text = (msg.get("body") or msg.get("text") or msg.get("message")
                         or msg.get("content") or f"[{direction}]")
            msg_ts    = _parse_mc_ts(
                msg.get("timestamp") or msg.get("createdAt") or msg.get("sent_at")
            ) or now_iso
            msg_agent = (msg.get("agent_name") or msg.get("agent") or
                         (agent if direction == "outbound" else None))

            if not msg_id:
                # Stable synthetic ID so re-runs are idempotent
                msg_id = f"n8n_{phone}_{msg_ts}"

            row = {
                "wa_message_id": msg_id,
                "lead_id":       lead_id,
                "direction":     direction,
                "body":          str(body_text)[:2000],
                "sent_at":       msg_ts,
            }
            if msg_agent:
                row["agent_name"] = msg_agent
            msg_rows.append(row)
            msgs_saved += 1
            if direction == "outbound":
                msgs_outbound += 1

        for i in range(0, len(msg_rows), 100):
            supabase.table("messages").upsert(
                msg_rows[i:i + 100], on_conflict="wa_message_id"
            ).execute()

    log.info(
        f"[n8n] contacts={contacts_seen} messages={msgs_saved} outbound={msgs_outbound}"
    )
    return {
        "status":          "ok",
        "contacts_saved":  contacts_seen,
        "messages_saved":  msgs_saved,
        "outbound_saved":  msgs_outbound,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
