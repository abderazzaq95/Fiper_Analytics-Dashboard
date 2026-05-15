import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from typing import AsyncGenerator
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client
from dotenv import load_dotenv

from pipeline import whatsapp, maqsam, ai_analyzer, alert_engine
from api import overview, agents, leads, channels, quality, journey

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fiper")

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
WA_VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN", "")
scheduler = AsyncIOScheduler()

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

_ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
_CLAUDE_MODEL  = "claude-haiku-4-5-20251001"

_AI_SYSTEM_PROMPT = """You are a quality assurance analyst for Fiper, a trading broker in Arabic-speaking markets.
Analyze the call transcript(s) between Fiper agents and leads.
Maqsam has already determined the sentiment — do NOT re-evaluate it; it is passed to you separately.

Respond ONLY in valid JSON. No explanation, no markdown, no extra text.

{
  "score": 0-100,
  "topics": [],
  "outcome": "converted"|"callback"|"not_interested"|"no_answer"|"ongoing",
  "follow_up_needed": true|false,
  "risk_flags": [],
  "treatment_score": 0-100,
  "summary": "Max 2 sentences. Arabic if the conversation is Arabic, English if English."
}

topics: pricing|product_fit|competitor|technical|follow_up|not_decision_maker|trading_education|account_info|greetings|profit_expectations
risk_flags: unanswered|profit_expectations|beginner_risk|stale_callback|negative_sentiment|slow_response
treatment_score: 90-100 excellent, 70-89 good, 50-69 average, 30-49 poor, 0-29 very poor"""


# ---------------------------------------------------------------------------
# Ingestion pipeline
# ---------------------------------------------------------------------------

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
        raw_msgs = await whatsapp.fetch_contact_messages(mc_id)
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


async def ingest_maqsam(days_back: int = 3):
    """Fetch Maqsam calls for the last N days. Defaults to 3 to cover timezone gaps
    between pipeline runs and to not miss calls from the previous calendar day."""
    log.info("Maqsam ingestion started")
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    try:
        calls = await maqsam.fetch_calls(date_from, date_to)
    except Exception as e:
        log.error(f"Maqsam fetch_calls failed: {e}")
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    # Batch-upsert unique leads first, then calls
    unique_phones: dict[str, None] = {}
    for call in calls:
        direction = call.get("direction", "outbound")
        phone = call.get("calleeNumber") if direction == "outbound" else call.get("callerNumber")
        if phone:
            unique_phones[phone] = None

    if unique_phones:
        lead_rows = [
            {"wa_contact_id": p, "phone": p, "channel": "maqsam", "updated_at": now_iso}
            for p in unique_phones
        ]
        supabase.table("leads").upsert(lead_rows, on_conflict="wa_contact_id").execute()

    # Build phone → lead_id map
    phone_to_lead: dict[str, str] = {}
    if unique_phones:
        result = (
            supabase.table("leads")
            .select("id,wa_contact_id")
            .in_("wa_contact_id", list(unique_phones.keys()))
            .execute()
        )
        phone_to_lead = {r["wa_contact_id"]: r["id"] for r in (result.data or [])}

    # Batch-upsert calls in chunks of 200
    call_rows = []
    for call in calls:
        direction = call.get("direction", "outbound")
        phone = call.get("calleeNumber") if direction == "outbound" else call.get("callerNumber")
        ts = call.get("timestamp")
        called_at = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat() if ts else None

        # Maqsam built-in AI fields
        summary_obj = call.get("summary") or {}
        summary_en  = summary_obj.get("en") or ""
        summary_ar  = summary_obj.get("ar") or ""
        mq_sentiment = call.get("sentiment")
        auto_tags    = call.get("callAutoTags") or []
        # Build formatted transcript text from callTranscription segments
        raw_transcript = call.get("callTranscription") or []
        transcript_lines = []
        for seg in raw_transcript:
            party   = "Agent" if seg.get("party") == "agent" else "Customer"
            content = (seg.get("content") or "").strip()
            if content:
                transcript_lines.append(f"{party}: {content}")
        transcript_text = "\n".join(transcript_lines)

        row = {
            "maqsam_id":        str(call.get("id")),
            "lead_id":          phone_to_lead.get(phone or ""),
            "agent_name":       maqsam.extract_agent_name(call),
            "duration_seconds": call.get("duration"),
            "outcome":          call.get("state"),
            "called_at":        called_at,
        }
        # Store Maqsam AI data when present (new columns added via migration)
        if transcript_text:
            row["transcript"] = transcript_text
        if summary_en:
            row["summary_en"] = summary_en
        if summary_ar:
            row["summary_ar"] = summary_ar
        if mq_sentiment:
            row["maqsam_sentiment"] = mq_sentiment
        if auto_tags:
            row["auto_tags"] = auto_tags

        call_rows.append(row)

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

    log.info(f"Maqsam ingestion done — {len(call_rows)} calls")


async def _call_claude_async(user_message: str) -> dict:
    """Call Claude Haiku via async httpx. Returns parsed JSON result dict."""
    import httpx as _httpx
    import json as _json

    payload = {
        "model": _CLAUDE_MODEL,
        "max_tokens": 512,
        "system": _AI_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_message}],
    }
    async with _httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": _ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            content=_json.dumps(payload),
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()

    try:
        return _json.loads(raw)
    except _json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}") + 1
        if start != -1:
            return _json.loads(raw[start:end])
        return {"score": 0, "topics": [], "outcome": "ongoing", "follow_up_needed": False,
                "risk_flags": [], "treatment_score": 50, "summary": "Parse error."}


async def run_ai_analysis():
    """Analyze leads that have no ai_analysis in the last 6 hours.

    Maqsam leads: uses transcript + maqsam_sentiment from calls table when available.
    WhatsApp leads: uses message bodies as conversation text.
    Skips leads with no transcript/messages or whose calls are all 0s duration.
    """
    from collections import defaultdict

    now = datetime.now(timezone.utc)
    # 3h window (1h buffer over the 2h schedule) to avoid edge-case gaps
    cutoff_iso = (now - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")

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
        log.info("AI analysis: no new leads to analyze")
        return

    log.info(f"AI analysis: {len(to_analyze)} leads to analyze")

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
            result = await _call_claude_async(user_msg)
            if mq_sentiment:
                result["sentiment"] = mq_sentiment

            # Upsert: update if exists, insert if not
            existing = (
                supabase.table("ai_analysis")
                .select("id")
                .eq("lead_id", lead_id)
                .execute().data or []
            )
            if existing:
                supabase.table("ai_analysis").update(result).eq("lead_id", lead_id).execute()
            else:
                supabase.table("ai_analysis").insert(
                    {"lead_id": lead_id, "source": source, **result}
                ).execute()

            supabase.table("leads").update({"score": result.get("score")}).eq("id", lead_id).execute()
            ok += 1
            await asyncio.sleep(0.3)  # stay within Claude rate limits

        except Exception as e:
            log.error(f"AI analysis failed for lead {lead_id}: {e}")
            err += 1

    log.info(f"AI analysis done — {ok} analyzed, {err} errors, {skipped} skipped (no data)")


async def run_pipeline():
    """Full pipeline: ingest → AI analysis → alerts. Runs every 2 hours."""
    log.info("Pipeline started")
    await ingest_manycontacts()
    await ingest_maqsam()
    await run_ai_analysis()
    try:
        alert_engine.run_all_checks()
    except Exception as e:
        log.error(f"Alert engine error: {e}")
    log.info("Pipeline complete")
    await _broadcast("data_updated", {"source": "pipeline"})


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm agent cache on startup
    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.warning(f"Could not pre-load agent cache: {e}")

    scheduler.add_job(run_pipeline, "interval", hours=2, id="pipeline", replace_existing=True)
    scheduler.add_job(_ping_self, "interval", minutes=10, id="keepalive", replace_existing=True)
    scheduler.start()
    log.info("Scheduler started — pipeline every 2 h, keep-alive ping every 10 min")
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

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def dashboard():
    return FileResponse("static/index.html")


# ---------------------------------------------------------------------------
# ManyContacts webhook (real-time messages)
# ---------------------------------------------------------------------------

@app.get("/webhook/manycontacts")
async def webhook_manycontacts_verify():
    """Handles GET verification pings some webhook platforms send before activating."""
    log.info("[webhook/mc] GET verification hit")
    return {"status": "ok"}


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
    agent_obj = body.get("agent") or body.get("user") or body.get("sender") or {}
    if isinstance(agent_obj, dict):
        agent_name = (agent_obj.get("name") or agent_obj.get("fullName")
                      or agent_obj.get("displayName") or agent_obj.get("username")
                      or agent_obj.get("email"))
    else:
        agent_name = str(agent_obj) if agent_obj else None

    # ── Contact phone ────────────────────────────────────────────────────────
    contact_obj = body.get("contact") or body.get("chat") or {}
    if isinstance(contact_obj, dict):
        phone = (contact_obj.get("number") or contact_obj.get("phone")
                 or contact_obj.get("wa_id") or contact_obj.get("id")
                 or contact_obj.get("jid", "").split("@")[0])  # WhatsApp JID format
    else:
        phone = str(contact_obj) if contact_obj else None
    # Fallback: top-level fields, or strip @s.whatsapp.net from "to" field
    to_raw = body.get("to") or body.get("recipient") or ""
    if isinstance(to_raw, str):
        to_raw = to_raw.split("@")[0]
    phone = phone or body.get("number") or body.get("phone") or body.get("contact_number") or to_raw or None

    # ── Message body ─────────────────────────────────────────────────────────
    msg_obj = body.get("message") or {}
    if isinstance(msg_obj, dict):
        body_text = (msg_obj.get("body") or msg_obj.get("text") or msg_obj.get("content")
                     or f"[{msg_obj.get('type', 'message')}]")
    else:
        body_text = str(msg_obj) if msg_obj else "[outbound]"
    body_text = body_text or body.get("body") or body.get("text") or body.get("content") or "[outbound]"

    # ── Message ID (for dedup) ───────────────────────────────────────────────
    if isinstance(msg_obj, dict):
        msg_id = msg_obj.get("id") or msg_obj.get("wamid")
    else:
        msg_id = None
    msg_id = msg_id or body.get("message_id") or body.get("id") or body.get("wamid")
    # Generate a synthetic stable ID if none provided
    if not msg_id and phone:
        ts_raw = body.get("timestamp") or body.get("createdAt") or body.get("created_at")
        msg_id = f"mc_out_{phone}_{ts_raw or now_iso}"

    # ── Timestamp ────────────────────────────────────────────────────────────
    ts_raw = (body.get("timestamp") or body.get("createdAt") or body.get("created_at")
              or body.get("time") or body.get("sentAt"))
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
async def webhook_manycontacts(request: Request):
    raw = await request.body()
    ct = request.headers.get("content-type", "")
    log.info(f"[webhook/mc] POST | size={len(raw)} | ct={ct!r}")

    try:
        body = json.loads(raw)
    except Exception:
        log.warning(f"[webhook/mc] non-JSON body: {raw[:500]!r}")
        return {"status": "ok"}

    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        if "entry" in body:
            # Meta WhatsApp Cloud API — inbound messages + delivery statuses
            await _handle_meta_format(body, now_iso)
        elif body.get("event") == "contact_created":
            # ManyContacts native: new contact/lead notification
            await _handle_mc_contact_created(body, now_iso)
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
# Meta webhook (kept only for webhook verification handshake)
# ---------------------------------------------------------------------------

@app.get("/webhook/whatsapp")
async def webhook_meta_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == WA_VERIFY_TOKEN:
        return int(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification failed")


# ---------------------------------------------------------------------------
# Utility endpoints
# ---------------------------------------------------------------------------

@app.get("/ping")
def ping():
    return {"status": "alive"}


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/api/run-pipeline")
@app.post("/api/trigger-pipeline")
async def trigger_pipeline():
    """Manually trigger the full pipeline (ingest → AI analysis → alerts)."""
    asyncio.create_task(run_pipeline())
    return {"status": "pipeline triggered", "steps": ["ingest_manycontacts", "ingest_maqsam", "run_ai_analysis", "alert_engine"]}


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
