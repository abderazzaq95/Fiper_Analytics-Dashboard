import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from supabase import create_client
from dotenv import load_dotenv

from pipeline import whatsapp, maqsam, ai_analyzer, alert_engine
from api import overview, agents, leads, channels, quality

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fiper")

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
WA_VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN", "")
scheduler = AsyncIOScheduler()


# ---------------------------------------------------------------------------
# Ingestion pipeline
# ---------------------------------------------------------------------------

async def ingest_manycontacts(hours_back: int = 2):
    """Poll ManyContacts for contacts updated in the last N hours, including full message history."""
    log.info("ManyContacts ingestion started")
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(hours=hours_back)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    # Refresh agent name cache first
    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.error(f"fetch_users failed: {e}")

    try:
        contacts = await whatsapp.fetch_contacts(date_from=date_from, date_to=date_to)
    except Exception as e:
        log.error(f"ManyContacts fetch_contacts failed: {e}")
        return

    for contact in contacts:
        mc_id = contact.get("id")
        phone = contact.get("number", "")
        name = contact.get("name")
        open_status = contact.get("open", 1)
        last_user_id = contact.get("last_user_id")
        agent_name = whatsapp.resolve_agent_name(last_user_id)
        updated_at = contact.get("updatedAt")

        # Map ManyContacts open field to our status
        status = "engaged" if open_status == 1 else "lost"

        supabase.table("leads").upsert({
            "wa_contact_id": mc_id,
            "phone": phone,
            "name": name,
            "channel": "whatsapp",
            "status": status,
            "assigned_agent": agent_name,
            "last_message_at": updated_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="wa_contact_id").execute()

    log.info(f"ManyContacts ingestion done — {len(contacts)} contacts synced")


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
        call_rows.append({
            "maqsam_id": str(call.get("id")),
            "lead_id": phone_to_lead.get(phone or ""),
            "agent_name": maqsam.extract_agent_name(call),
            "duration_seconds": call.get("duration"),
            "outcome": call.get("state"),
            "recording_url": call.get("recording_url"),
            "called_at": called_at,
        })

    chunk_size = 200
    for i in range(0, len(call_rows), chunk_size):
        supabase.table("calls").upsert(call_rows[i:i + chunk_size], on_conflict="maqsam_id").execute()

    log.info(f"Maqsam ingestion done — {len(calls)} calls")


async def run_ai_on_new_leads():
    """Run Claude analysis on leads that have new data but no ai_analysis yet.

    WhatsApp leads: must have at least one message.
    Maqsam leads:  must have at least one call with duration > 0 (skip pure no-answers).
    Uses bulk fetches to avoid N+1 queries.
    """
    from collections import defaultdict

    # Bulk-fetch everything needed
    all_leads = supabase.table("leads").select("id,channel").execute().data or []
    analyzed_ids = {
        r["lead_id"]
        for r in (supabase.table("ai_analysis").select("lead_id").execute().data or [])
    }

    all_msgs_raw = supabase.table("messages").select("lead_id,direction,body,sent_at").execute().data or []
    msgs_by_lead: dict = defaultdict(list)
    for m in all_msgs_raw:
        msgs_by_lead[m["lead_id"]].append(m)

    all_calls_raw = supabase.table("calls").select("lead_id,duration_seconds,outcome,agent_name,called_at").execute().data or []
    calls_by_lead: dict = defaultdict(list)
    for c in all_calls_raw:
        if c.get("lead_id"):
            calls_by_lead[c["lead_id"]].append(c)

    for lead in all_leads:
        lead_id = lead["id"]
        channel = lead.get("channel", "")

        if lead_id in analyzed_ids:
            continue

        if channel == "whatsapp":
            msgs = sorted(msgs_by_lead.get(lead_id, []), key=lambda m: m.get("sent_at") or "")
            if not msgs:
                continue
            source = "whatsapp"
            data_for_claude = msgs

        elif channel == "maqsam":
            calls = calls_by_lead.get(lead_id, [])
            # Skip leads where every call was unanswered (duration = 0)
            if not any((c.get("duration_seconds") or 0) > 0 for c in calls):
                continue
            source = "maqsam"
            data_for_claude = [
                {
                    "direction": "outbound",
                    "body": f"Phone call — Duration: {c.get('duration_seconds') or 0}s, "
                            f"Outcome: {c.get('outcome') or 'unknown'}, "
                            f"Agent: {c.get('agent_name') or 'agent'}",
                    "sent_at": c.get("called_at") or "",
                }
                for c in sorted(calls, key=lambda c: c.get("called_at") or "")
            ]
        else:
            continue

        try:
            result = ai_analyzer.analyze_conversation(data_for_claude)
            supabase.table("ai_analysis").insert({
                "lead_id": lead_id,
                "source": source,
                **result,
            }).execute()
            supabase.table("leads").update({"score": result.get("score")}).eq("id", lead_id).execute()
        except Exception as e:
            log.error(f"AI analysis failed for lead {lead_id}: {e}")


async def run_pipeline():
    await asyncio.gather(ingest_manycontacts(), ingest_maqsam())
    await run_ai_on_new_leads()
    try:
        alert_engine.run_all_checks()
    except Exception as e:
        log.error(f"Alert engine error: {e}")


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


def _extract_mc_outbound(body: dict) -> tuple[str | None, str | None, str, str, str | None]:
    """Parse ManyContacts native outbound webhook payload.

    Returns (phone, agent_name, body_text, sent_at, msg_id).
    Handles multiple known field-name patterns defensively.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # ── Agent name ───────────────────────────────────────────────────────────
    agent_obj = body.get("agent") or body.get("user") or {}
    if isinstance(agent_obj, dict):
        agent_name = (agent_obj.get("name") or agent_obj.get("fullName")
                      or agent_obj.get("username") or agent_obj.get("email"))
    else:
        agent_name = str(agent_obj) if agent_obj else None

    # ── Contact phone ────────────────────────────────────────────────────────
    contact_obj = body.get("contact") or {}
    if isinstance(contact_obj, dict):
        phone = (contact_obj.get("number") or contact_obj.get("phone")
                 or contact_obj.get("wa_id") or contact_obj.get("id"))
    else:
        phone = str(contact_obj) if contact_obj else None
    phone = phone or body.get("number") or body.get("phone") or body.get("contact_number")

    # ── Message body ─────────────────────────────────────────────────────────
    msg_obj = body.get("message") or {}
    if isinstance(msg_obj, dict):
        body_text = (msg_obj.get("body") or msg_obj.get("text") or msg_obj.get("content")
                     or f"[{msg_obj.get('type', 'message')}]")
    else:
        body_text = str(msg_obj) if msg_obj else "[outbound]"
    body_text = body_text or body.get("body") or body.get("text") or "[outbound]"

    # ── Message ID (for dedup) ───────────────────────────────────────────────
    if isinstance(msg_obj, dict):
        msg_id = msg_obj.get("id") or msg_obj.get("wamid")
    else:
        msg_id = None
    msg_id = msg_id or body.get("message_id") or body.get("id")
    # Generate a synthetic stable ID if none provided
    if not msg_id and phone:
        ts_raw = body.get("timestamp") or body.get("createdAt") or body.get("created_at")
        msg_id = f"mc_out_{phone}_{ts_raw or now_iso}"

    # ── Timestamp ────────────────────────────────────────────────────────────
    ts_raw = (body.get("timestamp") or body.get("createdAt") or body.get("created_at")
              or body.get("time"))
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
    log.info(
        f"[webhook/mc] POST | size={len(raw)} | "
        f"ct={request.headers.get('content-type', '')!r} | "
        f"preview={raw[:300]!r}"
    )
    try:
        body = json.loads(raw)
    except Exception:
        log.warning(f"[webhook/mc] non-JSON body: {raw[:300]!r}")
        return {"status": "ok"}

    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        if "entry" in body:
            # Meta WhatsApp Cloud API format
            await _handle_meta_format(body, now_iso)
        elif "agent" in body or "user" in body:
            # ManyContacts native outbound (agent reply) format
            await _handle_mc_outbound(body, now_iso)
        else:
            log.info(f"[webhook/mc] unrecognised format — keys={list(body.keys())} preview={str(body)[:200]}")
    except Exception as e:
        log.error(f"[webhook/mc] handler error: {e}")

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


@app.post("/api/trigger-pipeline")
async def trigger_pipeline():
    asyncio.create_task(run_pipeline())
    return {"status": "pipeline triggered"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
