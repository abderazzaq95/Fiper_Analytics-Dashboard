import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
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
    """Poll ManyContacts for contacts updated in the last N hours."""
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
        created_at = contact.get("createdAt")

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


async def ingest_maqsam():
    log.info("Maqsam ingestion started")
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(hours=2)).strftime("%Y-%m-%d")
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
    """Run Claude analysis on leads that have messages but no ai_analysis yet."""
    unanalyzed = (
        supabase.table("leads")
        .select("id")
        .execute()
        .data
    )
    for row in unanalyzed:
        lead_id = row["id"]
        existing = supabase.table("ai_analysis").select("id").eq("lead_id", lead_id).execute()
        if existing.data:
            continue
        msgs = supabase.table("messages").select("*").eq("lead_id", lead_id).execute().data
        if not msgs:
            continue
        try:
            result = ai_analyzer.analyze_conversation(msgs)
            supabase.table("ai_analysis").insert({
                "lead_id": lead_id,
                "source": "whatsapp",
                **result,
            }).execute()
            supabase.table("leads").update({
                "score": result.get("score"),
            }).eq("id", lead_id).execute()
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm agent cache on startup
    try:
        await whatsapp.fetch_users()
    except Exception as e:
        log.warning(f"Could not pre-load agent cache: {e}")

    scheduler.add_job(run_pipeline, "interval", hours=2, id="pipeline", replace_existing=True)
    scheduler.start()
    log.info("Scheduler started — pipeline runs every 2 hours")
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


# ---------------------------------------------------------------------------
# ManyContacts webhook (real-time messages)
# ---------------------------------------------------------------------------

@app.post("/webhook/manycontacts")
async def webhook_manycontacts(request: Request):
    """
    Receives real-time message events from ManyContacts.
    Expected payload shape (ManyContacts sends these on new messages):
      {
        "event": "new_message" | "contact_updated" | ...,
        "contact": { "id": ..., "number": ..., "name": ..., "last_user_id": ... },
        "message": { "id": ..., "text": ..., "type": "INBOUND"|"OUTBOUND",
                     "timestamp": ..., "user_id": ... }
      }
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    try:
        event = body.get("event", "")
        contact_data = body.get("contact", {})
        message_data = body.get("message", {})

        mc_id = contact_data.get("id")
        phone = contact_data.get("number", "")
        name = contact_data.get("name")
        last_user_id = contact_data.get("last_user_id")
        agent_name = whatsapp.resolve_agent_name(last_user_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        if mc_id:
            lead_result = supabase.table("leads").upsert({
                "wa_contact_id": mc_id,
                "phone": phone,
                "name": name or None,
                "channel": "whatsapp",
                "assigned_agent": agent_name,
                "last_message_at": now_iso,
                "updated_at": now_iso,
            }, on_conflict="wa_contact_id").execute()
            lead_id = lead_result.data[0]["id"] if lead_result.data else None
        else:
            lead_id = None

        if message_data and lead_id:
            msg_id = message_data.get("id")
            msg_type = message_data.get("type", "INBOUND").upper()
            direction = "inbound" if msg_type == "INBOUND" else "outbound"
            body_text = message_data.get("text") or message_data.get("body") or f"[{message_data.get('type', 'media')}]"
            msg_agent = whatsapp.resolve_agent_name(message_data.get("user_id")) or agent_name

            raw_ts = message_data.get("timestamp")
            if raw_ts:
                try:
                    sent_at = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    sent_at = now_iso
            else:
                sent_at = now_iso

            if msg_id:
                supabase.table("messages").upsert({
                    "wa_message_id": msg_id,
                    "lead_id": lead_id,
                    "direction": direction,
                    "body": body_text,
                    "agent_name": msg_agent,
                    "sent_at": sent_at,
                }, on_conflict="wa_message_id").execute()

            # Immediately check no_reply alert on inbound messages
            if direction == "inbound":
                try:
                    alert_engine.check_no_reply()
                except Exception as e:
                    log.error(f"no_reply check error: {e}")

    except Exception as e:
        log.error(f"ManyContacts webhook error: {e}")

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
