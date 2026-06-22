from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from pipeline.whatsapp import matches_business_line
load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
BATCH_SIZE = 100


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    if range_ == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("week", "7d"):
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("month", "30d"):
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    start = now - timedelta(days=now.weekday())
    return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def _paginate(build_query) -> list:
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def _exact_count(build_query) -> int:
    res = build_query().execute()
    return res.count or 0


def _unique_call_leads_count(since: str) -> int:
    try:
        rows = _paginate(
            lambda: supabase.table("calls")
            .select("lead_id")
            .gte("called_at", since)
        )
        return len({r.get("lead_id") for r in rows if r.get("lead_id")})
    except Exception as e:
        import logging
        logging.getLogger("fiper").warning(
            f"/api/channels/traffic unique Maqsam leads failed: {e}",
            exc_info=True,
        )
        return _exact_count(
            lambda: supabase.table("calls")
            .select("id", count="exact")
            .gte("called_at", since)
        )


@router.get("/api/channels")
def channels(
    range: str = Query("week", pattern="^(today|week|month|7d|30d)$"),
    wa_line: str = Query("all"),
):
    try:
        return _channels_inner(range, wa_line)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/channels error ({range}): {e}", exc_info=True)
        fallback = _channels_from_overview(range, wa_line)
        if fallback:
            return fallback
        return {"range": range, "whatsapp":{"leads":0,"converted":0,"conversion_rate":0,"messages":0,"avg_response_time_min":0}, "maqsam":{"leads":0,"converted":0,"conversion_rate":0,"calls":0,"avg_call_duration_seconds":0}}


def _channels_from_overview(range: str, wa_line: str = "all"):
    try:
        from api.overview import _overview_inner, _overview_count_fallback
        try:
            overview = _overview_inner(range, wa_line)
        except Exception:
            overview = _overview_count_fallback(range, wa_line)
        return {
            "range": range,
            "whatsapp": {
                "leads": overview.get("messages", {}).get("active_conversations") or overview.get("messages", {}).get("total") or 0,
                "converted": 0,
                "conversion_rate": 0,
                "messages": overview.get("messages", {}).get("total") or 0,
                "stored_messages": overview.get("messages", {}).get("stored_total") or 0,
                "avg_response_time_min": 0,
            },
            "maqsam": {
                "leads": overview.get("leads", {}).get("total") or 0,
                "converted": overview.get("leads", {}).get("converted") or 0,
                "conversion_rate": overview.get("leads", {}).get("conversion_rate") or 0,
                "calls": overview.get("calls", {}).get("total") or 0,
                "avg_call_duration_seconds": overview.get("calls", {}).get("avg_duration_seconds") or 0,
            },
        }
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/channels overview fallback error ({range}): {e}", exc_info=True)
        return None


def _channels_inner(range: str, wa_line: str = "all"):
    since = _since(range)

    stored_messages_rows = _paginate(
        lambda: supabase.table("messages")
        .select("id,direction,lead_id")
        .gte("sent_at", since)
    )
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        stored_messages_rows = [m for m in stored_messages_rows if matches_business_line(m, wa_line)]
    stored_messages_count = len(stored_messages_rows)

    wa_activity = _paginate(
        lambda: supabase.table("leads")
        .select("id,phone,status,channel")
        .eq("channel", "whatsapp")
        .gte("last_message_at", since)
    )
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        wa_activity = [l for l in wa_activity if matches_business_line(l, wa_line)]
    active_whatsapp_conversations = len({
        l.get("phone") or l.get("id")
        for l in wa_activity
        if l.get("phone") or l.get("id")
    })

    total_calls_maqsam = _exact_count(
        lambda: supabase.table("calls").select("id", count="exact").gte("called_at", since)
    )
    call_rows = _paginate(
        lambda: supabase.table("calls").select("lead_id,duration_seconds").gte("called_at", since)
    )

    active_whatsapp_keys = {
        l.get("phone") or l.get("id")
        for l in wa_activity
        if l.get("phone") or l.get("id")
    }
    mq_call_lead_ids = {c["lead_id"] for c in call_rows if c.get("lead_id")}
    mq_call_leads = []
    mq_ids = list(mq_call_lead_ids)
    idx = 0
    while idx < len(mq_ids):
        batch_ids = mq_ids[idx:idx + BATCH_SIZE]
        mq_call_leads.extend(
            supabase.table("leads")
            .select("id,phone,status")
            .in_("id", batch_ids)
            .execute().data or []
        )
        idx += BATCH_SIZE
    mq_unique_people = {
        l.get("phone") or l.get("id")
        for l in mq_call_leads
        if l.get("phone") or l.get("id")
    }

    wa_converted_people = {
        l.get("phone") or l.get("id")
        for l in wa_activity
        if l.get("status") == "converted" and (l.get("phone") or l.get("id"))
    }
    mq_converted_people = {
        l.get("phone") or l.get("id")
        for l in mq_call_leads
        if l.get("status") == "converted" and (l.get("phone") or l.get("id"))
    }

    avg_call_dur = round(
        sum(c.get("duration_seconds") or 0 for c in call_rows) / len(call_rows), 1
    ) if call_rows else 0

    return {
        "range": range,
        "whatsapp": {
            "leads": len(active_whatsapp_keys),
            "converted": len(wa_converted_people),
            "conversion_rate": round(len(wa_converted_people) / len(active_whatsapp_keys) * 100, 1) if active_whatsapp_keys else 0,
            "messages": active_whatsapp_conversations,
            "stored_messages": stored_messages_count,
            "avg_response_time_min": 0,
        },
        "maqsam": {
            "leads": len(mq_unique_people),
            "converted": len(mq_converted_people),
            "conversion_rate": round(len(mq_converted_people) / len(mq_unique_people) * 100, 1) if mq_unique_people else 0,
            "calls": total_calls_maqsam,
            "avg_call_duration_seconds": avg_call_dur,
        },
    }


_EMPTY_TRAFFIC = {
    p: {"whatsapp": {"leads": 0, "messages": 0}, "maqsam": {"leads": 0, "calls": 0}}
    for p in ("today", "week", "month")
}


@router.get("/api/channels/traffic")
def channels_traffic(wa_line: str = Query("all")):
    """Multi-period traffic breakdown: today / this week / this month."""
    try:
        return _channels_traffic_inner(wa_line)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/channels/traffic error: {e}", exc_info=True)
        return _EMPTY_TRAFFIC


def _channels_traffic_inner(wa_line: str = "all"):
    now = datetime.now(timezone.utc)
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    result: dict = {}
    for label, cutoff_dt in [("today", start_today), ("week", start_week), ("month", start_month)]:
        cutoff = cutoff_dt.isoformat()
        wa = _exact_count(
            lambda cutoff=cutoff: supabase.table("leads")
            .select("id,phone,status,channel")
            .eq("channel", "whatsapp")
            .gte("last_message_at", cutoff)
        )
        if wa_line and wa_line.lower() not in ("all", "*", "any"):
            wa_rows = _paginate(
                lambda cutoff=cutoff: supabase.table("leads")
                .select("id,phone,status,channel")
                .eq("channel", "whatsapp")
                .gte("last_message_at", cutoff)
            )
            wa = len([r for r in wa_rows if matches_business_line(r, wa_line)])
        mq = _unique_call_leads_count(cutoff)
        msgs = _exact_count(
            lambda cutoff=cutoff: supabase.table("messages")
            .select("id,direction,lead_id")
            .gte("sent_at", cutoff)
        )
        if wa_line and wa_line.lower() not in ("all", "*", "any"):
            msg_rows = _paginate(
                lambda cutoff=cutoff: supabase.table("messages")
                .select("id,direction,lead_id")
                .gte("sent_at", cutoff)
            )
            msgs = len([m for m in msg_rows if matches_business_line(m, wa_line)])
        cls = _exact_count(
            lambda cutoff=cutoff: supabase.table("calls")
            .select("id", count="exact")
            .gte("called_at", cutoff)
        )

        result[label] = {
            "whatsapp": {"leads": wa, "messages": msgs},
            "maqsam": {"leads": mq, "calls": cls},
        }

    return result
