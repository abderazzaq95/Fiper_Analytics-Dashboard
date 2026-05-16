from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    if range_ == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    deltas = {"7d": timedelta(days=7), "30d": timedelta(days=30)}
    return (now - deltas.get(range_, timedelta(days=7))).isoformat()


def _paginate(build_query) -> list:
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


@router.get("/api/channels")
def channels(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    try:
        return _channels_inner(range)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/channels error ({range}): {e}", exc_info=True)
        return {"range": range, "whatsapp":{"leads":0,"converted":0,"conversion_rate":0,"messages":0,"avg_response_time_min":0}, "maqsam":{"leads":0,"converted":0,"conversion_rate":0,"calls":0,"avg_call_duration_seconds":0}}


def _channels_inner(range: str):
    since = _since(range)

    # Leads: paginate — no date filter intentional (show channel totals all-time)
    leads = _paginate(lambda: supabase.table("leads").select("id,channel,status"))

    # Messages: small dataset
    messages = (
        supabase.table("messages").select("lead_id,direction,sent_at").gte("sent_at", since).execute().data or []
    )

    # Calls: use count="exact" for accurate total; sample for avg duration
    calls_res = (
        supabase.table("calls")
        .select("id,duration_seconds,called_at", count="exact")
        .gte("called_at", since)
        .execute()
    )
    total_calls_maqsam = calls_res.count if calls_res.count is not None else len(calls_res.data or [])
    calls_sample = calls_res.data or []

    wa_leads = [l for l in leads if l.get("channel") == "whatsapp"]
    mq_leads = [l for l in leads if l.get("channel") == "maqsam"]

    wa_converted = sum(1 for l in wa_leads if l.get("status") == "converted")
    mq_converted = sum(1 for l in mq_leads if l.get("status") == "converted")

    lead_ids_by_channel = {
        "whatsapp": {l["id"] for l in wa_leads},
    }

    wa_msgs = [m for m in messages if m.get("lead_id") in lead_ids_by_channel["whatsapp"]]
    response_times = []
    by_lead: dict[str, list] = {}
    for m in wa_msgs:
        by_lead.setdefault(m["lead_id"], []).append(m)

    for msgs in by_lead.values():
        sorted_msgs = sorted(msgs, key=lambda x: x["sent_at"])
        last_in = None
        for m in sorted_msgs:
            if m["direction"] == "inbound":
                last_in = datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))
            elif m["direction"] == "outbound" and last_in:
                out_t = datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))
                gap = (out_t - last_in).total_seconds() / 60
                if gap >= 0:
                    response_times.append(gap)
                last_in = None

    avg_response = round(sum(response_times) / len(response_times), 1) if response_times else 0
    avg_call_dur = round(
        sum(c.get("duration_seconds") or 0 for c in calls_sample) / len(calls_sample), 1
    ) if calls_sample else 0

    return {
        "range": range,
        "whatsapp": {
            "leads": len(wa_leads),
            "converted": wa_converted,
            "conversion_rate": round(wa_converted / len(wa_leads) * 100, 1) if wa_leads else 0,
            "messages": len(wa_msgs),
            "avg_response_time_min": avg_response,
        },
        "maqsam": {
            "leads": len(mq_leads),
            "converted": mq_converted,
            "conversion_rate": round(mq_converted / len(mq_leads) * 100, 1) if mq_leads else 0,
            "calls": total_calls_maqsam,
            "avg_call_duration_seconds": avg_call_dur,
        },
    }


@router.get("/api/channels/traffic")
def channels_traffic():
    """Multi-period traffic breakdown: today / 2d / 7d for each channel."""
    now = datetime.now(timezone.utc)
    since_7d = (now - timedelta(days=7)).isoformat()

    leads = (
        supabase.table("leads")
        .select("id,channel,created_at")
        .gte("created_at", since_7d)
        .execute()
        .data
    ) or []

    wa_lead_ids = {l["id"] for l in leads if l.get("channel") == "whatsapp"}

    messages = (
        supabase.table("messages")
        .select("lead_id,sent_at")
        .gte("sent_at", since_7d)
        .execute()
        .data
    ) or []

    # Use count="exact" per period to avoid row cap
    result: dict = {}
    for label, delta in [("today", None), ("2d", timedelta(days=2)), ("7d", timedelta(days=7))]:
        if delta is None:
            cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        else:
            cutoff = (now - delta).isoformat()
        p_leads = [l for l in leads if (l.get("created_at") or "") >= cutoff]
        wa = sum(1 for l in p_leads if l.get("channel") == "whatsapp")
        mq = sum(1 for l in p_leads if l.get("channel") == "maqsam")
        msgs = sum(1 for m in messages if (m.get("sent_at") or "") >= cutoff and m.get("lead_id") in wa_lead_ids)

        calls_count_res = (
            supabase.table("calls")
            .select("id", count="exact")
            .gte("called_at", cutoff)
            .execute()
        )
        cls = calls_count_res.count or 0

        result[label] = {
            "whatsapp": {"leads": wa, "messages": msgs},
            "maqsam": {"leads": mq, "calls": cls},
        }

    return result
