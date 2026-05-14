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
    deltas = {"today": timedelta(days=1), "7d": timedelta(days=7), "30d": timedelta(days=30)}
    return (now - deltas.get(range_, timedelta(days=7))).isoformat()


def _paginate(build_query) -> list:
    """Exhaust Supabase pagination — calls build_query() fresh each page."""
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


@router.get("/api/overview")
def overview(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    # Leads: paginate — need status + score for conversion rate and avg score
    leads = _paginate(
        lambda: supabase.table("leads").select("id,status,score,channel,created_at").gte("created_at", since)
    )

    # Messages: small dataset, no pagination needed
    messages = supabase.table("messages").select("id,direction,sent_at").gte("sent_at", since).execute().data or []

    # Calls: use count="exact" for accurate total; keep first-page sample for avg duration
    calls_res = (
        supabase.table("calls")
        .select("id,duration_seconds,called_at", count="exact")
        .gte("called_at", since)
        .execute()
    )
    total_calls = calls_res.count if calls_res.count is not None else len(calls_res.data or [])
    calls_sample = calls_res.data or []  # first 1000 — used only for avg duration

    # Alerts: small dataset
    alerts = supabase.table("alerts").select("id,severity,resolved,created_at").gte("created_at", since).execute().data or []

    total_leads = len(leads)
    converted = sum(1 for l in leads if l.get("status") == "converted")
    conversion_rate = round((converted / total_leads * 100) if total_leads else 0, 1)
    scored_leads = [l for l in leads if l.get("score") is not None]
    avg_score = round(sum(l["score"] for l in scored_leads) / len(scored_leads), 1) if scored_leads else 0

    inbound = sum(1 for m in messages if m.get("direction") == "inbound")
    outbound = sum(1 for m in messages if m.get("direction") == "outbound")

    avg_call_duration = round(
        sum(c.get("duration_seconds") or 0 for c in calls_sample) / len(calls_sample), 1
    ) if calls_sample else 0

    open_alerts = sum(1 for a in alerts if not a.get("resolved"))
    high_alerts = sum(1 for a in alerts if a.get("severity") == "HIGH" and not a.get("resolved"))

    return {
        "range": range,
        "leads": {
            "total": total_leads,
            "converted": converted,
            "conversion_rate": conversion_rate,
            "avg_score": avg_score,
        },
        "messages": {
            "inbound": inbound,
            "outbound": outbound,
            "total": len(messages),
        },
        "calls": {
            "total": total_calls,
            "avg_duration_seconds": avg_call_duration,
        },
        "alerts": {
            "open": open_alerts,
            "high": high_alerts,
        },
    }
