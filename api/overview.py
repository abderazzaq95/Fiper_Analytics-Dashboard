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
        delta = timedelta(days=1)
    elif range_ == "7d":
        delta = timedelta(days=7)
    else:
        delta = timedelta(days=30)
    return (now - delta).isoformat()


@router.get("/api/overview")
def overview(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    leads = supabase.table("leads").select("id,status,score,channel,created_at").gte("created_at", since).execute().data
    messages = supabase.table("messages").select("id,direction,sent_at").gte("sent_at", since).execute().data
    calls = supabase.table("calls").select("id,duration_seconds,called_at").gte("called_at", since).execute().data
    alerts = supabase.table("alerts").select("id,severity,resolved,created_at").gte("created_at", since).execute().data

    total_leads = len(leads)
    converted = sum(1 for l in leads if l.get("status") == "converted")
    conversion_rate = round((converted / total_leads * 100) if total_leads else 0, 1)
    avg_score = round(sum(l.get("score") or 0 for l in leads) / total_leads, 1) if total_leads else 0

    inbound = sum(1 for m in messages if m.get("direction") == "inbound")
    outbound = sum(1 for m in messages if m.get("direction") == "outbound")

    total_calls = len(calls)
    avg_call_duration = round(
        sum(c.get("duration_seconds") or 0 for c in calls) / total_calls, 1
    ) if total_calls else 0

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
