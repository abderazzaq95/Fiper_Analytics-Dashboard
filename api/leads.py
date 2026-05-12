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


@router.get("/api/leads")
def leads(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    all_leads = (
        supabase.table("leads")
        .select("id,name,phone,channel,status,score,assigned_agent,last_message_at,created_at")
        .gte("created_at", since)
        .order("score", desc=True)
        .execute()
        .data
    )

    status_counts = {}
    score_buckets = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for l in all_leads:
        status = l.get("status", "new")
        status_counts[status] = status_counts.get(status, 0) + 1
        score = l.get("score") or 0
        if score <= 25:
            score_buckets["0-25"] += 1
        elif score <= 50:
            score_buckets["26-50"] += 1
        elif score <= 75:
            score_buckets["51-75"] += 1
        else:
            score_buckets["76-100"] += 1

    return {
        "range": range,
        "total": len(all_leads),
        "funnel": status_counts,
        "score_distribution": score_buckets,
        "leads": all_leads,
    }
