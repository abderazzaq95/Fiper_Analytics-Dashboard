from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

_EMPTY_HOURLY = [{"hour": h, "calls": 0} for h in range(24)]


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    if range_ == "today":
        # Calendar day — since midnight UTC, not rolling 24h
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    deltas = {"7d": timedelta(days=7), "30d": timedelta(days=30)}
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
    try:
        return _overview_inner(range)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/overview error ({range}): {e}", exc_info=True)
        return {"range": range, "leads":{"total":0,"converted":0,"conversion_rate":0,"avg_score":0}, "messages":{"inbound":0,"outbound":0,"total":0}, "calls":{"total":0,"avg_duration_seconds":0}, "alerts":{"open":0,"high":0}, "hourly_distribution":_EMPTY_HOURLY}


def _overview_inner(range: str):
    since = _since(range)

    # Messages: small dataset
    messages = (
        supabase.table("messages").select("id,direction,lead_id,sent_at")
        .gte("sent_at", since).execute().data or []
    )

    # Calls: paginate for accurate call-based lead count; count="exact" for total
    calls_res = (
        supabase.table("calls")
        .select("id,duration_seconds,lead_id", count="exact")
        .gte("called_at", since)
        .execute()
    )
    total_calls = calls_res.count if calls_res.count is not None else 0
    calls_sample = calls_res.data or []  # first 1000 — used for avg duration

    # ── Active leads in period ────────────────────────────────────────────────
    # Derive from actual activity (calls + messages) rather than created_at.
    # created_at is unreliable for backfilled leads (all set to insertion date).
    call_lead_ids: set[str] = set()
    for batch in [calls_sample]:  # first 1000 covers today/7d well; paginate for 30d
        call_lead_ids.update(c["lead_id"] for c in batch if c.get("lead_id"))

    # For 30d, paginate all call lead_ids (up to ~40k calls)
    if range == "30d" and total_calls > 1000:
        call_lead_ids = set(
            c["lead_id"]
            for c in _paginate(
                lambda: supabase.table("calls")
                .select("lead_id")
                .gte("called_at", since)
            )
            if c.get("lead_id")
        )

    msg_lead_ids = {m["lead_id"] for m in messages if m.get("lead_id")}
    active_lead_ids = call_lead_ids | msg_lead_ids

    # Fetch lead details (status, score) for active leads only
    # Use a while loop — 'range' is shadowed by the function parameter name
    leads = []
    if active_lead_ids:
        id_list = list(active_lead_ids)
        idx = 0
        while idx < len(id_list):
            batch_ids = id_list[idx:idx + 500]
            chunk = (
                supabase.table("leads")
                .select("id,phone,status,score")
                .in_("id", batch_ids)
                .execute()
                .data or []
            )
            leads.extend(chunk)
            idx += 500

    # Alerts: small dataset
    alerts = (
        supabase.table("alerts").select("id,severity,resolved,created_at")
        .gte("created_at", since).execute().data or []
    )

    # Count DISTINCT phone numbers — same person may have a Maqsam lead and a
    # WhatsApp lead; we want unique people, not unique DB rows.
    unique_phones = {l.get("phone") for l in leads if l.get("phone")}
    total_leads = len(unique_phones)
    # A phone is "converted" if any of its lead records has status=converted
    converted_phones = {l.get("phone") for l in leads if l.get("status") == "converted" and l.get("phone")}
    converted = len(converted_phones)
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

    # Hourly distribution: bucket all calls by hour of day (0–23)
    all_called_at = _paginate(
        lambda: supabase.table("calls").select("called_at").gte("called_at", since)
    )
    hourly_counts = [0] * 24
    for c in all_called_at:
        ts = c.get("called_at")
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                hourly_counts[dt.hour] += 1
            except (ValueError, TypeError):
                pass
    hourly_distribution = [
        {"hour": i, "calls": count}
        for i, count in enumerate(hourly_counts)
    ]

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
        "hourly_distribution": hourly_distribution,
    }
