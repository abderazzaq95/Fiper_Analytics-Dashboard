from fastapi import APIRouter, Query
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from pipeline.whatsapp import add_whatsapp_line_select, matches_business_line
load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

_EMPTY_HOURLY = [{"hour": h, "calls": 0} for h in range(24)]
BATCH_SIZE = 100


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    if range_ == "today":
        # Calendar day — since midnight UTC, not rolling 24h
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("week", "7d"):
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("month", "30d"):
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    start = now - timedelta(days=now.weekday())
    return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def _minutes_between(later: str, earlier: str) -> float:
    try:
        later_dt = datetime.fromisoformat(later.replace("Z", "+00:00"))
        earlier_dt = datetime.fromisoformat(earlier.replace("Z", "+00:00"))
        return (later_dt - earlier_dt).total_seconds() / 60
    except (ValueError, TypeError, AttributeError):
        return 0


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


def _safe_count(build_query) -> int:
    try:
        res = build_query().execute()
        return res.count or 0
    except Exception:
        return 0


def _overview_count_fallback(range: str, wa_line: str = "all") -> dict:
    since = _since(range)
    try:
        msg_rows = _paginate(
            lambda: supabase.table("messages")
            .select("id,direction,lead_id,whatsapp_business_number")
            .gte("sent_at", since)
        )
        if wa_line and wa_line.lower() not in ("all", "*", "any"):
            msg_rows = [m for m in msg_rows if matches_business_line(m, wa_line)]
    except Exception:
        msg_rows = []
    total_messages = len(msg_rows)
    inbound = sum(1 for m in msg_rows if m.get("direction") == "inbound")
    outbound = sum(1 for m in msg_rows if m.get("direction") == "outbound")
    try:
        wa_rows = _paginate(
            lambda: supabase.table("leads")
            .select("id,phone,channel,last_message_at,whatsapp_business_number")
            .eq("channel", "whatsapp")
            .gte("last_message_at", since)
        )
        if wa_line and wa_line.lower() not in ("all", "*", "any"):
            wa_rows = [r for r in wa_rows if matches_business_line(r, wa_line)]
        active_whatsapp = len({
            r.get("phone") or r.get("id")
            for r in wa_rows
            if r.get("phone") or r.get("id")
        })
    except Exception:
        active_whatsapp = 0
    total_calls = _safe_count(
        lambda: supabase.table("calls").select("id", count="exact").gte("called_at", since).limit(1)
    )
    call_rows = []
    try:
        call_rows = (
            supabase.table("calls")
            .select("lead_id,duration_seconds")
            .gte("called_at", since)
            .range(0, 999)
            .execute()
            .data or []
        )
    except Exception:
        call_rows = []
    unique_leads = len({c.get("lead_id") for c in call_rows if c.get("lead_id")}) or total_calls
    avg_call_duration = round(
        sum(c.get("duration_seconds") or 0 for c in call_rows) / len(call_rows), 1
    ) if call_rows else 0
    if range == "today":
        open_alerts = _safe_count(
            lambda: supabase.table("alerts").select("id", count="exact").eq("resolved", False).limit(1)
        )
        high_alerts = _safe_count(
            lambda: supabase.table("alerts").select("id", count="exact").eq("resolved", False).eq("severity", "HIGH").limit(1)
        )
    else:
        open_alerts = _safe_count(
            lambda: supabase.table("alerts").select("id", count="exact").gte("created_at", since).eq("resolved", False).limit(1)
        )
        high_alerts = _safe_count(
            lambda: supabase.table("alerts").select("id", count="exact").gte("created_at", since).eq("resolved", False).eq("severity", "HIGH").limit(1)
        )
    return {
        "range": range,
        "leads": {"total": unique_leads, "converted": 0, "conversion_rate": 0, "avg_score": 0},
        "messages": {
            "inbound": inbound,
            "outbound": outbound,
            "stored_total": total_messages,
            "active_conversations": active_whatsapp,
            "total": active_whatsapp,
            "capture_stale": False,
        },
        "calls": {"total": total_calls, "avg_duration_seconds": avg_call_duration},
        "alerts": {"open": open_alerts, "high": high_alerts},
        "hourly_distribution": _EMPTY_HOURLY,
        "fallback": "count",
    }


@router.get("/api/overview")
def overview(
    range: str = Query("week", pattern="^(today|week|month|7d|30d)$"),
    wa_line: str = Query("all"),
):
    try:
        data = _overview_inner(range, wa_line)
        if (
            not data.get("calls", {}).get("total")
            and not data.get("messages", {}).get("total")
            and not data.get("leads", {}).get("total")
        ):
            fallback = _overview_count_fallback(range, wa_line)
            if fallback["calls"]["total"] or fallback["messages"]["total"] or fallback["messages"]["active_conversations"]:
                return fallback
        return data
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/overview error ({range}): {e}", exc_info=True)
        fallback = _overview_count_fallback(range, wa_line)
        if fallback["calls"]["total"] or fallback["messages"]["total"] or fallback["messages"]["active_conversations"]:
            return fallback
        return {"range": range, "leads":{"total":0,"converted":0,"conversion_rate":0,"avg_score":0}, "messages":{"inbound":0,"outbound":0,"total":0}, "calls":{"total":0,"avg_duration_seconds":0}, "alerts":{"open":0,"high":0}, "hourly_distribution":_EMPTY_HOURLY}


def _overview_inner(range: str, wa_line: str = "all"):
    since = _since(range)

    # Messages
    messages = (
        supabase.table("messages").select(add_whatsapp_line_select("id,direction,lead_id,sent_at"))
        .gte("sent_at", since).execute().data or []
    )
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        messages = [m for m in messages if matches_business_line(m, wa_line)]

    # WhatsApp activity: ManyContacts exposes updated conversations even when
    # it does not expose message bodies. This is the honest dashboard number for
    # live WhatsApp activity; stored message rows remain available as detail.
    wa_activity = _paginate(
        lambda: supabase.table("leads")
        .select("id,phone,last_message_at,channel,whatsapp_business_number")
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
    active_whatsapp_phones = {l.get("phone") for l in wa_activity if l.get("phone")}
    active_whatsapp_lead_ids = {l.get("id") for l in wa_activity if l.get("id")}
    if active_whatsapp_phones:
        phones = list(active_whatsapp_phones)
        idx = 0
        while idx < len(phones):
            batch_phones = phones[idx:idx + BATCH_SIZE]
            rows = (
                supabase.table("leads")
                .select("id,phone")
                .eq("channel", "whatsapp")
                .in_("phone", batch_phones)
                .execute().data or []
            )
            active_whatsapp_lead_ids.update(r["id"] for r in rows if r.get("id"))
            idx += BATCH_SIZE
    latest_whatsapp_activity = max(
        (l.get("last_message_at") or "" for l in wa_activity),
        default="",
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

    # Paginate all call lead_ids when the selected range exceeds Supabase's first-page cap.
    if total_calls > 1000:
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
            batch_ids = id_list[idx:idx + BATCH_SIZE]
            chunk = (
                supabase.table("leads")
                .select("id,phone,status,score,channel,whatsapp_business_number")
                .in_("id", batch_ids)
                .execute()
                .data or []
            )
            leads.extend(chunk)
            idx += BATCH_SIZE
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        # Keep Maqsam leads; filter only WhatsApp rows by the selected line.
        leads = [l for l in leads if matches_business_line(l, wa_line)]

    # Alerts: small dataset
    alert_query = supabase.table("alerts").select("id,lead_id,severity,resolved,created_at")
    if range != "today":
        alert_query = alert_query.gte("created_at", since)
    alerts = alert_query.execute().data or []
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        # Build lead_map from the alert lead IDs directly, not from active_lead_ids,
        # so alerts for leads that weren't active today are still attributed correctly.
        alert_lead_ids = list({a["lead_id"] for a in alerts if a.get("lead_id")})
        alert_lead_rows: list[dict] = []
        for i in range(0, len(alert_lead_ids), BATCH_SIZE):
            alert_lead_rows.extend(
                supabase.table("leads")
                .select("id,channel,whatsapp_business_number")
                .in_("id", alert_lead_ids[i:i + BATCH_SIZE])
                .execute()
                .data or []
            )
        alert_lead_map = {r["id"]: r for r in alert_lead_rows}
        alerts = [a for a in alerts if matches_business_line(alert_lead_map.get(a.get("lead_id")), wa_line)]

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
    if active_whatsapp_conversations == 0 and msg_lead_ids:
        msg_lead_list = list(msg_lead_ids)
        stored_message_keys = set()
        idx = 0
        while idx < len(msg_lead_list):
            rows = (
                supabase.table("leads")
                .select("id,phone,channel,whatsapp_business_number")
                .in_("id", msg_lead_list[idx:idx + BATCH_SIZE])
                .execute().data or []
            )
            stored_message_keys.update(
                r.get("phone") or r.get("id")
                for r in rows
                if r.get("channel") == "whatsapp"
                and (r.get("phone") or r.get("id"))
            )
            idx += BATCH_SIZE
        active_whatsapp_conversations = len(stored_message_keys)

    latest_stored_message = max(
        (m.get("sent_at") or "" for m in messages),
        default="",
    )
    capture_lag_min = (
        _minutes_between(latest_whatsapp_activity, latest_stored_message)
        if latest_whatsapp_activity and latest_stored_message else 0
    )
    capture_stale = bool(
        latest_whatsapp_activity
        and (not latest_stored_message or capture_lag_min > 10)
    )

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
            "stored_total": len(messages),
            "active_conversations": active_whatsapp_conversations,
            "total": active_whatsapp_conversations,
            "latest_activity_at": latest_whatsapp_activity or None,
            "latest_stored_at": latest_stored_message or None,
            "capture_lag_min": round(capture_lag_min, 1),
            "capture_stale": capture_stale,
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
