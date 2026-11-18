import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastapi import APIRouter, Query

from api import dataset
from pipeline.time_ranges import RANGE_PATTERN

load_dotenv()
router = APIRouter()
_LOCAL_TZ = ZoneInfo(os.getenv("REPORT_TIMEZONE", "Etc/GMT-2"))  # peak hours in Fiper's local time


def _empty(range_: str) -> dict:
    return {
        "range": range_,
        "leads": {"total": 0, "converted": 0, "conversion_rate": 0, "avg_score": 0},
        "messages": {"inbound": 0, "outbound": 0, "stored_total": 0, "active_conversations": 0, "total": 0},
        "calls": {"total": 0, "avg_duration_seconds": 0},
        "alerts": {"open": 0, "high": 0},
        "hourly_distribution": [{"hour": hour, "calls": 0} for hour in range(24)],
    }


def _minutes_between(later: str, earlier: str) -> float:
    try:
        end = datetime.fromisoformat(later.replace("Z", "+00:00"))
        start = datetime.fromisoformat(earlier.replace("Z", "+00:00"))
        return max(0.0, (end - start).total_seconds() / 60)
    except (ValueError, AttributeError):
        return 0.0


@router.get("/api/overview")
def overview(
    range: str = Query("week", pattern=RANGE_PATTERN),
    wa_line: str = Query("all"),
    team: str = Query(""),
):
    try:
        return dataset.view(range, team, wa_line).cached("overview", _overview_inner)
    except Exception as exc:
        logging.getLogger("fiper").error(f"/api/overview error ({range}): {exc}", exc_info=True)
        return _empty(range)


def _overview_inner(view: dataset.View) -> dict:
    messages, activity, calls, alerts = view.messages, view.activity, view.calls, view.alerts

    conversations = {lead.get("phone") or lead.get("id") for lead in activity if lead.get("phone") or lead.get("id")}
    message_lead_ids = {m["lead_id"] for m in messages if m.get("lead_id")}
    call_lead_ids = {c["lead_id"] for c in calls if c.get("lead_id")}
    leads = [view.leads[i] for i in message_lead_ids | call_lead_ids if i in view.leads]
    if not conversations:
        conversations = {
            view.leads[i].get("phone") or i
            for i in message_lead_ids if i in view.leads and view.leads[i].get("channel") == "whatsapp"
        }

    # Unique people: one person can have a WhatsApp and a Maqsam lead row.
    people = {lead.get("phone") for lead in leads if lead.get("phone")}
    converted = {lead.get("phone") for lead in leads if lead.get("status") == "converted" and lead.get("phone")}
    scored = [lead["score"] for lead in leads if lead.get("score") is not None]

    latest_activity = max((lead.get("last_message_at") or "" for lead in activity), default="")
    latest_stored = max((m.get("sent_at") or "" for m in messages), default="")
    capture_lag = _minutes_between(latest_activity, latest_stored) if latest_activity and latest_stored else 0

    hourly = [0] * 24
    for call in calls:
        try:
            when = datetime.fromisoformat(str(call.get("called_at")).replace("Z", "+00:00"))
            hourly[when.astimezone(_LOCAL_TZ).hour] += 1
        except ValueError:
            pass
    open_alerts = [a for a in alerts if not a.get("resolved")]

    return {
        "range": view.period.range,
        "leads": {
            "total": len(people),
            "converted": len(converted),
            "conversion_rate": round(len(converted) / len(people) * 100, 1) if people else 0,
            "avg_score": round(sum(scored) / len(scored), 1) if scored else 0,
        },
        "messages": {
            "inbound": sum(1 for m in messages if m.get("direction") == "inbound"),
            "outbound": sum(1 for m in messages if m.get("direction") == "outbound"),
            "stored_total": len(messages),
            "active_conversations": len(conversations),
            "total": len(conversations),
            "latest_activity_at": latest_activity or None,
            "latest_stored_at": latest_stored or None,
            "capture_lag_min": round(capture_lag, 1),
            "capture_stale": bool(latest_activity and (not latest_stored or capture_lag > 10)),
        },
        "calls": {
            "total": len(calls),
            "avg_duration_seconds": round(sum(c.get("duration_seconds") or 0 for c in calls) / len(calls), 1) if calls else 0,
        },
        "alerts": {
            "open": len(open_alerts),
            "high": sum(1 for a in open_alerts if a.get("severity") == "HIGH"),
        },
        "hourly_distribution": [{"hour": hour, "calls": count} for hour, count in enumerate(hourly)],
    }
