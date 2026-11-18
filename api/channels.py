import logging

from fastapi import APIRouter, Query

from api import dataset
from pipeline.time_ranges import RANGE_PATTERN

router = APIRouter()


def _person(lead: dict):
    return lead.get("phone") or lead.get("id")


def _empty(range_: str) -> dict:
    return {
        "range": range_,
        "whatsapp": {"leads": 0, "converted": 0, "conversion_rate": 0, "messages": 0, "stored_messages": 0,
                     "avg_response_time_min": 0},
        "maqsam": {"leads": 0, "converted": 0, "conversion_rate": 0, "calls": 0, "avg_call_duration_seconds": 0},
    }


@router.get("/api/channels")
def channels(
    range: str = Query("week", pattern=RANGE_PATTERN),
    wa_line: str = Query("all"),
    team: str = Query(""),
):
    try:
        return dataset.view(range, team, wa_line).cached("channels", _channels_inner)
    except Exception as exc:
        logging.getLogger("fiper").error(f"/api/channels error ({range}): {exc}", exc_info=True)
        return _empty(range)


def _rate(part: set, whole: set) -> float:
    return round(len(part) / len(whole) * 100, 1) if whole else 0


def _channels_inner(view: dataset.View) -> dict:
    activity, calls = view.activity, view.calls
    whatsapp_people = {_person(lead) for lead in activity if _person(lead)}
    whatsapp_converted = {_person(lead) for lead in activity if lead.get("status") == "converted" and _person(lead)}

    call_leads = [view.period.leads[i] for i in {c.get("lead_id") for c in calls} if i in view.period.leads]
    call_people = {_person(lead) for lead in call_leads if _person(lead)}
    call_converted = {_person(lead) for lead in call_leads if lead.get("status") == "converted" and _person(lead)}
    median = view.reply_stats()["team"].get("median_reply")
    outcomes = [str(c.get("outcome") or "").lower() for c in calls]
    completed = outcomes.count("completed")
    pickup_base = completed + outcomes.count("no_answer") + outcomes.count("busy")

    return {
        "range": view.period.range,
        "whatsapp": {
            "leads": len(whatsapp_people),
            "converted": len(whatsapp_converted),
            "conversion_rate": _rate(whatsapp_converted, whatsapp_people),
            "messages": len(whatsapp_people),
            "stored_messages": len(view.messages),
            "avg_response_time_min": round(median, 1) if median is not None else 0,
        },
        "maqsam": {
            "leads": len(call_people),
            "converted": len(call_converted),
            "conversion_rate": _rate(call_converted, call_people),
            "calls": len(calls),
            "pickup_rate": round(completed / pickup_base * 100, 1) if pickup_base else 0,
            "avg_call_duration_seconds": round(sum(c.get("duration_seconds") or 0 for c in calls) / len(calls), 1) if calls else 0,
        },
    }
