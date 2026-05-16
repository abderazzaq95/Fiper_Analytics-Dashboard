from fastapi import APIRouter, Query
from supabase import create_client
import anthropic
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import Counter

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
ai_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
MODEL = "claude-sonnet-4-20250514"


def _paginate(build_query) -> list:
    rows, offset = [], 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    if range_ == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    deltas = {"7d": timedelta(days=7), "30d": timedelta(days=30)}
    return (now - deltas.get(range_, timedelta(days=7))).isoformat()


@router.get("/api/quality")
def quality(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    try:
        return _quality_inner(range)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/quality error ({range}): {e}", exc_info=True)
        return {"range": range, "alerts": {"total":0,"open":0,"by_severity":{"HIGH":0,"MED":0,"LOW":0},"list":[]}, "sentiment":{"positive":0,"neutral":0,"negative":0}, "top_topics":{}, "top_risk_flags":{}, "faq_topics":[], "avg_treatment_score":0, "call_outcomes":{}, "complaints_count":0, "avg_messages_per_lead":0}


def _quality_inner(range: str):
    since = _since(range)

    alerts = (
        supabase.table("alerts")
        .select("*")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .execute()
        .data
    ) or []

    # Resolve agent_name for alerts that have lead_id but no agent_name
    unresolved_lead_ids = list({
        a["lead_id"] for a in alerts
        if not a.get("agent_name") and a.get("lead_id")
    })
    if unresolved_lead_ids:
        lead_rows = (
            supabase.table("leads")
            .select("id,assigned_agent")
            .in_("id", unresolved_lead_ids)
            .execute()
            .data
        ) or []
        lead_agent_map = {r["id"]: r.get("assigned_agent") for r in lead_rows}
        for a in alerts:
            if not a.get("agent_name") and a.get("lead_id"):
                a["agent_name"] = lead_agent_map.get(a["lead_id"])

    analyses = (
        supabase.table("ai_analysis")
        .select("sentiment,topics,treatment_score,risk_flags,analyzed_at,source")
        .gte("analyzed_at", since)
        .execute()
        .data
    ) or []

    calls = _paginate(
        lambda: supabase.table("calls").select("id,outcome").gte("called_at", since)
    )

    messages = (
        supabase.table("messages")
        .select("lead_id")
        .gte("sent_at", since)
        .execute()
        .data
    ) or []

    sentiments = [a["sentiment"] for a in analyses if a.get("sentiment")]
    all_topics = [t for a in analyses for t in (a.get("topics") or [])]
    all_risk_flags = [f for a in analyses for f in (a.get("risk_flags") or [])]
    # Exclude maqsam entries with treatment_score=0 (no-answer call artifacts)
    treatment_scores = [
        a["treatment_score"] for a in analyses
        if a.get("treatment_score") is not None
        and not (a.get("source") == "maqsam" and a["treatment_score"] == 0)
    ]

    # Call outcome breakdown for donut chart + answer rate
    call_outcomes: dict = {}
    for c in calls:
        key = (c.get("outcome") or "unknown").lower()
        call_outcomes[key] = call_outcomes.get(key, 0) + 1

    # FAQ topics: structured list with Arabic labels
    _TOPIC_LABELS: dict[str, dict] = {
        "pricing":            {"en": "Pricing",             "ar": "التسعير"},
        "product_fit":        {"en": "Product Fit",         "ar": "ملاءمة المنتج"},
        "trading_education":  {"en": "Trading Education",   "ar": "تعليم التداول"},
        "competitor":         {"en": "Competitor",          "ar": "منافس"},
        "follow_up":          {"en": "Follow Up",           "ar": "متابعة"},
        "not_decision_maker": {"en": "Not Decision Maker",  "ar": "ليس صاحب القرار"},
        "account_info":       {"en": "Account Info",        "ar": "معلومات الحساب"},
        "greetings":          {"en": "Greetings",           "ar": "تحيات"},
        "profit_expectations":{"en": "Profit Expectations", "ar": "توقعات الربح"},
        "technical":          {"en": "Technical",           "ar": "تقني"},
    }
    topic_counter = Counter(all_topics)
    total_topic_count = sum(topic_counter.values())
    faq_topics = [
        {
            "key":   topic,
            "count": count,
            "pct":   round(count / total_topic_count * 100, 1) if total_topic_count else 0,
            "en":    _TOPIC_LABELS.get(topic, {}).get("en", topic.replace("_", " ").title()),
            "ar":    _TOPIC_LABELS.get(topic, {}).get("ar", topic),
        }
        for topic, count in topic_counter.most_common(10)
    ]

    return {
        "range": range,
        "alerts": {
            "total": len(alerts),
            "open": sum(1 for a in alerts if not a.get("resolved")),
            "by_severity": {
                "HIGH": sum(1 for a in alerts if a.get("severity") == "HIGH"),
                "MED": sum(1 for a in alerts if a.get("severity") == "MED"),
                "LOW": sum(1 for a in alerts if a.get("severity") == "LOW"),
            },
            "list": alerts[:50],
        },
        "sentiment": {
            "positive": sentiments.count("positive"),
            "neutral": sentiments.count("neutral"),
            "negative": sentiments.count("negative"),
        },
        "top_topics": dict(Counter(all_topics).most_common(10)),
        "top_risk_flags": dict(Counter(all_risk_flags).most_common(10)),
        "faq_topics": faq_topics,
        "avg_treatment_score": round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
        "call_outcomes": call_outcomes,
        "complaints_count": sentiments.count("negative"),
        "avg_messages_per_lead": round(
            len(messages) / len({m["lead_id"] for m in messages if m.get("lead_id")}), 1
        ) if messages and any(m.get("lead_id") for m in messages) else 0,
    }


@router.get("/api/quality/summary")
def quality_summary(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    analyses = (
        supabase.table("ai_analysis")
        .select("sentiment,topics,risk_flags,treatment_score,outcome,summary")
        .gte("analyzed_at", since)
        .execute()
        .data
    )
    alerts = (
        supabase.table("alerts")
        .select("type,severity,message")
        .gte("created_at", since)
        .eq("resolved", False)
        .execute()
        .data
    )

    context = f"""
Period: last {range}
Total conversations analyzed: {len(analyses)}
Sentiments: positive={sum(1 for a in analyses if a.get('sentiment')=='positive')}, neutral={sum(1 for a in analyses if a.get('sentiment')=='neutral')}, negative={sum(1 for a in analyses if a.get('sentiment')=='negative')}
Open alerts: {len(alerts)}
High severity alerts: {sum(1 for a in alerts if a.get('severity')=='HIGH')}
Common risk flags: {list({f for a in analyses for f in (a.get('risk_flags') or [])})}
Common topics: {list({t for a in analyses for t in (a.get('topics') or [])})}
Avg treatment score: {round(sum(a.get('treatment_score') or 0 for a in analyses) / len(analyses), 1) if analyses else 'N/A'}
"""

    response = ai_client.messages.create(
        model=MODEL,
        max_tokens=400,
        messages=[{
            "role": "user",
            "content": f"You are a CRM analytics assistant for Fiper, a trading broker. Write a concise weekly quality summary (3-4 bullet points) for the management team based on this data. Be direct and actionable. Use English.\n\n{context}"
        }]
    )

    return {"range": range, "summary": response.content[0].text}
