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


def _since(range_: str) -> str:
    now = datetime.now(timezone.utc)
    deltas = {"today": timedelta(days=1), "7d": timedelta(days=7), "30d": timedelta(days=30)}
    return (now - deltas.get(range_, timedelta(days=7))).isoformat()


@router.get("/api/quality")
def quality(range: str = Query("7d", pattern="^(today|7d|30d)$")):
    since = _since(range)

    alerts = (
        supabase.table("alerts")
        .select("*")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .execute()
        .data
    )

    analyses = (
        supabase.table("ai_analysis")
        .select("sentiment,topics,treatment_score,risk_flags,analyzed_at")
        .gte("analyzed_at", since)
        .execute()
        .data
    )

    sentiments = [a["sentiment"] for a in analyses if a.get("sentiment")]
    all_topics = [t for a in analyses for t in (a.get("topics") or [])]
    all_risk_flags = [f for a in analyses for f in (a.get("risk_flags") or [])]
    treatment_scores = [a["treatment_score"] for a in analyses if a.get("treatment_score") is not None]

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
        "avg_treatment_score": round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
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
