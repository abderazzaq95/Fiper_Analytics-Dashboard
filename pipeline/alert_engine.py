from datetime import datetime, timezone
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY")
)


def _upsert_alert(lead_id: str, agent_name: str, severity: str, alert_type: str, message: str):
    existing = (
        supabase.table("alerts")
        .select("id")
        .eq("lead_id", lead_id)
        .eq("type", alert_type)
        .eq("resolved", False)
        .execute()
    )
    if existing.data:
        return
    supabase.table("alerts").insert({
        "lead_id": lead_id,
        "agent_name": agent_name,
        "severity": severity,
        "type": alert_type,
        "message": message,
    }).execute()


def check_no_reply():
    """HIGH: inbound message older than 60 min with no subsequent outbound reply."""
    now = datetime.now(timezone.utc)
    messages = supabase.table("messages").select("lead_id,sent_at,direction").execute().data

    by_lead: dict[str, list[dict]] = {}
    for m in messages:
        by_lead.setdefault(m["lead_id"], []).append(m)

    for lead_id, msgs in by_lead.items():
        sorted_msgs = sorted(msgs, key=lambda x: x["sent_at"])
        last_inbound = None
        for m in sorted_msgs:
            if m["direction"] == "inbound":
                last_inbound = m
            elif m["direction"] == "outbound" and last_inbound:
                last_inbound = None

        if last_inbound:
            sent = datetime.fromisoformat(last_inbound["sent_at"].replace("Z", "+00:00"))
            gap_min = (now - sent).total_seconds() / 60
            if gap_min > 60:
                lead = supabase.table("leads").select("assigned_agent").eq("id", lead_id).single().execute().data
                agent = lead.get("assigned_agent", "unknown") if lead else "unknown"
                _upsert_alert(
                    lead_id, agent, "HIGH", "no_reply",
                    f"Inbound message unanswered for {int(gap_min)} minutes."
                )


def check_stale_callbacks():
    """HIGH: lead status=callback not updated in 48h."""
    callbacks = (
        supabase.table("leads")
        .select("id,assigned_agent,updated_at")
        .eq("status", "callback")
        .execute()
        .data
    )
    now = datetime.now(timezone.utc)
    for lead in callbacks:
        updated = datetime.fromisoformat(lead["updated_at"].replace("Z", "+00:00"))
        hours = (now - updated).total_seconds() / 3600
        if hours > 48:
            _upsert_alert(
                lead["id"], lead.get("assigned_agent", "unknown"),
                "HIGH", "stale_callback",
                f"Callback lead not contacted in {int(hours)} hours."
            )


def check_negative_sentiment():
    """MED: AI analysis returned negative sentiment."""
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,sentiment")
        .eq("sentiment", "negative")
        .execute()
        .data
    )
    for a in analyses:
        lead = supabase.table("leads").select("assigned_agent").eq("id", a["lead_id"]).single().execute().data
        agent = lead.get("assigned_agent", "unknown") if lead else "unknown"
        _upsert_alert(
            a["lead_id"], agent, "MED", "negative_sentiment",
            "AI analysis detected negative sentiment in this conversation."
        )


def check_slow_response():
    """MED: agent average response time > 15 min (checked per lead conversation)."""
    messages = (
        supabase.table("messages")
        .select("lead_id,direction,sent_at,agent_name")
        .execute()
        .data
    )
    by_lead: dict[str, list[dict]] = {}
    for m in messages:
        by_lead.setdefault(m["lead_id"], []).append(m)

    for lead_id, msgs in by_lead.items():
        sorted_msgs = sorted(msgs, key=lambda x: x["sent_at"])
        gaps = []
        last_inbound_time = None
        for m in sorted_msgs:
            if m["direction"] == "inbound":
                last_inbound_time = datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))
            elif m["direction"] == "outbound" and last_inbound_time:
                outbound_time = datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))
                gap = (outbound_time - last_inbound_time).total_seconds() / 60
                if gap > 0:
                    gaps.append(gap)
                last_inbound_time = None

        if gaps and (sum(gaps) / len(gaps)) > 15:
            agent = sorted_msgs[-1].get("agent_name", "unknown")
            _upsert_alert(
                lead_id, agent, "MED", "slow_response",
                f"Average response time is {int(sum(gaps)/len(gaps))} minutes (threshold: 15 min)."
            )


def check_profit_expectations():
    """MED: AI detected profit_expectations topic."""
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,topics")
        .execute()
        .data
    )
    for a in analyses:
        if "profit_expectations" in (a.get("topics") or []):
            lead = supabase.table("leads").select("assigned_agent").eq("id", a["lead_id"]).single().execute().data
            agent = lead.get("assigned_agent", "unknown") if lead else "unknown"
            _upsert_alert(
                a["lead_id"], agent, "MED", "profit_expectations",
                "Lead raised unrealistic profit expectations. Training flag required."
            )


def check_beginner_risk():
    """MED: beginner lead (score < 30) with no onboarding topic detected."""
    leads = (
        supabase.table("leads")
        .select("id,assigned_agent,score")
        .lt("score", 30)
        .execute()
        .data
    )
    for lead in leads:
        analyses = (
            supabase.table("ai_analysis")
            .select("topics")
            .eq("lead_id", lead["id"])
            .execute()
            .data
        )
        all_topics = [t for a in analyses for t in (a.get("topics") or [])]
        if "trading_education" not in all_topics:
            _upsert_alert(
                lead["id"], lead.get("assigned_agent", "unknown"),
                "MED", "beginner_risk",
                "Beginner lead with no trading education or onboarding offered."
            )


def check_poor_treatment():
    """MED: AI treatment_score < 50."""
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,treatment_score")
        .lt("treatment_score", 50)
        .execute()
        .data
    )
    for a in analyses:
        lead = supabase.table("leads").select("assigned_agent").eq("id", a["lead_id"]).single().execute().data
        agent = lead.get("assigned_agent", "unknown") if lead else "unknown"
        _upsert_alert(
            a["lead_id"], agent, "MED", "poor_treatment",
            f"Low treatment score ({a['treatment_score']}/100). Agent interaction quality needs review."
        )


def check_weak_engagement():
    """LOW: 75%+ of conversations have fewer than 3 messages."""
    conversations = (
        supabase.table("leads")
        .select("id,assigned_agent")
        .execute()
        .data
    )
    total = len(conversations)
    if total == 0:
        return

    low_engagement = 0
    for lead in conversations:
        count = (
            supabase.table("messages")
            .select("id", count="exact")
            .eq("lead_id", lead["id"])
            .execute()
            .count
        )
        if (count or 0) < 3:
            low_engagement += 1

    if total > 0 and (low_engagement / total) >= 0.75:
        supabase.table("alerts").insert({
            "lead_id": None,
            "agent_name": "team",
            "severity": "LOW",
            "type": "weak_engagement",
            "message": f"{low_engagement}/{total} conversations have fewer than 3 messages.",
        }).execute()


def run_all_checks():
    check_no_reply()
    check_stale_callbacks()
    check_negative_sentiment()
    check_slow_response()
    check_profit_expectations()
    check_beginner_risk()
    check_poor_treatment()
    check_weak_engagement()
