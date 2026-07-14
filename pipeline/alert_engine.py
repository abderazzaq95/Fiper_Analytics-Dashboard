from datetime import datetime, timezone
from supabase import create_client
import os
import re
from dotenv import load_dotenv
from pipeline import email_notifications

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY")
)

MAX_OPERATIONAL_ALERT_MINUTES = 24 * 60


def _paginate(build_query) -> list:
    rows: list = []
    offset = 0
    while True:
        batch = build_query().range(offset, offset + 999).execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def _fmt_duration(minutes: float) -> str:
    m = int(minutes)
    if m < 60:
        return f"{m} min"
    if m < 1440:
        h, rem = divmod(m, 60)
        return f"{h}h {rem}m" if rem else f"{h}h"
    days = m // 1440
    h = (m % 1440) // 60
    return f"{days}d {h}h" if h else f"{days}d"

_BANAL_NO_REPLY_MESSAGES = {
    "ok",
    "okay",
    "yes",
    "no",
    "thanks",
    "thank you",
    "done",
    "\u062a\u0645\u0627\u0645",  # تمام
    "\u062a\u0645",  # تم
    "\u0646\u0639\u0645",  # نعم
    "\u0627\u064a",  # اي
    "\u0625\u064a",  # إي
    "\u0627\u064a\u0648\u0647",  # ايوه
    "\u0623\u064a\u0648\u0647",  # أيوه
    "\u0627\u0648\u0643\u064a",  # اوكي
    "\u0623\u0648\u0643\u064a",  # أوكي
    "\u0627\u0648\u0643",  # اوك
    "\u0623\u0648\u0643",  # أوك
    "\u0634\u0643\u0631\u0627",  # شكرا
    "\u0634\u0643\u0631\u0627\u064b",  # شكراً
    "\u0645\u0634\u0643\u0648\u0631",  # مشكور
    "\u0645\u0634\u0643\u0648\u0631\u0647",  # مشكوره
    "\u062d\u0633\u0646\u0627",  # حسنا
}


def _is_banal_no_reply_message(body: str | None) -> bool:
    text = (body or "").strip()
    if not text:
        return True

    normalized = " ".join(text.lower().replace("\u0640", "").split())
    normalized = normalized.strip(" .,!\u061f?\u061b;:()[]{}\"'`~|/\\")

    if normalized in {"[reaction]", "[sticker]", "[image]", "[video]", "reaction", "sticker", "image", "video"}:
        return True
    if normalized in _BANAL_NO_REPLY_MESSAGES:
        return True
    if re.fullmatch(r"[\d\u0660-\u0669]+", normalized):
        return True
    if len(normalized) <= 4 and not any(ch.isalnum() for ch in normalized):
        return True

    compact = re.sub(r"[\W_]+", " ", normalized, flags=re.UNICODE).strip()
    banal_prefixes = (
        "ok", "okay", "thanks", "thank you", "done", "yes", "no",
        "\u062a\u0645", "\u062a\u0645\u0627\u0645", "\u0646\u0639\u0645", "\u0627\u064a", "\u0625\u064a",
        "\u0627\u064a\u0648\u0647", "\u0623\u064a\u0648\u0647", "\u0627\u0648\u0643\u064a", "\u0623\u0648\u0643\u064a",
        "\u0634\u0643\u0631\u0627", "\u0634\u0643\u0631\u0627\u064b", "\u0634\u0643\u0631\u0627 \u0627\u0633\u062a\u0627\u0630",
        "\u0645\u0634\u0643\u0648\u0631", "\u0645\u0634\u0643\u0648\u0631\u0647", "\u062a\u0633\u0644\u0645", "\u062d\u0633\u0646\u0627",
    )
    if len(compact) <= 24 and any(compact == p or compact.startswith(p + " ") for p in banal_prefixes):
        return True
    return False
def _upsert_alert(lead_id, agent_name, severity, alert_type, message):
    existing = (
        supabase.table("alerts")
        .select("id,message")
        .eq("lead_id", lead_id)
        .eq("type", alert_type)
        .eq("resolved", False)
        .execute()
    )
    if existing.data:
        # Update the message in place so it always reflects the latest duration
        existing_id = existing.data[0]["id"]
        if existing.data[0].get("message") != message:
            supabase.table("alerts").update({"message": message, "agent_name": agent_name}).eq("id", existing_id).execute()
        return
    alert = {
        "lead_id": lead_id,
        "agent_name": agent_name,
        "severity": severity,
        "type": alert_type,
        "message": message,
    }
    supabase.table("alerts").insert(alert).execute()
    try:
        email_notifications.notify_agent_alert(alert)
    except Exception:
        pass


def _lead_ids_for_phone(phone: str | None) -> set[str]:
    if not phone:
        return set()
    rows = (
        supabase.table("leads")
        .select("id")
        .eq("channel", "whatsapp")
        .eq("phone", phone)
        .execute()
        .data
        or []
    )
    lead_ids = {row["id"] for row in rows if row.get("id")}
    if not lead_ids:
        rows = (
            supabase.table("leads")
            .select("id")
            .eq("channel", "whatsapp")
            .eq("wa_contact_id", phone)
            .execute()
            .data
            or []
        )
        lead_ids.update(row["id"] for row in rows if row.get("id"))
    return lead_ids


def _lead_row_map_for_phone(phone: str | None) -> dict[str, dict]:
    if not phone:
        return {}
    rows = (
        supabase.table("leads")
        .select("id,phone,wa_contact_id,assigned_agent,updated_at")
        .eq("channel", "whatsapp")
        .eq("phone", phone)
        .execute()
        .data
        or []
    )
    if not rows:
        rows = (
            supabase.table("leads")
            .select("id,phone,wa_contact_id,assigned_agent,updated_at")
            .eq("channel", "whatsapp")
            .eq("wa_contact_id", phone)
            .execute()
            .data
            or []
        )
    return {row["id"]: row for row in rows if row.get("id")}


def resolve_no_reply(lead_id: str | None = None, phone: str | None = None):
    """Resolve any open no_reply alert for this conversation.

    We resolve by both lead_id and phone so duplicate lead rows for the same
    WhatsApp number cannot leave an answered conversation stuck open.
    """
    lead_ids: set[str] = set()
    if lead_id:
        lead_ids.add(lead_id)
        row = (
            supabase.table("leads")
            .select("phone,wa_contact_id")
            .eq("id", lead_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if row:
            lead_phone = row[0].get("phone") or row[0].get("wa_contact_id")
            lead_ids.update(_lead_ids_for_phone(lead_phone))
    if phone:
        lead_ids.update(_lead_ids_for_phone(phone))

    if not lead_ids:
        return

    supabase.table("alerts").update({"resolved": True}).in_("lead_id", list(lead_ids)).eq("type", "no_reply").eq("resolved", False).execute()


def check_no_reply():
    """HIGH: inbound message older than 60 min with no subsequent outbound reply.
    Also auto-resolves existing alerts for leads that have since been answered."""
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - (MAX_OPERATIONAL_ALERT_MINUTES * 60)
    cutoff_iso = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
    messages = _paginate(
        lambda: supabase.table("messages")
        .select("lead_id,sent_at,direction,body")
        .gte("sent_at", cutoff_iso)
        .order("sent_at", desc=False)
    )

    leads = _paginate(
        lambda: supabase.table("leads")
        .select("id,phone,wa_contact_id,assigned_agent,updated_at")
        .eq("channel", "whatsapp")
    )

    lead_phone_map: dict[str, str] = {}
    phone_leads: dict[str, list[dict]] = {}
    for lead in leads:
        phone = lead.get("phone") or lead.get("wa_contact_id")
        if not phone:
            continue
        lead_phone_map[lead["id"]] = phone
        phone_leads.setdefault(phone, []).append(lead)

    by_phone: dict[str, list[dict]] = {}
    for m in messages:
        phone = lead_phone_map.get(m.get("lead_id"))
        if not phone:
            continue
        by_phone.setdefault(phone, []).append(m)

    # Track which leads still have unanswered inbounds
    still_unanswered_phones: set[str] = set()

    for phone, msgs in by_phone.items():
        sorted_msgs = sorted(msgs, key=lambda x: x["sent_at"])
        last_inbound = None
        for m in sorted_msgs:
            if m["direction"] == "inbound":
                if _is_banal_no_reply_message(m.get("body")):
                    continue
                last_inbound = m
            elif m["direction"] == "outbound" and last_inbound:
                last_inbound = None

        if last_inbound:
            sent = datetime.fromisoformat(last_inbound["sent_at"].replace("Z", "+00:00"))
            gap_min = (now - sent).total_seconds() / 60
            if 60 < gap_min <= MAX_OPERATIONAL_ALERT_MINUTES:
                still_unanswered_phones.add(phone)
                lead_rows = phone_leads.get(phone, [])
                lead_rows = sorted(
                    lead_rows,
                    key=lambda row: row.get("updated_at") or "",
                    reverse=True,
                )
                lead_id = lead_rows[0].get("id") if lead_rows else None
                agent = (lead_rows[0].get("assigned_agent") if lead_rows else None) or "unknown"
                if not lead_id:
                    continue
                _upsert_alert(
                    lead_id, agent, "HIGH", "no_reply",
                    f"Inbound message unanswered for {_fmt_duration(gap_min)}."
                )

    # Auto-resolve alerts for leads that have now been answered
    open_alerts = _paginate(
        lambda: supabase.table("alerts")
        .select("id,lead_id")
        .eq("type", "no_reply")
        .eq("resolved", False)
    )
    resolved_ids = []
    for alert in open_alerts:
        phone = lead_phone_map.get(alert["lead_id"])
        if phone and phone not in still_unanswered_phones:
            resolved_ids.append(alert["id"])
    if resolved_ids:
        supabase.table("alerts").update({"resolved": True}).in_("id", resolved_ids).execute()


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


def check_negative_sentiment(leads_with_msgs: set):
    """MED: AI analysis returned negative sentiment — only for leads with real messages."""
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,sentiment")
        .eq("sentiment", "negative")
        .execute()
        .data
    )
    for a in analyses:
        if a["lead_id"] not in leads_with_msgs:
            continue
        lead = supabase.table("leads").select("assigned_agent").eq("id", a["lead_id"]).single().execute().data
        agent = (lead.get("assigned_agent") if lead else None) or "unknown"
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

    qualifying_leads: set[str] = set()

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
                if 0 < gap <= MAX_OPERATIONAL_ALERT_MINUTES:
                    gaps.append(gap)
                last_inbound_time = None

        if gaps and (sum(gaps) / len(gaps)) > 15:
            agent = sorted_msgs[-1].get("agent_name", "unknown")
            qualifying_leads.add(lead_id)
            _upsert_alert(
                lead_id, agent, "MED", "slow_response",
                f"Average response time is {_fmt_duration(sum(gaps)/len(gaps))} (threshold: 15 min)."
            )

    open_alerts = (
        supabase.table("alerts")
        .select("id,lead_id")
        .eq("type", "slow_response")
        .eq("resolved", False)
        .execute()
        .data or []
    )
    resolved_ids = [
        alert["id"]
        for alert in open_alerts
        if alert.get("lead_id") not in qualifying_leads
    ]
    if resolved_ids:
        supabase.table("alerts").update({"resolved": True}).in_("id", resolved_ids).execute()


def check_profit_expectations(leads_with_msgs: set):
    """MED: AI detected profit_expectations topic — only for leads with real messages."""
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,topics")
        .execute()
        .data
    )
    for a in analyses:
        if a["lead_id"] not in leads_with_msgs:
            continue
        if "profit_expectations" in (a.get("topics") or []):
            lead = supabase.table("leads").select("assigned_agent").eq("id", a["lead_id"]).single().execute().data
            agent = (lead.get("assigned_agent") if lead else None) or "unknown"
            _upsert_alert(
                a["lead_id"], agent, "MED", "profit_expectations",
                "Lead raised unrealistic profit expectations. Training flag required."
            )


def check_beginner_risk(leads_with_msgs: set):
    """MED: beginner lead (score < 30) with no trading education — only for leads with real messages."""
    leads = (
        supabase.table("leads")
        .select("id,assigned_agent,score")
        .lt("score", 30)
        .execute()
        .data
    )
    for lead in leads:
        if lead["id"] not in leads_with_msgs:
            continue
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


def check_poor_treatment(leads_with_msgs: set):
    """MED: AI treatment_score < 50 — only for leads with real messages."""
    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,treatment_score")
        .lt("treatment_score", 50)
        .execute()
        .data
    )
    for a in analyses:
        if a["lead_id"] not in leads_with_msgs:
            continue
        lead = supabase.table("leads").select("assigned_agent").eq("id", a["lead_id"]).single().execute().data
        agent = (lead.get("assigned_agent") if lead else None) or "unknown"
        _upsert_alert(
            a["lead_id"], agent, "MED", "poor_treatment",
            f"Low treatment score ({a['treatment_score']}/100). Agent interaction quality needs review."
        )


def check_weak_engagement():
    """LOW: 75%+ of conversations have fewer than 3 messages. Fires at most once (deduped)."""
    # Dedup: skip if an open weak_engagement team alert already exists
    existing = (
        supabase.table("alerts")
        .select("id")
        .is_("lead_id", "null")
        .eq("type", "weak_engagement")
        .eq("resolved", False)
        .execute()
    )
    if existing.data:
        return

    conversations = supabase.table("leads").select("id,assigned_agent").execute().data
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

    if (low_engagement / total) >= 0.75:
        supabase.table("alerts").insert({
            "lead_id": None,
            "agent_name": "team",
            "severity": "LOW",
            "type": "weak_engagement",
            "message": f"{low_engagement}/{total} conversations have fewer than 3 messages.",
        }).execute()


def run_all_checks():
    # Pre-fetch leads that have at least one real message — used as guard
    # for conversation-quality checks so Maqsam no-answer calls don't trigger them.
    leads_with_msgs = {
        m["lead_id"]
        for m in (supabase.table("messages").select("lead_id").execute().data or [])
    }

    check_no_reply()
    check_stale_callbacks()
    check_negative_sentiment(leads_with_msgs)
    check_slow_response()
    check_profit_expectations(leads_with_msgs)
    check_beginner_risk(leads_with_msgs)
    check_poor_treatment(leads_with_msgs)
    check_weak_engagement()
