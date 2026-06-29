from fastapi import APIRouter, Query
from supabase import create_client
import os
import json
import requests
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import Counter
from pipeline.whatsapp import add_whatsapp_line_select, matches_business_line, resolve_agent_name

load_dotenv()
router = APIRouter()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"


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
    if range_ in ("week", "7d"):
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    if range_ in ("month", "30d"):
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    start = now - timedelta(days=now.weekday())
    return start.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def _digits_only(value: str | None) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _lead_phone(lead: dict | None) -> str | None:
    if not lead:
        return None
    phone = (lead.get("phone") or "").strip()
    if phone:
        return phone
    wa_contact_id = str(lead.get("wa_contact_id") or "").strip()
    if wa_contact_id and _digits_only(wa_contact_id):
        return wa_contact_id
    return None


def _lead_agent_name(lead: dict | None, latest_msg: dict | None = None) -> str | None:
    candidates = []
    if lead and lead.get("assigned_agent"):
        candidates.append(lead.get("assigned_agent"))
    if latest_msg and latest_msg.get("agent_name"):
        candidates.append(latest_msg.get("agent_name"))
    for value in candidates:
        if not value:
            continue
        if re.fullmatch(r"[0-9a-fA-F-]{36}", str(value).strip()):
            resolved = resolve_agent_name(value)
            if resolved and not re.fullmatch(r"[0-9a-fA-F-]{36}", str(resolved).strip()) and resolved.lower() not in ("unknown", "team", "n/a"):
                return resolved
        normalized = str(value).strip()
        if normalized and normalized.lower() not in ("unknown", "team", "n/a"):
            return normalized
    return None


def _topic_fallback_title(topic: str, examples: list[dict]) -> str:
    stop_en = {
        'the','and','for','with','from','this','that','into','your','our','their','about','have','has','had',
        'client','customer','lead','agent','message','messages','conversation','conversations','summary','summaries',
        'business','topic','topics','info','information','details',
    }
    stop_ar = {
        '??????','??????','????????','????????','????????','????????','?????','?????','????','??????','???????',
        '???','???','???','???','???','??','??','???','???','??','??','??','????','????','?????','????',
    }
    texts = [str((e.get('text') or '')).strip() for e in examples if (e.get('text') or '').strip()]
    if not texts:
        return topic.replace('_', ' ').title()
    best = texts[0]
    best_score = -1
    for txt in texts[:3]:
        tokens = re.findall(r"[\w\u0600-\u06FF]+", txt.lower())
        tokens = [tok for tok in tokens if tok not in stop_en and tok not in stop_ar and len(tok) > 1]
        score = len(tokens)
        if score > best_score:
            best_score = score
            best = txt
    tokens = re.findall(r"[\w\u0600-\u06FF]+", best)
    cleaned = []
    for tok in tokens:
        low = tok.lower()
        if low in stop_en or low in stop_ar or len(low) <= 1:
            continue
        cleaned.append(tok)
        if len(cleaned) >= 4:
            break
    if cleaned:
        candidate = ' '.join(cleaned)
        latin = sum(1 for ch in candidate if ch.isascii() and ch.isalpha())
        nonlatin = sum(1 for ch in candidate if not ch.isascii() and ch.isalpha())
        if latin >= nonlatin and len(candidate.split()) <= 4:
            return candidate
    return topic.replace('_', ' ').title()


def _topic_titles_from_examples(topic_specs: list[dict]) -> dict[str, str]:
    titles: dict[str, str] = {}
    if GEMINI_API_KEY and topic_specs:
        try:
            prompt_items = []
            for spec in topic_specs:
                examples = [e.get('text', '') for e in (spec.get('examples') or [])[:3] if e.get('text')]
                prompt_items.append({'key': spec['key'], 'examples': examples})
            response = requests.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                headers={'content-type': 'application/json'},
                json={
                    'contents': [{
                        'role': 'user',
                        'parts': [{
                            'text': (
                                'You are naming CRM topic groups for Fiper. '
                                'For each group, create a concise English topic title of 2-4 words based only on the examples. '
                                'Do not copy the examples verbatim. '
                                'Return JSON only as an array of objects with keys "key" and "title". '
                                'Avoid duplicates across titles and avoid fixed business labels unless the examples clearly support them.\n\n'
                                + json.dumps(prompt_items, ensure_ascii=False)
                            )
                        }],
                    }],
                    'generationConfig': {'temperature': 0.2, 'maxOutputTokens': 300},
                },
                timeout=25,
            )
            response.raise_for_status()
            raw_text = (
                response.json()
                .get('candidates', [{}])[0]
                .get('content', {})
                .get('parts', [{}])[0]
                .get('text', '')
                .strip()
            )
            raw_text = re.sub(r'^```json\s*', '', raw_text)
            raw_text = re.sub(r'^```\s*', '', raw_text)
            raw_text = re.sub(r'```$', '', raw_text).strip()
            parsed = json.loads(raw_text)
            if isinstance(parsed, dict):
                parsed = parsed.get('topics') or parsed.get('items') or parsed.get('data') or []
            if isinstance(parsed, list):
                for item in parsed:
                    key = str((item or {}).get('key') or '').strip()
                    title = str((item or {}).get('title') or '').strip()
                    if key and title:
                        latin = sum(1 for ch in title if ch.isascii() and ch.isalpha())
                        nonlatin = sum(1 for ch in title if not ch.isascii() and ch.isalpha())
                        if latin < nonlatin or len(title.split()) > 5:
                            continue
                        titles[key] = title
        except Exception:
            titles = {}

    for spec in topic_specs:
        key = spec['key']
        titles.setdefault(key, _topic_fallback_title(key, spec.get('examples') or []))
    return titles


@router.get("/api/quality")
def quality(
    range: str = Query("week", pattern="^(today|week|month|7d|30d)$"),
    wa_line: str = Query("all"),
):
    try:
        return _quality_inner(range, wa_line)
    except Exception as e:
        import logging
        logging.getLogger("fiper").error(f"/api/quality error ({range}): {e}", exc_info=True)
        return {"range": range, "alerts": {"total":0,"open":0,"by_severity":{"HIGH":0,"MED":0,"LOW":0},"list":[]}, "sentiment":{"positive":0,"neutral":0,"negative":0}, "top_topics":{}, "top_risk_flags":{}, "risk_flag_details":{}, "faq_topics":[], "avg_treatment_score":0, "call_outcomes":{}, "complaints_count":0, "avg_messages_per_lead":0}


def _quality_inner(range: str, wa_line: str = "all"):
    since = _since(range)

    alerts = (
        supabase.table("alerts")
        .select("*")
        .gte("created_at", since)
        .eq("resolved", False)
        .order("created_at", desc=True)
        .execute()
        .data
    ) or []

    # Enrich alerts with lead phone/name and resolve missing agent_name
    lead_map = {}
    alert_msg_map = {}
    all_alert_lead_ids = list({a["lead_id"] for a in alerts if a.get("lead_id")})
    if all_alert_lead_ids:
        lead_rows = (
            supabase.table("leads")
            .select("id,phone,wa_contact_id,name,assigned_agent,channel,whatsapp_business_number")
            .in_("id", all_alert_lead_ids)
            .execute()
            .data
        ) or []
        lead_map = {r["id"]: r for r in lead_rows}
        if wa_line and wa_line.lower() not in ("all", "*", "any"):
            alerts = [a for a in alerts if matches_business_line(lead_map.get(a.get("lead_id") or ""), wa_line)]
        alert_msg_map = {}
        try:
            alert_msg_rows = (
                supabase.table("messages")
                .select("lead_id,direction,body,agent_name,sent_at")
                .in_("lead_id", all_alert_lead_ids)
                .order("sent_at", desc=True)
                .limit(1000)
                .execute()
                .data
            ) or []
            for msg in alert_msg_rows:
                lead_id = msg.get("lead_id")
                if lead_id and lead_id not in alert_msg_map:
                    alert_msg_map[lead_id] = msg
        except Exception:
            alert_msg_map = {}
        for a in alerts:
            lead = lead_map.get(a.get("lead_id") or "")
            latest_msg = alert_msg_map.get(a.get("lead_id") or "", {})
            if lead:
                if not a.get("agent_name"):
                    a["agent_name"] = _lead_agent_name(lead, latest_msg)
                a["lead_phone"] = _lead_phone(lead)
                a["lead_name"]  = lead.get("name")

    # For open no_reply alerts, fetch the last unanswered inbound message body
    no_reply_open = [a for a in alerts if a.get("type") == "no_reply" and not a.get("resolved")]
    for a in no_reply_open:
        lead_id = a.get("lead_id")
        if not lead_id:
            continue
        try:
            msg = (
                supabase.table("messages")
                .select("body")
                .eq("lead_id", lead_id)
                .eq("direction", "inbound")
                .order("sent_at", desc=True)
                .limit(1)
                .execute()
                .data
            )
            if msg:
                body = (msg[0].get("body") or "").strip()
                a["last_message_body"] = body[:200] if body else None
        except Exception:
            pass

    analyses = (
        supabase.table("ai_analysis")
        .select("lead_id,sentiment,topics,treatment_score,risk_flags,analyzed_at,source,summary")
        .gte("analyzed_at", since)
        .execute()
        .data
    ) or []

    topic_example_rows = []
    try:
        topic_example_rows = (
            supabase.table("ai_analysis")
            .select("lead_id,topics,summary")
            .gte("analyzed_at", since)
            .execute()
            .data
        ) or []
    except Exception:
        topic_example_rows = []

    calls = _paginate(
        lambda: supabase.table("calls").select("id,outcome").gte("called_at", since)
    )

    messages = (
        supabase.table("messages")
        .select("lead_id,whatsapp_business_number")
        .gte("sent_at", since)
        .execute()
        .data
    ) or []
    if wa_line and wa_line.lower() not in ("all", "*", "any"):
        messages = [m for m in messages if matches_business_line(m, wa_line)]

    # Use explicit sentiment when available; derive from treatment_score otherwise.
    # WA analyses historically have NULL sentiment because the AI prompt didn't ask for it.
    sentiments = []
    for _a in analyses:
        _s = (_a.get("sentiment") or "").strip().lower()
        if _s in ("positive", "neutral", "negative"):
            sentiments.append(_s)
        elif _a.get("treatment_score") is not None:
            _ts = _a["treatment_score"]
            if _ts >= 70:
                sentiments.append("positive")
            elif _ts >= 40:
                sentiments.append("neutral")
            else:
                sentiments.append("negative")
    excluded_topics = {"greetings", "no_answer"}
    all_topics = [
        t
        for a in analyses
        for t in (a.get("topics") or [])
        if t not in excluded_topics
    ]
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

    topic_counter = Counter(all_topics)
    total_topic_count = sum(topic_counter.values())
    topic_specs = [{"key": topic, "count": count, "examples": []} for topic, count in topic_counter.most_common(10)]
    analysis_lead_ids = list({a["lead_id"] for a in analyses if a.get("lead_id")})
    analysis_lead_map = {}
    if analysis_lead_ids:
        try:
            lead_rows = (
                supabase.table("leads")
                .select("id,phone,wa_contact_id,name,assigned_agent,channel,whatsapp_business_number")
                .in_("id", analysis_lead_ids)
                .execute()
                .data
            ) or []
            analysis_lead_map = {r["id"]: r for r in lead_rows}
        except Exception:
            analysis_lead_map = {}

    latest_message_map = {}
    if analysis_lead_ids:
        try:
            msg_rows = (
                supabase.table("messages")
                .select("lead_id,direction,body,agent_name,sent_at")
                .in_("lead_id", analysis_lead_ids)
                .order("sent_at", desc=True)
                .limit(1000)
                .execute()
                .data
            ) or []
            for msg in msg_rows:
                lead_id = msg.get("lead_id")
                if lead_id and lead_id not in latest_message_map:
                    latest_message_map[lead_id] = msg
        except Exception:
            latest_message_map = {}

    if wa_line and wa_line.lower() not in ("all", "*", "any") and analysis_lead_map:
        analyses = [
            a for a in analyses
            if matches_business_line(analysis_lead_map.get(a.get("lead_id") or ""), wa_line)
        ]
        if topic_example_rows:
            topic_example_rows = [
                a for a in topic_example_rows
                if matches_business_line(analysis_lead_map.get(a.get("lead_id") or ""), wa_line)
            ]
        if latest_message_map:
            latest_message_map = {
                lid: msg for lid, msg in latest_message_map.items()
                if matches_business_line(analysis_lead_map.get(lid or ""), wa_line)
            }

    risk_flag_details: dict[str, list[dict]] = {}
    for analysis in sorted(analyses, key=lambda a: a.get("analyzed_at") or "", reverse=True):
        lead_id = analysis.get("lead_id")
        lead = analysis_lead_map.get(lead_id or "", {})
        latest_msg = latest_message_map.get(lead_id or "", {})
        summary = (
            analysis.get("summary")
            or ""
        ).strip()
        message_body = (latest_msg.get("body") or "").strip()
        if len(summary) > 320:
            summary = summary[:317].rstrip() + "..."
        if len(message_body) > 320:
            message_body = message_body[:317].rstrip() + "..."
        for flag in analysis.get("risk_flags") or []:
            bucket = risk_flag_details.setdefault(flag, [])
            if len(bucket) >= 10:
                continue
            bucket.append({
                "lead_id": lead_id,
                "lead_phone": _lead_phone(lead),
                "lead_name": lead.get("name"),
                "agent_name": _lead_agent_name(lead, latest_msg),
                "source": analysis.get("source"),
                "analyzed_at": analysis.get("analyzed_at"),
                "sentiment": analysis.get("sentiment"),
                "treatment_score": analysis.get("treatment_score"),
                "summary": summary,
                "message": message_body,
                "message_direction": latest_msg.get("direction"),
                "message_at": latest_msg.get("sent_at"),
            })

    alert_flag_aliases = {
        "no_reply": "unanswered",
        "unanswered": "unanswered",
        "slow_response": "slow_response",
        "beginner_risk": "beginner_risk",
        "negative_sentiment": "negative_sentiment",
        "poor_treatment": "negative_sentiment",
        "stale_callback": "stale_callback",
    }
    for a in alerts:
        flag = alert_flag_aliases.get(str(a.get("type") or "").strip().lower())
        if not flag:
            continue
        bucket = risk_flag_details.setdefault(flag, [])
        if len(bucket) >= 3:
            continue
        lead_id = a.get("lead_id")
        lead = lead_map.get(lead_id or "", {})
        latest_msg = alert_msg_map.get(lead_id or "", {})
        if any(
            e.get("lead_id") == lead_id and e.get("message") == (a.get("message") or "").strip()
            for e in bucket
        ):
            continue
        bucket.append({
            "lead_id": lead_id,
            "lead_phone": _lead_phone(lead),
            "lead_name": lead.get("name"),
            "agent_name": a.get("agent_name") or _lead_agent_name(lead, latest_msg),
            "source": "alert",
            "analyzed_at": a.get("created_at"),
            "sentiment": None,
            "treatment_score": None,
            "summary": (a.get("message") or "").strip(),
            "message": (latest_msg.get("body") or "").strip(),
            "message_direction": latest_msg.get("direction"),
            "message_at": latest_msg.get("sent_at"),
        })

    topic_examples: dict[str, list[dict]] = {}
    for analysis in topic_example_rows:
        summary = (
            analysis.get("summary")
            or ""
        ).strip()
        if not summary:
            continue
        if len(summary) > 260:
            summary = summary[:257].rstrip() + "..."
        candidate_topics = [t for t in (analysis.get("topics") or []) if t not in excluded_topics]
        if not candidate_topics:
            continue
        candidate_topics = sorted(
            candidate_topics,
            key=lambda t: (len(topic_examples.get(t, [])), -topic_counter.get(t, 0), t),
        )
        for topic in candidate_topics:
            examples = topic_examples.setdefault(topic, [])
            if len(examples) >= 4:
                continue
            if any(e.get("lead_id") == analysis.get("lead_id") and e.get("text") == summary for e in examples):
                continue
            examples.append({"lead_id": analysis.get("lead_id"), "text": summary})
            break

    for spec in topic_specs:
        spec["examples"] = topic_examples.get(spec["key"], [])
    topic_titles = _topic_titles_from_examples(topic_specs)

    faq_topics = [
        {
            "key":   topic,
            "title": topic_titles.get(topic, topic.replace("_", " ").title()),
            "count": count,
            "pct":   round(count / total_topic_count * 100, 1) if total_topic_count else 0,
            "examples": topic_examples.get(topic, []),
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
        "top_topics": dict(topic_counter.most_common(10)),
        "topic_titles": topic_titles,
        "top_risk_flags": dict(Counter(all_risk_flags).most_common(10)),
        "risk_flag_details": risk_flag_details,
        "faq_topics": faq_topics,
        "avg_treatment_score": round(sum(treatment_scores) / len(treatment_scores), 1) if treatment_scores else 0,
        "call_outcomes": call_outcomes,
        "complaints_count": sentiments.count("negative"),
        "avg_messages_per_lead": round(
            len(messages) / len({m["lead_id"] for m in messages if m.get("lead_id")}), 1
        ) if messages and any(m.get("lead_id") for m in messages) else 0,
    }


@router.get("/api/quality/summary")
def quality_summary(range: str = Query("week", pattern="^(today|week|month|7d|30d)$")):
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

    if not GEMINI_API_KEY:
        return {"range": range, "summary": "GEMINI_API_KEY is missing."}

    response = requests.post(
        f"{GEMINI_URL}?key={GEMINI_API_KEY}",
        headers={"content-type": "application/json"},
        json={
            "contents": [{
                "role": "user",
                "parts": [{
                    "text": f"You are a CRM analytics assistant for Fiper, a trading broker. Write a concise weekly quality summary (3-4 bullet points) for the management team based on this data. Be direct and actionable. Use English.\n\n{context}"
                }],
            }],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 500,
            },
        },
        timeout=30,
    )
    response.raise_for_status()
    summary = (
        response.json()
        .get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
    )

    return {"range": range, "summary": summary}
