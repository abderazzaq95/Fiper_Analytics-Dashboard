from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import html
import os
import re

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "Fiper Alerts <onboarding@resend.dev>")
SALES_SUPERVISOR_EMAIL = os.getenv("SALES_SUPERVISOR_EMAIL", "")


def _agent_env_key(agent_name: str | None) -> str:
    slug = re.sub(r"[^A-Z0-9]+", "_", (agent_name or "").upper()).strip("_")
    return f"AGENT_EMAIL_{slug}" if slug else ""


def _send_email(to: str, subject: str, html_body: str) -> bool:
    if not (RESEND_API_KEY and to):
        return False
    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": EMAIL_FROM,
            "to": [to],
            "subject": subject,
            "html": html_body,
        },
        timeout=20,
    )
    response.raise_for_status()
    return True


def notify_agent_alert(alert: dict) -> bool:
    agent = alert.get("agent_name") or "unknown"
    recipient = os.getenv(_agent_env_key(agent), "")
    if not recipient:
        return False

    severity = alert.get("severity") or "Alert"
    alert_type = (alert.get("type") or "alert").replace("_", " ").title()
    message = alert.get("message") or ""
    lead_id = alert.get("lead_id") or ""
    html_body = f"""
    <h2>New Fiper Alert</h2>
    <p><b>Agent:</b> {html.escape(agent)}</p>
    <p><b>Severity:</b> {html.escape(severity)}</p>
    <p><b>Type:</b> {html.escape(alert_type)}</p>
    <p><b>Message:</b> {html.escape(message)}</p>
    <p><b>Lead ID:</b> {html.escape(str(lead_id))}</p>
    """
    return _send_email(recipient, f"Fiper Alert - {severity} - {alert_type}", html_body)


def send_supervisor_report(report_label: str = "") -> bool:
    if not SALES_SUPERVISOR_EMAIL:
        return False

    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    alerts = (
        supabase.table("alerts")
        .select("severity,type,agent_name,message,created_at,resolved")
        .gte("created_at", today)
        .execute()
        .data
    ) or []
    open_alerts = [a for a in alerts if not a.get("resolved")]

    calls = (
        supabase.table("calls")
        .select("id,outcome,duration_seconds,called_at")
        .gte("called_at", today)
        .execute()
        .data
    ) or []
    messages = (
        supabase.table("messages")
        .select("id,direction,sent_at")
        .gte("sent_at", today)
        .execute()
        .data
    ) or []
    leads = (
        supabase.table("leads")
        .select("id,channel,created_at,last_message_at")
        .gte("created_at", today)
        .execute()
        .data
    ) or []

    severity_counts = Counter(a.get("severity") or "UNKNOWN" for a in open_alerts)
    type_counts = Counter(a.get("type") or "unknown" for a in open_alerts)
    by_agent: dict[str, list] = defaultdict(list)
    for alert in open_alerts:
        by_agent[alert.get("agent_name") or "unknown"].append(alert)

    completed = sum(1 for c in calls if (c.get("outcome") or "").lower() == "completed")
    no_answer = sum(1 for c in calls if (c.get("outcome") or "").lower() == "no_answer")
    busy = sum(1 for c in calls if (c.get("outcome") or "").lower() == "busy")
    pickup_base = completed + no_answer + busy
    pickup_rate = round(completed / pickup_base * 100, 1) if pickup_base else 0
    avg_duration = round(
        sum(c.get("duration_seconds") or 0 for c in calls) / len(calls), 1
    ) if calls else 0

    agent_rows = "".join(
        f"<li>{html.escape(agent)}: {len(items)} open alerts</li>"
        for agent, items in sorted(by_agent.items(), key=lambda kv: len(kv[1]), reverse=True)[:10]
    )
    type_rows = "".join(
        f"<li>{html.escape(k.replace('_', ' ').title())}: {v}</li>"
        for k, v in type_counts.most_common(10)
    )

    html_body = f"""
    <h2>Fiper Sales Report {html.escape(report_label)}</h2>
    <p><b>UTC time:</b> {now.strftime('%Y-%m-%d %H:%M')}</p>
    <h3>Work today</h3>
    <ul>
      <li>New leads: {len(leads)}</li>
      <li>Total calls: {len(calls)}</li>
      <li>Pickup rate: {pickup_rate}%</li>
      <li>Avg call duration: {avg_duration}s</li>
      <li>Stored WhatsApp messages: {len(messages)}</li>
      <li>Inbound / outbound messages: {sum(1 for m in messages if m.get('direction') == 'inbound')} / {sum(1 for m in messages if m.get('direction') == 'outbound')}</li>
    </ul>
    <h3>Alerts today</h3>
    <ul>
      <li>Open alerts: {len(open_alerts)}</li>
      <li>High: {severity_counts.get('HIGH', 0)}</li>
      <li>Medium: {severity_counts.get('MED', 0)}</li>
      <li>Low: {severity_counts.get('LOW', 0)}</li>
    </ul>
    <h3>Alert types</h3>
    <ul>{type_rows or '<li>No alerts</li>'}</ul>
    <h3>Agents with alerts</h3>
    <ul>{agent_rows or '<li>No agent alerts</li>'}</ul>
    """
    return _send_email(
        SALES_SUPERVISOR_EMAIL,
        f"Fiper Sales Report {report_label}".strip(),
        html_body,
    )
