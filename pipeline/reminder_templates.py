"""Email / Telegram / WhatsApp templates for reply reminders.

Pure rendering (no database or network) so it can be previewed and tested
without credentials. Arabic first, short English line underneath.

Email HTML uses tables + inline styles only, so it renders the same in Gmail,
Outlook and mobile clients. Phone numbers are wrapped in dir="ltr" and customer
messages use dir="auto" so mixed Arabic/English text never scrambles.
"""
from __future__ import annotations

import html
import os
import re
from datetime import datetime, tzinfo


def _logo_url() -> str:
    # Served by this app (static/email/fiper-logo.png). RENDER_EXTERNAL_URL is
    # set automatically on Render; EMAIL_LOGO_URL overrides it.
    explicit = os.getenv("EMAIL_LOGO_URL", "").strip()
    if explicit:
        return explicit
    base = os.getenv("RENDER_EXTERNAL_URL", "https://fiper-analytics-dashboard.onrender.com").rstrip("/")
    return f"{base}/static/email/fiper-logo.png"

# Brand (matches static/index.html)
BLUE = "#0774FF"
BLUE_DARK = "#1262FF"
NAVY = "#2D3C58"
TEXT = "#4C5671"
MUTED = "#8D9097"
BG = "#F6F9FF"
BORDER = "#E9EDFF"
AMBER_BG, AMBER_FG = "#FFF4E0", "#B45309"
RED_BG, RED_FG = "#FDECEC", "#C62828"
FONT = "'Segoe UI', Tahoma, Arial, sans-serif"

LINES = {
    "96897245526": ("عُمان", "Oman", "🇴🇲"),
    "905318880855": ("تركيا", "Turkey", "🇹🇷"),
}


# ── Small formatting helpers ─────────────────────────────────────────────────

def _e(value) -> str:
    return html.escape(str(value or ""))


def short(text: str | None, limit: int = 220) -> str:
    value = re.sub(r"\s+", " ", (text or "").strip())
    return value if len(value) <= limit else value[: limit - 1] + "…"


def wait_ar(minutes: float) -> str:
    m = int(round(minutes))
    if m < 60:
        return f"{m} دقيقة"
    h, rem = divmod(m, 60)
    return f"{h} س {rem} د" if rem else f"{h} س"


def wait_en(minutes: float) -> str:
    m = int(round(minutes))
    if m < 60:
        return f"{m} min"
    h, rem = divmod(m, 60)
    return f"{h}h {rem}m" if rem else f"{h}h"


def local_time(dt: datetime | None, tz: tzinfo) -> str:
    return dt.astimezone(tz).strftime("%d/%m %H:%M") if dt else "-"


def line_labels(line: str | None) -> tuple[str, str, str]:
    digits = re.sub(r"\D+", "", line or "")
    return LINES.get(digits, (digits, digits, ""))


def phone_display(phone: str | None) -> str:
    if not phone or phone.startswith("lead:"):
        return ""
    return f"+{phone}"


def urgency(minutes: float, stage: int) -> tuple[str, str]:
    return (RED_BG, RED_FG) if stage >= 2 or minutes >= 45 else (AMBER_BG, AMBER_FG)


# ── Email building blocks ────────────────────────────────────────────────────

def _shell(*, preheader: str, accent: str, icon: str, title_ar: str, title_en: str, body: str,
           footer_ar: str, footer_en: str, label: str = "", gradient: str = "") -> str:
    # Solid colour first; the gradient is a progressive enhancement (Outlook ignores it).
    header_bg = f"background:{accent};" + (f"background-image:{gradient};" if gradient else "")
    label_html = (
        f"<div style='display:inline-block;background:#FFD54F;color:{NAVY};font-weight:700;"
        f"font-size:12px;padding:3px 10px;border-radius:20px;margin-bottom:8px'>{_e(label)}</div><br>"
        if label else ""
    )
    return f"""<!doctype html>
<html lang="ar" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_e(title_ar)}</title></head>
<body style="margin:0;padding:0;background:{BG};font-family:{FONT};color:{TEXT}">
<div style="display:none;max-height:0;overflow:hidden;opacity:0">{_e(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{BG}">
<tr><td align="center" style="padding:24px 12px">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:620px;background:#FFFFFF;border:1px solid {BORDER};border-radius:14px;overflow:hidden">
    <tr><td style="background:#FFFFFF;padding:16px 24px;border-bottom:1px solid {BORDER}">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
        <td align="left" dir="ltr"><img src="{_e(_logo_url())}" width="130" height="48" alt="Fiper"
            style="display:block;border:0;outline:none;width:130px;height:48px;font-family:{FONT};font-size:22px;font-weight:700;color:{NAVY}"></td>
        <td align="right" style="font-size:12px;color:{MUTED}" dir="rtl">تنبيهات المبيعات<br><span dir="ltr">Sales alerts</span></td>
      </tr></table>
    </td></tr>
    <tr><td style="{header_bg}padding:20px 24px;color:#FFFFFF" dir="rtl">
      {label_html}
      <div style="font-size:22px;font-weight:700">{icon} {_e(title_ar)}</div>
      <div style="font-size:14px;opacity:.9;margin-top:2px" dir="ltr" align="right">{_e(title_en)}</div>
    </td></tr>
    <tr><td style="padding:22px 24px" dir="rtl">{body}</td></tr>
    <tr><td style="padding:16px 24px;background:{BG};border-top:1px solid {BORDER};font-size:12px;color:{MUTED}" dir="rtl">
      <div>{_e(footer_ar)}</div>
      <div dir="ltr" align="right" style="margin-top:2px">{_e(footer_en)}</div>
    </td></tr>
  </table>
  <div style="font-size:11px;color:{MUTED};margin-top:12px" dir="ltr">Fiper · automatic reply reminders</div>
</td></tr></table>
</body></html>"""


def _customer_card(item: dict, tz: tzinfo, tz_label: str, *, show_agent: bool = False) -> str:
    bg, fg = urgency(item["elapsed_min"], item.get("stage", 1))
    line_ar, _, _ = line_labels(item.get("line"))
    name = item.get("name") or "عميل"
    phone = phone_display(item.get("phone"))
    # No flag emoji in email: Windows renders them as letters ("OM").
    line_badge = (
        f"<span style='display:inline-block;background:{BG};border:1px solid {BORDER};color:{NAVY};"
        f"font-size:12px;font-weight:400;padding:2px 8px;border-radius:12px;margin-right:6px'>خط {_e(line_ar)}</span>"
        if line_ar else ""
    )
    agent_row = ""
    if show_agent:
        agent = item.get("agent") or "غير مُسنَد · UNASSIGNED"
        reminders = item.get("reminders", 0)
        agent_row = (
            f"<div style='font-size:13px;color:{TEXT};margin-top:6px'>👤 <b>{_e(agent)}</b>"
            f" &nbsp;·&nbsp; تذكيرات مُرسلة: {reminders}</div>"
        )
    return f"""
<table role="presentation" width="100%" cellpadding="0" cellspacing="0"
       style="border:1px solid {BORDER};border-right:4px solid {fg};border-radius:10px;margin:0 0 12px 0">
<tr><td style="padding:14px 16px" dir="rtl">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
    <td style="font-size:16px;font-weight:700;color:{NAVY}" dir="rtl">{_e(name)}{line_badge}</td>
    <td align="left" style="white-space:nowrap">
      <span style="display:inline-block;background:{bg};color:{fg};font-weight:700;font-size:13px;padding:4px 10px;border-radius:14px">
        ⏳ {_e(wait_ar(item["elapsed_min"]))}</span>
    </td>
  </tr></table>
  <div style="font-size:14px;color:{TEXT};margin-top:4px"><span dir="ltr">{_e(phone)}</span>
    <span style="color:{MUTED}"> · منذ <span dir="ltr">{_e(local_time(item.get("since"), tz))}</span> ({_e(tz_label)})</span></div>
  {agent_row}
  <div dir="auto" style="margin-top:10px;background:{BG};border-radius:8px;padding:10px 12px;font-size:14px;color:{NAVY};white-space:pre-wrap">💬 {_e(short(item.get("body"), 400))}</div>
</td></tr></table>"""


# ── Agent reminder ───────────────────────────────────────────────────────────

def agent_reminder(agent: str, items: list[dict], other_waiting: int, tz: tzinfo, tz_label: str) -> tuple[str, str, str]:
    """Returns (subject, plain_text, html) for one agent."""
    second = any(i.get("stage", 1) >= 2 for i in items)
    n = len(items)
    if second:
        icon, accent, gradient = "🔴", RED_FG, f"linear-gradient(180deg,#E53935 0%,{RED_FG} 100%)"
        title_ar, title_en = "تذكير ثانٍ: ما زالوا بانتظار ردك", "Second reminder — still waiting for your reply"
        subject = f"🔴 تذكير ثانٍ: عملاء ما زالوا بانتظار ردك ({n}) · Still waiting"
    else:
        icon, accent, gradient = "⏰", BLUE, f"linear-gradient(180deg,#308BFF 0%,{BLUE_DARK} 100%)"
        title_ar, title_en = "عملاء بانتظار ردك", "Customers waiting for your reply"
        subject = f"⏰ عملاء بانتظار ردك ({n}) · Customers waiting"
    cards = "".join(_customer_card(i, tz, tz_label) for i in items)
    more = (
        f"<p style='font-size:13px;color:{MUTED};margin:4px 0 0'>وعملاء آخرون بانتظارك أيضاً: {other_waiting}"
        f" <span dir='ltr'>(+{other_waiting} more waiting)</span></p>" if other_waiting else ""
    )
    body = f"""
<p style="font-size:16px;color:{NAVY};margin:0 0 4px">مرحباً <b>{_e(agent)}</b>،</p>
<p style="font-size:14px;margin:0 0 16px">العملاء التالون كتبوا لك ولم يصلهم رد بعد.
  <br><span dir="ltr" style="color:{MUTED};font-size:13px">These customers wrote to you and have not received a reply yet.</span></p>
{cards}{more}
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:14px"><tr>
<td style="background:{'#FDECEC' if second else '#EBF3FF'};border-radius:10px;padding:14px 16px;font-size:14px;color:{NAVY}" dir="rtl">
  <b>✅ المطلوب:</b> يرجى الرد عبر ManyContacts أو الاتصال بالعميل الآن.
  <div dir="ltr" align="right" style="font-size:13px;color:{TEXT};margin-top:4px">Please reply in ManyContacts or call the customer now.</div>
</td></tr></table>"""
    html_doc = _shell(
        preheader=f"عملاء بانتظار ردك ({n}) — {short(items[0].get('body'), 60)}",
        accent=accent, gradient=gradient,
        icon=icon, title_ar=title_ar, title_en=title_en, body=body,
        footer_ar="يتوقف هذا التذكير تلقائياً بمجرد ردك أو اتصالك بالعميل.",
        footer_en="This reminder stops automatically as soon as you reply or call.",
    )
    return subject, agent_reminder_text(agent, items, other_waiting, tz, second), html_doc


def agent_reminder_text(agent: str, items: list[dict], other_waiting: int, tz: tzinfo, second: bool) -> str:
    """Telegram / WhatsApp version (plain text, Arabic first)."""
    head = ("🔴 تذكير ثانٍ: ما زالوا بانتظار ردك\nSecond reminder — still waiting"
            if second else "⏰ عملاء بانتظار ردك\nCustomers waiting for your reply")
    blocks = []
    for idx, i in enumerate(items, 1):
        line_ar, _, flag = line_labels(i.get("line"))
        line = f" · {flag} {line_ar}" if line_ar else ""
        phone = phone_display(i.get("phone"))
        blocks.append(
            f"{idx}) {i.get('name') or 'عميل'} {phone}{line}\n"
            f"⏳ ينتظر منذ {wait_ar(i['elapsed_min'])} ({local_time(i.get('since'), tz)})\n"
            f"💬 {short(i.get('body'), 160)}"
        )
    more = f"\n\nوعملاء آخرون بانتظارك: {other_waiting}" if other_waiting else ""
    return (
        f"{head}\n\nمرحباً {agent}،\n\n" + "\n\n".join(blocks) + more +
        "\n\n✅ يرجى الرد عبر ManyContacts أو الاتصال بالعميل الآن."
        "\nPlease reply in ManyContacts or call the customer now."
        "\n\nيتوقف التذكير تلقائياً بمجرد الرد."
    )


# ── Supervisor escalation ────────────────────────────────────────────────────

def days_ar(hours: float) -> str:
    d = round(hours / 24)
    if d <= 1:
        return "يوم"
    if d == 2:
        return "يومين"
    return f"{d} أيام" if d <= 10 else f"{d} يوماً"


def days_en(hours: float) -> str:
    d = round(hours / 24)
    return "1 day" if d <= 1 else f"{d} days"


def _stage_heading(esc_stage: int, followup_hours: tuple[float, ...]) -> tuple[str, str, str]:
    """(arabic, english, colour) for an escalation stage."""
    if esc_stage <= 0:
        return "تصعيد جديد", "New escalation", RED_FG
    hours = followup_hours[esc_stage - 1] if esc_stage - 1 < len(followup_hours) else followup_hours[-1]
    final = esc_stage >= len(followup_hours)
    if final:
        return f"متابعة أخيرة: ما زالوا دون رد بعد {days_ar(hours)}", f"Final follow-up — still unanswered after {days_en(hours)}", "#6A1B9A"
    return f"متابعة: ما زالوا دون رد بعد {days_ar(hours)}", f"Follow-up — still unanswered after {days_en(hours)}", AMBER_FG


def escalation_footer(followup_hours: tuple[float, ...]) -> tuple[str, str]:
    if not followup_hours:
        return ("تصلك هذه الرسالة لأنك مسؤول متابعة العملاء دون رد.",
                "You receive this as the contact for unanswered customers.")
    parts_ar = [f"بعد {days_ar(h)}" for h in followup_hours]
    parts_en = [f"after {days_en(h)}" for h in followup_hours]
    if len(parts_ar) == 1:
        ar = f"إذا بقي العميل دون رد، ستصلك متابعة {parts_ar[0]}."
        en = f"If a customer is still unanswered, you get a follow-up {parts_en[0]}."
    else:
        ar = ("إذا بقي العميل دون رد، ستصلك متابعة " + "، ثم ".join(parts_ar[:-1])
              + f"، ثم متابعة أخيرة {parts_ar[-1]}. ويتوقف كل شيء تلقائياً بمجرد الرد على العميل.")
        en = ("If a customer is still unanswered, you get a follow-up " + ", then ".join(parts_en[:-1])
              + f" and a final one {parts_en[-1]}. Everything stops automatically once the customer is answered.")
    return ar, en


def escalation(items: list[dict], tz: tzinfo, tz_label: str, escalate_after_min: float, shadow: bool,
               followup_hours: tuple[float, ...] = (24, 72)) -> tuple[str, str, str]:
    n = len(items)
    tag = "[SHADOW] " if shadow else ""
    only_followups = all(i.get("esc_stage", 0) > 0 for i in items)
    if only_followups:
        subject = f"{tag}⏳ متابعة: عملاء ما زالوا دون رد ({n}) · Follow-up: {n} still unanswered — Fiper"
        title_ar, title_en, icon = f"متابعة: ما زالوا دون رد ({n})", f"Follow-up — {n} customer(s) still unanswered", "⏳"
    else:
        subject = f"{tag}🚨 عملاء دون رد ({n}) · {n} customer(s) unanswered — Fiper"
        title_ar, title_en, icon = f"عملاء دون رد ({n})", f"{n} customer(s) still unanswered", "🚨"

    by_agent: dict[str, int] = {}
    for i in items:
        key = i.get("agent") or "غير مُسنَد · UNASSIGNED"
        by_agent[key] = by_agent.get(key, 0) + 1
    summary_rows = "".join(
        f"<tr><td style='padding:6px 10px;border-bottom:1px solid {BORDER};color:{NAVY}' dir='rtl'>{_e(a)}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid {BORDER};font-weight:700;color:{RED_FG}' align='left'>{c}</td></tr>"
        for a, c in sorted(by_agent.items(), key=lambda kv: -kv[1])
    )
    # Sections: new escalations first, then follow-ups (latest stage last).
    sections_html = []
    text_sections = []
    for stage in sorted({i.get("esc_stage", 0) for i in items}):
        group = sorted((i for i in items if i.get("esc_stage", 0) == stage), key=lambda i: -i["elapsed_min"])
        h_ar, h_en, colour = _stage_heading(stage, followup_hours)
        sections_html.append(
            f"<div style='margin:18px 0 10px;padding-bottom:6px;border-bottom:2px solid {colour};"
            f"font-size:15px;font-weight:700;color:{colour}' dir='rtl'>{_e(h_ar)} ({len(group)})"
            f"<div dir='ltr' align='right' style='font-size:12px;font-weight:400;color:{MUTED}'>{_e(h_en)}</div></div>"
            + "".join(_customer_card({**i, "stage": 2}, tz, tz_label, show_agent=True) for i in group)
        )
        text_sections.append(
            f"— {h_ar} ({len(group)}) —\n" + "\n\n".join(
                f"{idx}) {i.get('name') or 'عميل'} {phone_display(i.get('phone'))} — {i.get('agent') or 'UNASSIGNED'}\n"
                f"⏳ {wait_ar(i['elapsed_min'])} ({local_time(i.get('since'), tz)}) · 🔔 {i.get('reminders', 0)}\n"
                f"💬 {short(i.get('body'), 120)}"
                for idx, i in enumerate(group, 1)
            )
        )
    cards = "".join(sections_html)
    shadow_note = (
        "<p style='background:#FFF8E1;border:1px solid #FFE082;border-radius:8px;padding:10px 12px;font-size:13px'>"
        "وضع التجربة: لم يُرسل أي شيء للموظفين. <span dir='ltr'>Shadow mode — nothing was sent to agents.</span></p>"
        if shadow else ""
    )
    body = f"""
{shadow_note}
<p style="font-size:14px;margin:0 0 14px">هؤلاء العملاء ينتظرون أكثر من <b>{_e(wait_ar(escalate_after_min))}</b> من وقت العمل، أو لا يوجد موظف مسؤول عن المحادثة.
  <br><span dir="ltr" style="color:{MUTED};font-size:13px">Waiting longer than {_e(wait_en(escalate_after_min))} of working time, or nobody owns the chat.</span></p>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid {BORDER};border-radius:10px;margin-bottom:18px">
  <tr><td style="padding:8px 10px;background:{BG};font-weight:700;color:{NAVY}" dir="rtl">حسب الموظف · By agent</td>
      <td style="padding:8px 10px;background:{BG};font-weight:700;color:{NAVY}" align="left">#</td></tr>
  {summary_rows}
</table>
{cards}"""
    footer_ar, footer_en = escalation_footer(tuple(followup_hours))
    html_doc = _shell(
        preheader=f"{title_ar} — {title_en}",
        accent=RED_FG if not only_followups else AMBER_FG, icon=icon,
        title_ar=title_ar, title_en=title_en,
        body=body, label="SHADOW" if shadow else "",
        footer_ar=footer_ar, footer_en=footer_en,
    )
    text = f"{tag}{icon} {title_ar} · {title_en}\n\n" + "\n\n".join(text_sections) + f"\n\n{footer_ar}"
    return subject, text, html_doc


# ── Shadow preview digest ────────────────────────────────────────────────────

def shadow_digest(batches: list[dict], tz: tzinfo, tz_label: str) -> tuple[str, str]:
    """batches: [{"agent": str, "items": [...], "other": int, "at": datetime}]"""
    n = len(batches)
    blocks = []
    for b in batches:
        second = any(i.get("stage", 1) >= 2 for i in b["items"])
        tag = "🔴 تذكير ثانٍ · 2nd" if second else "⏰ تذكير أول · 1st"
        cards = "".join(_customer_card(i, tz, tz_label) for i in b["items"])
        blocks.append(
            f"<div style='margin:18px 0 8px;font-size:15px;color:{NAVY}'><b>إلى · To: {_e(b['agent'])}</b>"
            f" <span style='color:{MUTED};font-size:13px'>· {tag} · <span dir='ltr'>{_e(local_time(b['at'], tz))}</span></span></div>"
            + cards
        )
    body = (
        "<p style='background:#FFF8E1;border:1px solid #FFE082;border-radius:8px;padding:10px 12px;font-size:13px'>"
        "هذه معاينة فقط — لم يُرسل أي شيء للموظفين. للتفعيل: REMINDER_MODE=live على Render."
        "<br><span dir='ltr'>Preview only — nothing was sent to agents. To go live set REMINDER_MODE=live on Render.</span></p>"
        + "".join(blocks)
    )
    subject = f"[SHADOW] 👀 معاينة التذكيرات ({n}) · reminders preview"
    html_doc = _shell(
        preheader=f"{n} reminder message(s) would have been sent",
        accent=NAVY, icon="👀", title_ar="معاينة التذكيرات", title_en="Reminders preview (shadow mode)",
        body=body, label="SHADOW",
        footer_ar="كل بطاقة أعلاه هي ما كان سيصل للموظف.",
        footer_en="Each card above is what the agent would have received.",
    )
    return subject, html_doc
