"""Daily / weekly report emails. Pure rendering (no database).

Same frame as the reminder emails (Fiper logo, Arabic first, English line
underneath). Tables + inline styles only, so Gmail/Outlook/mobile all render
it the same way.
"""
from __future__ import annotations

from datetime import datetime, timedelta, tzinfo

from pipeline import reminder_templates as T
from pipeline.reminder_templates import AMBER_BG, AMBER_FG, BG, BLUE, BORDER, MUTED, NAVY, RED_BG, RED_FG, TEXT, _e

GOOD_BG, GOOD_FG = "#E8F5EC", "#1B7F3B"
TONES = {"good": (GOOD_BG, GOOD_FG), "warn": (AMBER_BG, AMBER_FG), "bad": (RED_BG, RED_FG), "neutral": (BG, NAVY)}

TOPICS = {
    "account_info": ("الحساب والتسجيل", "Account & setup"),
    "follow_up": ("متابعة", "Follow-up"),
    "trading_education": ("تعليم التداول", "Trading education"),
    "pricing": ("الأسعار والعمولات", "Pricing & fees"),
    "technical": ("مشاكل تقنية", "Technical issues"),
    "product_fit": ("ملاءمة المنتج", "Product fit"),
    "competitor": ("المنافسون", "Competitors"),
    "not_decision_maker": ("ليس صاحب القرار", "Not the decision maker"),
    "profit_expectations": ("توقعات الأرباح", "Profit expectations"),
    "beginner_risk": ("عميل مبتدئ", "Beginner"),
}
DAYS_AR = ["الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت", "الأحد"]
DAYS_EN = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


# ── Formatting ───────────────────────────────────────────────────────────────

def mins_ar(m) -> str:
    if m is None:
        return "—"
    if m < 1:
        return "<1 د"
    m = int(round(m))
    if m < 60:
        return f"{m} د"
    h, r = divmod(m, 60)
    return f"{h} س {r} د" if r else f"{h} س"


def mins_en(m) -> str:
    if m is None:
        return "—"
    if m < 1:
        return "<1 min"
    m = int(round(m))
    if m < 60:
        return f"{m} min"
    h, r = divmod(m, 60)
    return f"{h}h {r}m" if r else f"{h}h"


def pct(v) -> str:
    return "—" if v is None else f"{v}%"


def talk(seconds) -> str:
    s = int(seconds or 0)
    h, rem = divmod(s // 60, 60)
    return f"{h}h {rem:02d}m" if h else f"{rem}m"


def reply_tone(m):
    return "neutral" if m is None else ("good" if m <= 10 else "warn" if m <= 30 else "bad")


def sla_tone(p):
    return "neutral" if p is None else ("good" if p >= 80 else "warn" if p >= 60 else "bad")


def pickup_tone(p):
    # 24% is Fiper's LOW PERF threshold (QC rules).
    return "neutral" if p is None else ("good" if p >= 30 else "warn" if p >= 24 else "bad")


# ── Building blocks ──────────────────────────────────────────────────────────

def chip(text: str, tone: str = "neutral") -> str:
    bg, fg = TONES[tone]
    return (f"<span style='display:inline-block;background:{bg};color:{fg};font-weight:700;font-size:12px;"
            f"padding:3px 8px;border-radius:12px;white-space:nowrap' dir='ltr'>{_e(text)}</span>")


def delta(cur, prev, *, lower_is_better=False, unit="") -> str:
    if cur is None or prev is None:
        return ""
    diff = cur - prev
    if abs(diff) < 1e-9:
        return f"<span style='color:{MUTED};font-size:12px'>= {_e(str(prev))}{unit}</span>"
    good = (diff < 0) if lower_is_better else (diff > 0)
    colour = GOOD_FG if good else RED_FG
    arrow = "▲" if diff > 0 else "▼"
    shown = int(round(abs(diff)))
    return f"<span style='color:{colour};font-size:12px;font-weight:700' dir='ltr'>{arrow} {shown}{unit}</span>"


def kpi_grid(cards: list[dict], per_row: int = 3) -> str:
    """cards: {ar, en, value, tone, sub, delta}"""
    rows = []
    width = f"{100 // per_row}%"
    for i in range(0, len(cards), per_row):
        cells = []
        for c in cards[i:i + per_row]:
            bg, fg = TONES[c.get("tone", "neutral")]
            cells.append(
                f"<td width='{width}' valign='top' style='padding:6px'>"
                f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
                f"style='background:{bg};border-radius:10px'><tr><td style='padding:12px 12px' dir='rtl'>"
                f"<div style='font-size:12px;color:{TEXT}'>{_e(c['ar'])}</div>"
                f"<div style='font-size:24px;font-weight:800;color:{fg};margin-top:4px' dir='ltr' align='right'>{_e(c['value'])}"
                f" {c.get('delta', '')}</div>"
                f"<div style='font-size:11px;color:{MUTED}' dir='ltr' align='right'>{_e(c['en'])}</div>"
                + (f"<div style='font-size:12px;color:{TEXT};margin-top:4px'>{c['sub']}</div>" if c.get("sub") else "")
                + "</td></tr></table></td>"
            )
        while len(cells) < per_row:
            cells.append(f"<td width='{width}'></td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0' style='margin:0 -6px'>{''.join(rows)}</table>"


def section(icon: str, title_ar: str, title_en: str, inner: str, note: str = "") -> str:
    return (
        f"<div style='margin:26px 0 10px' dir='rtl'>"
        f"<div style='font-size:17px;font-weight:800;color:{NAVY}'>{icon} {_e(title_ar)}</div>"
        f"<div style='font-size:12px;color:{MUTED}' dir='ltr' align='right'>{_e(title_en)}</div>"
        + (f"<div style='font-size:12px;color:{TEXT};margin-top:4px'>{note}</div>" if note else "")
        + f"</div>{inner}"
    )


def data_table(headers: list[tuple[str, str]], rows: list[list[str]], first_col_width: str = "") -> str:
    head = "".join(
        f"<th style='padding:8px 6px;background:{BG};border-bottom:2px solid {BORDER};font-size:12px;color:{NAVY};"
        f"text-align:{'right' if i == 0 else 'center'};{('width:' + first_col_width + ';') if (i == 0 and first_col_width) else ''}' dir='rtl'>"
        f"{_e(ar)}<div style='font-weight:400;font-size:10px;color:{MUTED}' dir='ltr'>{_e(en)}</div></th>"
        for i, (ar, en) in enumerate(headers)
    )
    body = "".join(
        "<tr>" + "".join(
            f"<td style='padding:8px 6px;border-bottom:1px solid {BORDER};font-size:13px;color:{NAVY};"
            f"text-align:{'right' if i == 0 else 'center'};{'background:#FBFCFF;' if r % 2 else ''}' dir='rtl'>{cell}</td>"
            for i, cell in enumerate(row)
        ) + "</tr>"
        for r, row in enumerate(rows)
    )
    return (f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
            f"style='border:1px solid {BORDER};border-radius:10px;border-collapse:separate;overflow:hidden'>"
            f"<tr>{head}</tr>{body}</table>")


def bar(value: float, max_value: float, colour: str = BLUE, width: int = 110) -> str:
    frac = 0 if not max_value else max(0.0, min(1.0, value / max_value))
    filled = max(2, int(width * frac)) if value else 0
    return (f"<table role='presentation' cellpadding='0' cellspacing='0' style='display:inline-table;vertical-align:middle' dir='ltr'><tr>"
            f"<td style='width:{filled}px;height:8px;background:{colour};border-radius:4px;font-size:0;line-height:0'>&nbsp;</td>"
            f"<td style='width:{width - filled}px;height:8px;background:{BORDER};border-radius:4px;font-size:0;line-height:0'>&nbsp;</td>"
            f"</tr></table> <span style='font-size:12px' dir='ltr'>{_e(str(int(value)))}</span>")


def notice(text_ar: str, text_en: str, tone: str = "good") -> str:
    bg, fg = TONES[tone]
    return (f"<div style='background:{bg};color:{fg};border-radius:10px;padding:12px 14px;font-size:14px;font-weight:600' dir='rtl'>"
            f"{_e(text_ar)}<div dir='ltr' align='right' style='font-size:12px;font-weight:400'>{_e(text_en)}</div></div>")


# ── Shared sections ──────────────────────────────────────────────────────────

def _waiting_section(waiting: list[dict], tz: tzinfo, limit: int = 12) -> str:
    if not waiting:
        return section("✅", "بانتظار الرد الآن", "Waiting for a reply right now",
                       notice("لا يوجد عملاء بانتظار الرد الآن 👏", "No customer is waiting right now."))
    rows = []
    # Urgent customers (money, complaints, new leads) first, then the longest waits.
    waiting = sorted(waiting, key=lambda w: (w.get("priority") != "urgent", -w["elapsed_min"]))
    for w in waiting[:limit]:
        status = [chip("عاجل · Urgent", "bad")] if w.get("priority") == "urgent" else []
        if w["escalations"] > 1:
            status.append(chip(f"متابعة ×{w['escalations'] - 1}", "bad"))
        elif w["escalations"]:
            status.append(chip("تم التصعيد", "bad"))
        elif w["reminders"]:
            status.append(chip(f"تذكير ×{w['reminders']}", "warn"))
        name = _e(w.get("name") or "عميل")
        phone = T.phone_display(w.get("phone"))
        rows.append([
            f"<b>{name}</b><div style='font-size:11px;color:{MUTED}' dir='ltr'>{_e(phone)}</div>"
            f"<div dir='auto' style='font-size:12px;color:{TEXT};margin-top:2px'>💬 {_e(T.short(w.get('body'), 70))}</div>",
            _e(w.get("agent") or "غير مُسنَد"),
            chip(mins_en(w["elapsed_min"]), "bad" if w["elapsed_min"] >= 120 else "warn"),
            " ".join(status) or "—",
        ])
    more = len(waiting) - limit
    note = (f"<span dir='ltr'>{len(waiting)}</span> عميل · العاجل أولاً ثم الأطول انتظاراً"
            f"{f' · و{more} آخرون في لوحة التحكم' if more > 0 else ''}")
    return section("⚠️", "بانتظار الرد الآن", "Waiting for a reply right now (urgent first, then longest)",
                   data_table([("العميل", "Customer"), ("الموظف", "Agent"), ("الانتظار", "Waiting"), ("الحالة", "Status")],
                              rows, "46%"), note)


def ai_cards(rem: dict) -> list[dict]:
    """What the AI check decided (empty until it has checked something)."""
    if not (rem.get("decided") or rem.get("ai_checked")):
        return []
    med = rem.get("urgent_median_answer_min")
    return [
        {"ar": "عملاء عاجلون", "en": "Urgent customers", "value": rem.get("urgent", 0),
         "tone": "warn" if rem.get("urgent") else "good",
         "sub": "مال، شكوى أو عميل جديد"},
        {"ar": "وسيط الرد على العاجل", "en": "Median reply to urgent", "value": mins_en(med) if med is not None else "—",
         "tone": reply_tone(med) if med is not None else "neutral",
         "sub": f"تم الرد على <b dir='ltr'>{rem.get('urgent_answered', 0)}</b>"},
        {"ar": "لا يحتاج رداً (بدون تذكير)", "en": "No reply needed (no reminder sent)", "value": rem.get("no_reply_needed", 0),
         "tone": "good",
         "sub": f"من <b dir='ltr'>{rem.get('ai_checked', 0)}</b> فحصها الذكاء الاصطناعي"},
    ]


def _reminder_strip(rem: dict) -> str:
    cards = [
        {"ar": "عملاء تم تذكير موظفيهم", "en": "Customers reminded", "value": rem.get("reminded_customers", 0)},
        {"ar": "تم الرد بعد التذكير", "en": "Answered after a reminder", "value": rem.get("answered_after_reminder", 0), "tone": "good"},
        {"ar": "تصعيد للمشرف", "en": "Escalated to supervisor", "value": rem.get("escalated", 0),
         "tone": "bad" if rem.get("escalated") else "good"},
    ] + ai_cards(rem)
    return section("🔔", "نظام التذكير", "Reply reminders · AI check", kpi_grid(cards))


def _footer(tz_label: str) -> tuple[str, str]:
    return ("زمن الرد = من رسالة العميل حتى أول رد واتساب أو مكالمة من الموظف، ويُحسب بوقت العمل فقط.",
            "Reply time = from the customer's message to the agent's first WhatsApp reply or call, counted in working time only.")


# ── Daily report ─────────────────────────────────────────────────────────────

def daily_report(data: dict, tz: tzinfo, tz_label: str, label: str = "") -> tuple[str, str]:
    m, agents, waiting = data["metrics"], data["agents"], data["waiting"]
    resp, calls = m["resp"], m["calls"]
    local_now = data["now"].astimezone(tz)
    day_ar = f"{DAYS_AR[local_now.weekday()]} {local_now:%d/%m}"
    day_en = f"{DAYS_EN[local_now.weekday()]} {local_now:%d %b}"
    long_waits = sum(1 for w in waiting if w["elapsed_min"] >= 120)
    by_channel = m.get("new_leads_by_channel", {})

    cards = [
        {"ar": "عملاء كتبوا اليوم", "en": "Customers who wrote today", "value": resp["customers"]},
        {"ar": "وسيط زمن أول رد", "en": "Median first reply", "value": mins_en(resp["median_reply"]),
         "tone": reply_tone(resp["median_reply"])},
        {"ar": "رد خلال 15 دقيقة", "en": "Answered within 15 min", "value": pct(resp["sla_pct"]),
         "tone": sla_tone(resp["sla_pct"])},
        {"ar": "بانتظار الرد الآن", "en": "Waiting now", "value": len(waiting),
         "tone": "bad" if long_waits else ("warn" if waiting else "good"),
         "sub": f"أكثر من ساعتين: <b dir='ltr'>{long_waits}</b>" if waiting else ""},
        {"ar": "المكالمات", "en": "Calls", "value": calls["calls"], "tone": pickup_tone(calls["pickup_pct"]),
         "sub": f"نسبة الرد <b dir='ltr'>{pct(calls['pickup_pct'])}</b> · <span dir='ltr'>{talk(calls['talk_seconds'])}</span>"},
        {"ar": "عملاء جدد", "en": "New leads", "value": m["new_leads"],
         "sub": " · ".join(f"<span dir='ltr'>{_e(k)} {v}</span>" for k, v in sorted(by_channel.items(), key=lambda kv: -kv[1])[:3])},
    ]

    active = [a for a in agents.values() if a.episodes or a.calls or a.waiting_now or a.messages_sent]
    active.sort(key=lambda a: (-a.waiting_now, -(a.episodes + a.calls)))
    rows = [[
        f"<b>{_e(a.name)}</b>",
        _e(str(len(a.chats))),
        chip(mins_en(a.median_reply), reply_tone(a.median_reply)) if a.answered else "—",
        chip(pct(a.sla_pct), sla_tone(a.sla_pct)) if a.sla_base else "—",
        chip(str(a.waiting_now), "bad") if a.waiting_now else "0",
        chip(str(a.escalated), "bad") if a.escalated else "0",
        _e(str(a.calls)),
        chip(pct(a.pickup_pct), pickup_tone(a.pickup_pct)) if a.pickup_pct is not None else "—",
        f"<span dir='ltr'>{talk(a.talk_seconds)}</span>" if a.talk_seconds else "—",
    ] for a in active]
    agents_html = section(
        "👥", "أداء الموظفين اليوم", "Agents today (who has customers waiting first)",
        data_table([("الموظف", "Agent"), ("عملاء تم الرد", "Answered"), ("وسيط الرد", "Median reply"),
                    ("≤ 15 د", "≤ 15 min"), ("ينتظرون", "Waiting"), ("تصعيد", "Escalated"),
                    ("مكالمات", "Calls"), ("نسبة الرد", "Pickup"), ("وقت المكالمات", "Talk time")], rows, "20%")
        if rows else notice("لا يوجد نشاط بعد اليوم.", "No activity yet today.", "neutral"),
    )

    body = (
        kpi_grid(cards)
        + _waiting_section(waiting, tz)
        + agents_html
        + _reminder_strip(m.get("reminders") or {})
    )
    footer_ar, footer_en = _footer(tz_label)
    title_en = f"Daily sales report · {day_en}" + (f" · {label}" if label else "")
    html_doc = T.email_shell(
        preheader=f"{resp['customers']} عميل · وسيط الرد {mins_ar(resp['median_reply'])} · ينتظرون الآن {len(waiting)}",
        accent=BLUE, gradient=f"linear-gradient(180deg,#308BFF 0%,{T.BLUE_DARK} 100%)", icon="📊",
        title_ar=f"تقرير المبيعات اليومي · {day_ar}", title_en=title_en, body=body,
        footer_ar=footer_ar, footer_en=footer_en,
    )
    subject = f"📊 تقرير المبيعات اليومي · Daily report — {local_now:%d/%m}" + (f" {label}" if label else "")
    return subject, html_doc


# ── Weekly report ────────────────────────────────────────────────────────────

def weekly_report(data: dict, tz: tzinfo, tz_label: str, label: str = "") -> tuple[str, str]:
    m, p, agents, prev_agents = data["metrics"], data["prev"], data["agents"], data["prev_agents"]
    resp, calls, presp, pcalls = m["resp"], m["calls"], p["resp"], p["calls"]
    start_l = data["start"].astimezone(tz)
    end_l = (data["end"] - timedelta(seconds=1)).astimezone(tz)
    period = f"{start_l:%d/%m} – {end_l:%d/%m}"
    rem = m.get("reminders") or {}

    cards = [
        {"ar": "عملاء كتبوا", "en": "Customers who wrote", "value": resp["customers"],
         "delta": delta(resp["customers"], presp["customers"])},
        {"ar": "وسيط زمن أول رد", "en": "Median first reply", "value": mins_en(resp["median_reply"]),
         "tone": reply_tone(resp["median_reply"]),
         "delta": delta(resp["median_reply"], presp["median_reply"], lower_is_better=True, unit="m")},
        {"ar": "رد خلال 15 دقيقة", "en": "Answered within 15 min", "value": pct(resp["sla_pct"]),
         "tone": sla_tone(resp["sla_pct"]), "delta": delta(resp["sla_pct"], presp["sla_pct"], unit="pt")},
        {"ar": "تصعيد للمشرف", "en": "Escalated customers", "value": rem.get("escalated", 0),
         "tone": "bad" if rem.get("escalated") else "good",
         "sub": f"لم يُرد عليهم أبداً: <b dir='ltr'>{rem.get('expired_unanswered', 0)}</b>"},
        {"ar": "المكالمات", "en": "Calls", "value": calls["calls"], "tone": pickup_tone(calls["pickup_pct"]),
         "delta": delta(calls["calls"], pcalls["calls"]),
         "sub": f"نسبة الرد <b dir='ltr'>{pct(calls['pickup_pct'])}</b> {delta(calls['pickup_pct'], pcalls['pickup_pct'], unit='pt')}"},
        {"ar": "عملاء جدد", "en": "New leads", "value": m["new_leads"], "delta": delta(m["new_leads"], p["new_leads"])},
    ]

    insights = data.get("insights") or []
    insights_html = section("💡", "أهم الملاحظات", "Key insights", (
        "<table role='presentation' width='100%' cellpadding='0' cellspacing='0'>" + "".join(
            f"<tr><td style='padding:8px 12px;border-right:3px solid {BLUE};background:{BG};font-size:14px;color:{NAVY}' dir='rtl'>"
            f"{_e(ar)}<div dir='ltr' align='right' style='font-size:12px;color:{MUTED}'>{_e(en)}</div></td></tr>"
            f"<tr><td style='height:6px;font-size:0;line-height:0'>&nbsp;</td></tr>"
            for ar, en in insights) + "</table>"
    ) if insights else notice("لا توجد ملاحظات خاصة هذا الأسبوع.", "Nothing special to report this week.", "neutral"))

    active = [a for a in agents.values() if a.episodes or a.calls]
    active.sort(key=lambda a: (-len(a.chats), -a.calls))
    rows = []
    for a in active:
        key = next((k for k, v in agents.items() if v is a), None)
        prev = prev_agents.get(key)
        quality = round(sum(a.treatment) / len(a.treatment)) if a.treatment else None
        rows.append([
            f"<b>{_e(a.name)}</b>",
            _e(str(len(a.chats))),
            (chip(mins_en(a.median_reply), reply_tone(a.median_reply)) + "<br>"
             + delta(a.median_reply, prev.median_reply if prev else None, lower_is_better=True, unit="m")) if a.answered else "—",
            chip(pct(a.sla_pct), sla_tone(a.sla_pct)) if a.sla_base else "—",
            chip(str(a.escalated), "bad") if a.escalated else "0",
            _e(str(a.calls)),
            chip(pct(a.pickup_pct), pickup_tone(a.pickup_pct)) if a.pickup_pct is not None else "—",
            f"<span dir='ltr'>{talk(a.talk_seconds)}</span>" if a.talk_seconds else "—",
            chip(f"{quality}", "good" if quality >= 70 else "warn" if quality >= 50 else "bad") if quality is not None else "—",
        ])
    agents_html = section(
        "👥", "أداء الموظفين", "Agents this week (Δ = change vs previous week)",
        data_table([("الموظف", "Agent"), ("عملاء تم الرد", "Answered"), ("وسيط الرد", "Median reply"),
                    ("≤ 15 د", "≤ 15 min"), ("تصعيد", "Escalated"), ("مكالمات", "Calls"),
                    ("نسبة الرد", "Pickup"), ("وقت المكالمات", "Talk time"), ("الجودة", "AI quality")], rows, "18%")
        if rows else notice("لا يوجد نشاط.", "No activity.", "neutral"),
        "الجودة = متوسط تقييم الذكاء الاصطناعي لطريقة التعامل (0–100)." if any(a.treatment for a in active) else "",
    )

    # Daily trend
    days = []
    cur = start_l
    while cur <= end_l:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    max_cust = max([resp["by_day"].get(d, {}).get("customers", 0) for d in days] + [1])
    trend_rows = []
    for d in days:
        rd = resp["by_day"].get(d, {})
        cd = calls["by_day"].get(d, {})
        dt = datetime.strptime(d, "%Y-%m-%d")
        trend_rows.append([
            f"<b>{DAYS_AR[dt.weekday()]}</b> <span style='color:{MUTED};font-size:11px' dir='ltr'>{dt:%d/%m}</span>",
            bar(rd.get("customers", 0), max_cust),
            chip(mins_en(rd.get("median_reply")), reply_tone(rd.get("median_reply"))) if rd.get("median_reply") is not None else "—",
            chip(pct(rd.get("sla_pct")), sla_tone(rd.get("sla_pct"))) if rd.get("sla_pct") is not None else "—",
            _e(str(cd.get("calls", 0))),
            pct(cd.get("pickup_pct")),
        ])
    trend_html = section("📅", "الأسبوع يوماً بيوم", "Day by day",
                         data_table([("اليوم", "Day"), ("عملاء كتبوا", "Customers"), ("وسيط الرد", "Median reply"),
                                     ("≤ 15 د", "≤ 15 min"), ("مكالمات", "Calls"), ("نسبة الرد", "Pickup")], trend_rows, "22%"))

    # Hours (2-hour blocks)
    by_hour = resp["by_hour"]
    blocks = []
    for h in range(0, 24, 2):
        vals = by_hour.get(h, []) + by_hour.get(h + 1, [])
        if vals:
            answered = [v for v in vals if v is not None]
            med = sorted(answered)[len(answered) // 2] if answered else None
            blocks.append((h, len(vals), med))
    max_block = max([b[1] for b in blocks] + [1])
    hour_rows = [[
        f"<span dir='ltr'>{h:02d}:00–{h + 2:02d}:00</span>",
        bar(n, max_block, "#7C4DFF"),
        chip(mins_en(med), reply_tone(med)) if med is not None else "—",
    ] for h, n, med in blocks]
    after = m.get("after_hours_pct")
    hours_html = section("🕒", "متى يكتب العملاء؟", f"When customers write ({tz_label})",
                         data_table([("الوقت", "Time"), ("طلبات العملاء", "Customer waits"), ("وسيط الرد", "Median reply")],
                                    hour_rows, "30%") if hour_rows else "",
                         f"خارج أوقات العمل: <b dir='ltr'>{after}%</b> من رسائل العملاء." if after is not None else "")

    # Topics
    topics_html = ""
    if m.get("topics"):
        total = m.get("topics_total") or 1
        max_t = m["topics"][0][1]
        topic_rows = [[
            f"<b>{_e(TOPICS.get(t, (t.replace('_', ' ').title(), ''))[0])}</b>"
            f"<div style='font-size:11px;color:{MUTED}' dir='ltr'>{_e(TOPICS.get(t, ('', t.replace('_', ' ').title()))[1])}</div>",
            bar(c, max_t, "#00A3A3"),
            f"<span dir='ltr'>{round(c / total * 100)}%</span>",
        ] for t, c in m["topics"]]
        topics_html = section("🗂️", "عن ماذا يسأل العملاء؟", f"What customers ask about (AI, {m.get('analyses', 0)} conversations)",
                              data_table([("الموضوع", "Topic"), ("عدد", "Count"), ("النسبة", "Share")], topic_rows, "40%"),
                              "استخدمها لتحضير ردود جاهزة وتدريب الفريق على الأسئلة الأكثر تكراراً.")

    rem_cards = [
        {"ar": "عملاء تم تذكير موظفيهم", "en": "Customers reminded", "value": rem.get("reminded_customers", 0)},
        {"ar": "تم الرد بعد التذكير", "en": "Answered after reminder", "value": rem.get("answered_after_reminder", 0), "tone": "good"},
        {"ar": "متابعات للمشرف", "en": "Supervisor follow-ups", "value": rem.get("followups", 0),
         "tone": "warn" if rem.get("followups") else "good"},
    ] + ai_cards(rem)

    body = (
        kpi_grid(cards)
        + insights_html
        + agents_html
        + trend_html
        + hours_html
        + topics_html
        + section("🔔", "نظام التذكير", "Reply reminders this week · AI check", kpi_grid(rem_cards))
    )
    footer_ar, footer_en = _footer(tz_label)
    html_doc = T.email_shell(
        preheader=f"{resp['customers']} عميل · وسيط الرد {mins_ar(resp['median_reply'])} · رد خلال 15 د {pct(resp['sla_pct'])}",
        accent=NAVY, gradient=f"linear-gradient(180deg,#3B4D70 0%,{NAVY} 100%)", icon="📈",
        title_ar=f"التقرير الأسبوعي · {period}", title_en=f"Weekly report · {period}" + (f" · {label}" if label else ""),
        body=body, footer_ar=footer_ar + " المقارنة مع الأسبوع السابق.", footer_en=footer_en + " Arrows compare with the previous week.",
    )
    subject = f"📈 التقرير الأسبوعي · Weekly report — {period}"
    return subject, html_doc
