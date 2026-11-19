"""Does a waiting customer really need an answer, and how urgently?

Gemini reads the customer's latest messages together with the agent's last
message. Word rules are the fallback whenever the AI is unavailable, paused
by its quota, or over the daily limit. The rules never decide "no reply
needed": a failing AI must not silence a reminder.

Free-tier friendly: callers limit how many checks run per cycle, calls are
spaced, a 429 pauses the AI for a while, and there is a daily cap.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone

import requests

from pipeline.reply_tracker import is_banal_message

log = logging.getLogger("fiper.reply_ai")

PRIORITIES = ("urgent", "normal", "low")
DAILY_LIMIT = int(os.getenv("REMINDER_AI_DAILY_LIMIT", "500"))
MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("REMINDER_AI_SPACING_SECONDS", "4.5"))
PAUSE_AFTER_LIMIT = timedelta(minutes=15)

URGENT = re.compile(
    r"(سحب|ايداع|إيداع|حوال[ةه]|تحويل|رصيد|ارباح|أرباح|فلوس|مبلغ|مصاري|شكوى|احتيال|نصب|"
    r"ليش ما|ما ?ترد|ما ?حدا رد|ما ?رديت|صارلكم|withdraw|deposit|refund|money|balance|scam)",
    re.I)
GREETING = re.compile(
    r"^(ال?سلام عليكم|السلام|سلام|مرحب\S*|اهلا\S*|أهلا\S*|هلا\S*|hi+|hello|hey|صباح الخير|مساء الخير|"
    r"صباح النور|مساء النور|good (morning|evening))\b", re.I)

_state = {"day": None, "used": 0, "paused_until": None, "last_call": 0.0}
_lock = threading.Lock()


def mode() -> str:
    value = os.getenv("REMINDER_AI_MODE", "shadow").strip().lower()
    return value if value in {"off", "shadow", "live"} else "shadow"


def _greeting_only(text: str) -> bool:
    words = " ".join(text.split()).strip(" .!,")
    return bool(GREETING.match(words.lower())) and len(words.split()) <= 4 and not re.search(r"[?؟]", words)


def rules(context: dict) -> dict:
    """Fallback without AI. Deliberately cautious: never "no reply needed", and
    "urgent" only for clear money/complaint words, so a failing AI can never
    cause MORE reminders than before."""
    texts = [t for t in context.get("customer", []) if t and not is_banal_message(t)]
    if URGENT.search(" ".join(texts)):
        priority = "urgent"
    elif texts and all(_greeting_only(t) for t in texts) and not context.get("first_contact"):
        priority = "low"
    else:
        priority = "normal"
    return {"needs_reply": True, "priority": priority, "reason": "word rules", "source": "rules"}


def _prompt(context: dict) -> str:
    customer = "\n".join(f"{i}) {t[:400]}" for i, t in enumerate(context.get("customer", [])[-5:], 1))
    return (
        "You review WhatsApp chats of Fiper, an online trading broker. Customers write in Arabic dialects, "
        "French or English. Decide whether the sales agent must still answer the customer's latest messages.\n"
        "needs_reply=false ONLY when the customer just closes or pauses the chat and asks nothing: thanks, ok, "
        "agreement, goodbye, blessings, an emoji, or saying they will come back later. If in doubt, needs_reply=true.\n"
        "priority: urgent = money (deposit, withdrawal, payout, balance, transfer), account problem, complaint or "
        "anger, repeated messages, or a new lead asking how to start; low = only a greeting or small talk in an "
        "ongoing chat; otherwise normal.\n"
        "Voice notes [audio], images [image] or documents cannot be read: needs_reply=true.\n"
        'Answer JSON only: {"needs_reply": true|false, "priority": "urgent"|"normal"|"low", '
        '"reason": "max 10 English words"}\n\n'
        f"New lead without any earlier agent reply: {'yes' if context.get('first_contact') else 'no'}\n"
        f"Agent's last message before: {(context.get('agent_last') or '(none)')[:400]}\n"
        f"Customer messages since then, oldest first:\n{customer or '(none)'}"
    )


def parse_answer(text: str) -> dict | None:
    match = re.search(r"\{.*\}", text or "", re.S)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except ValueError:
        return None
    priority = str(data.get("priority") or "normal").lower()
    return {
        "needs_reply": data.get("needs_reply") is not False,
        "priority": priority if priority in PRIORITIES else "normal",
        "reason": " ".join(str(data.get("reason") or "").split())[:120],
        "source": "ai",
    }


def available() -> bool:
    if not os.getenv("GEMINI_API_KEY"):
        return False
    with _lock:
        today = date.today()
        if _state["day"] != today:
            _state.update(day=today, used=0)
        paused = _state["paused_until"]
        return _state["used"] < DAILY_LIMIT and not (paused and datetime.now(timezone.utc) < paused)


def classify(context: dict) -> dict:
    """AI decision, or the rules when the AI cannot be used right now."""
    if not available():
        return rules(context)
    with _lock:
        wait = _state["last_call"] + MIN_SECONDS_BETWEEN_CALLS - time.time()
        _state["used"] += 1
    if wait > 0:
        time.sleep(wait)
    _state["last_call"] = time.time()
    # Own setting on purpose: GEMINI_MODEL may still name a retired 1.5 model.
    model = os.getenv("REMINDER_AI_MODEL", "").strip() or "gemini-2.5-flash-lite"
    try:
        response = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            params={"key": os.getenv("GEMINI_API_KEY")},
            json={
                "contents": [{"role": "user", "parts": [{"text": _prompt(context)}]}],
                "generationConfig": {"temperature": 0, "maxOutputTokens": 120, "responseMimeType": "application/json"},
            },
            timeout=20,
        )
        if response.status_code == 429:
            with _lock:
                _state["paused_until"] = datetime.now(timezone.utc) + PAUSE_AFTER_LIMIT
            log.warning("[reply_ai] Gemini limit reached; using word rules for 15 minutes")
            return rules(context)
        if response.status_code in (400, 403, 404):
            log.warning(f"[reply_ai] Gemini refused the request ({response.status_code}); check REMINDER_AI_MODEL / key")
        response.raise_for_status()
        parts = response.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])
        answer = parse_answer(parts[0].get("text", "") if parts else "")
        return answer or rules(context)
    except Exception as exc:
        log.warning(f"[reply_ai] Gemini unavailable: {type(exc).__name__}")
        return rules(context)
