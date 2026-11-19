"""Does a waiting customer really need an answer, and how urgently?

The AI reads the customer's latest messages together with the agent's last
message. Gemini (free tier) is asked first; when it fails or is out of quota,
Claude (Anthropic API) answers instead. Word rules are the last fallback; they
never decide "no reply needed": a failing AI must not silence a reminder.

Cost/quota guards: callers limit how many checks run per cycle, Gemini calls
are spaced, an error pauses that provider for a while, and there is a daily cap.
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
MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("REMINDER_AI_SPACING_SECONDS", "4.5"))  # Gemini free tier
PAUSE_AFTER_ERROR = timedelta(minutes=15)
# Models that support server-side refusal fallbacks (fallbacks="default").
_CLAUDE_FALLBACK_MODELS = {"claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5", "claude-fable-5-1", "claude-fable-5-2"}

URGENT = re.compile(
    r"(سحب|ايداع|إيداع|حوال[ةه]|تحويل|رصيد|ارباح|أرباح|فلوس|مبلغ|مصاري|شكوى|احتيال|نصب|"
    r"ليش ما|ما ?ترد|ما ?حدا رد|ما ?رديت|صارلكم|withdraw|deposit|refund|money|balance|scam)",
    re.I)
GREETING = re.compile(
    r"^(ال?سلام عليكم|السلام|سلام|مرحب\S*|اهلا\S*|أهلا\S*|هلا\S*|hi+|hello|hey|صباح الخير|مساء الخير|"
    r"صباح النور|مساء النور|good (morning|evening))\b", re.I)

ANSWER_SCHEMA = {
    "type": "object",
    "properties": {
        "needs_reply": {"type": "boolean"},
        "priority": {"type": "string", "enum": list(PRIORITIES)},
        "reason": {"type": "string"},
    },
    "required": ["needs_reply", "priority", "reason"],
    "additionalProperties": False,
}

_state = {"day": None, "used": 0, "last_call": 0.0, "last_error": None, "paused": {"gemini": None, "claude": None}}
_lock = threading.Lock()
_claude_client = None


def mode() -> str:
    value = os.getenv("REMINDER_AI_MODE", "live").strip().lower()
    return value if value in {"off", "shadow", "live"} else "live"


# ── Word rules (last fallback) ────────────────────────────────────────────────

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


# ── Shared prompt + answer parsing ────────────────────────────────────────────

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


def parse_answer(text: str, source: str = "gemini") -> dict | None:
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
        "source": source,
    }


def _clean(message) -> str:
    # Provider error text, never anything that looks like a key.
    text = " ".join(str(message or "").split())
    return re.sub(r"(key=|AIza|sk-ant-)[\w-]+", "[key]", text)


# ── Availability / limits ─────────────────────────────────────────────────────

def _key(provider: str) -> str:
    return (os.getenv("GEMINI_API_KEY") if provider == "gemini" else os.getenv("ANTHROPIC_API_KEY")) or ""


def _ready(provider: str) -> bool:
    paused = _state["paused"].get(provider)
    return bool(_key(provider)) and not (paused and datetime.now(timezone.utc) < paused)


def _pause(provider: str) -> None:
    with _lock:
        _state["paused"][provider] = datetime.now(timezone.utc) + PAUSE_AFTER_ERROR


def available() -> bool:
    with _lock:
        today = date.today()
        if _state["day"] != today:
            _state.update(day=today, used=0)
        if _state["used"] >= DAILY_LIMIT:
            return False
    return _ready("gemini") or _ready("claude")


def _fallback(context: dict, why: str) -> dict:
    """Word rules, with the reason the AI was not used (stored in ai_reason)."""
    _state["last_error"] = why
    return {**rules(context), "reason": f"word rules · {why}"[:120]}


# ── Providers ─────────────────────────────────────────────────────────────────

def _ask_gemini(context: dict) -> tuple[dict | None, str]:
    with _lock:
        wait = _state["last_call"] + MIN_SECONDS_BETWEEN_CALLS - time.time()
    if wait > 0:
        time.sleep(wait)
    _state["last_call"] = time.time()
    # Own setting on purpose: GEMINI_MODEL may still name a retired 1.5 model.
    model = os.getenv("REMINDER_AI_MODEL", "").strip() or "gemini-2.5-flash-lite"
    try:
        response = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            params={"key": _key("gemini")},
            json={
                "contents": [{"role": "user", "parts": [{"text": _prompt(context)}]}],
                "generationConfig": {"temperature": 0, "maxOutputTokens": 120, "responseMimeType": "application/json"},
            },
            timeout=20,
        )
    except Exception as exc:
        return None, f"Gemini unreachable ({type(exc).__name__})"
    if response.status_code >= 400:
        try:
            message = response.json().get("error", {}).get("message", "")
        except ValueError:
            message = ""
        _pause("gemini")  # quota (429) or setup problem: don't retry every check
        return None, f"Gemini HTTP {response.status_code}: {_clean(message)}"[:100]
    try:
        parts = response.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])
        answer = parse_answer(parts[0].get("text", "") if parts else "", "gemini")
    except Exception:
        answer = None
    return answer, "" if answer else "Gemini answer unreadable"


def _claude():
    global _claude_client
    if _claude_client is None:
        import anthropic
        _claude_client = anthropic.Anthropic(api_key=_key("claude"), timeout=60.0, max_retries=1)
    return _claude_client


def _ask_claude(context: dict) -> tuple[dict | None, str]:
    import anthropic

    model = os.getenv("REMINDER_AI_CLAUDE_MODEL", "").strip() or "claude-opus-5-5"
    request = {
        "model": model,
        "max_tokens": 2000,  # thinking counts toward it; the answer itself is ~40 tokens
        "output_config": {"effort": "low", "format": {"type": "json_schema", "schema": ANSWER_SCHEMA}},
        "messages": [{"role": "user", "content": _prompt(context)}],
    }
    try:
        if model in _CLAUDE_FALLBACK_MODELS:
            # Server-side refusal fallback: a declined request is re-run on another model.
            response = _claude().beta.messages.create(
                **request, betas=["server-side-fallback-2026-07-01"], fallbacks="default")
        else:
            response = _claude().messages.create(**request)
    except anthropic.APIConnectionError as exc:
        return None, f"Claude unreachable ({type(exc).__name__})"
    except anthropic.APIStatusError as exc:
        _pause("claude")
        return None, f"Claude HTTP {exc.status_code}: {_clean(getattr(exc, 'message', ''))}"[:100]
    if response.stop_reason in ("refusal", "max_tokens"):
        return None, f"Claude stopped: {response.stop_reason}"
    text = next((block.text for block in response.content if block.type == "text"), "")
    answer = parse_answer(text, "claude")
    return answer, "" if answer else "Claude answer unreadable"


def classify(context: dict) -> dict:
    """AI decision (Gemini, then Claude), or the word rules when neither can answer."""
    if not (_key("gemini") or _key("claude")):
        return _fallback(context, "no AI key set (GEMINI_API_KEY / ANTHROPIC_API_KEY)")
    if not available():
        return _fallback(context, _state.get("last_error") or "AI paused or daily limit")
    with _lock:
        _state["used"] += 1
    problems = []
    for provider, ask in (("gemini", _ask_gemini), ("claude", _ask_claude)):
        if not _ready(provider):
            continue
        answer, why = ask(context)
        if answer:
            _state["last_error"] = None
            return answer
        log.warning(f"[reply_ai] {why}")
        problems.append(why)
    return _fallback(context, " / ".join(problems) or "AI paused")
