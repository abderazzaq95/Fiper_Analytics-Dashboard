import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"

SYSTEM_PROMPT = """You are an analytics engine for Fiper, a trading broker in Arabic-speaking markets.
Analyze the conversation between a Fiper agent and a lead.

IMPORTANT — ROLES: The AGENT is the Fiper employee making the outbound call. The CUSTOMER or LEAD is the person being called by Fiper. Always evaluate agent quality from the Fiper employee's perspective.
IMPORTANT — COMPANY NAME: The company is called FIPER (فايبر in Arabic). It is a trading broker.
Always write "Fiper" in the summary. Never write "Viber", "Fighter", "Faiber", or "financial brokerage company". Never say "financial brokerage" — always say "Fiper".
Write the summary in the same language as the conversation: Arabic if the conversation is in Arabic, English if in English.

Respond ONLY in valid JSON. No explanation, no markdown, no extra text.

{
  "sentiment": "positive" | "neutral" | "negative",
  "score": 0-100,
  "topics": [],
  "outcome": "converted"|"callback"|"not_interested"|"no_answer"|"ongoing",
  "follow_up_needed": true | false,
  "risk_flags": [],
  "treatment_score": 0-100,
  "summary": "Max 2 sentences in the conversation language. Use the company name Fiper."
}

topics options: pricing | product_fit | competitor | technical | follow_up | not_decision_maker | trading_education | account_info | greetings | profit_expectations
risk_flags options: unanswered | profit_expectations | beginner_risk | stale_callback | negative_sentiment | slow_response"""


def format_conversation(messages: list[dict]) -> str:
    lines = []
    for m in messages:
        direction = m.get("direction", "unknown")
        role = "Agent" if direction == "outbound" else "Lead"
        body = m.get("body", "")
        ts = m.get("sent_at", "")
        lines.append(f"[{ts}] {role}: {body}")
    return "\n".join(lines)


def analyze_conversation(messages: list[dict]) -> dict:
    conversation_text = format_conversation(messages)
    if not conversation_text.strip():
        return _empty_result()

    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is missing")

    response = requests.post(
        f"{GEMINI_URL}?key={GEMINI_API_KEY}",
        headers={"content-type": "application/json"},
        json={
            "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "contents": [{
                "role": "user",
                "parts": [{"text": f"Analyze this conversation:\n\n{conversation_text}"}],
            }],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 700,
                "responseMimeType": "application/json",
            },
        },
        timeout=30,
    )
    response.raise_for_status()

    raw = (
        response.json()
        .get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
        .strip()
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        return json.loads(raw[start:end]) if start != -1 else _empty_result()


def _empty_result() -> dict:
    return {
        "sentiment": "neutral",
        "score": 0,
        "topics": [],
        "outcome": "ongoing",
        "follow_up_needed": False,
        "risk_flags": [],
        "treatment_score": 50,
        "summary": "No conversation data available.",
    }
