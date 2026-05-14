"""
Batch AI analysis for all Maqsam calls not yet analyzed.
Targets: completed calls (duration > 30s) whose lead has no ai_analysis row.

Usage:  python run_ai_backfill.py
"""
import asyncio
import json as _json
import logging
import os
import sys
import time
from collections import defaultdict

import httpx
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ai_backfill")

SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
SB_H = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
}
SB_H_COUNT = {**SB_H, "Prefer": "count=exact"}

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = """You are an analytics engine for Fiper, a trading broker in Arabic-speaking markets.
Analyze the conversation between a Fiper agent and a lead.
Respond ONLY in valid JSON. No explanation, no markdown, no extra text.

{
  "sentiment": "positive" | "neutral" | "negative",
  "score": 0-100,
  "topics": [],
  "outcome": "converted"|"callback"|"not_interested"|"no_answer"|"ongoing",
  "follow_up_needed": true | false,
  "risk_flags": [],
  "treatment_score": 0-100,
  "summary": "Max 2 sentences. Arabic if conversation is Arabic, English if English."
}

topics options: pricing | product_fit | competitor | technical | follow_up | not_decision_maker | trading_education | account_info | greetings | profit_expectations
risk_flags options: unanswered | profit_expectations | beginner_risk | stale_callback | negative_sentiment | slow_response"""


async def fetch_all(c: httpx.AsyncClient, path: str, params: dict) -> list:
    """Paginate Supabase REST in chunks of 1000."""
    rows = []
    offset = 0
    while True:
        r = await c.get(f"{SB}/rest/v1/{path}", headers=SB_H,
                        params={**params, "limit": 1000, "offset": offset})
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def analyze(calls: list[dict]) -> dict:
    """Call Claude to analyze a set of calls for one lead."""
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    lines = []
    for c in sorted(calls, key=lambda x: x.get("called_at") or ""):
        lines.append(
            f"[{c.get('called_at', '')[:16]}] Agent call — "
            f"Duration: {c.get('duration_seconds') or 0}s, "
            f"Outcome: {c.get('outcome') or 'unknown'}, "
            f"Agent: {c.get('agent_name') or 'agent'}"
        )
    conversation_text = "\n".join(lines)

    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=512,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"Analyze this conversation:\n\n{conversation_text}"}],
    )
    raw = response.content[0].text.strip()
    try:
        return _json.loads(raw)
    except _json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}") + 1
        if start != -1:
            return _json.loads(raw[start:end])
        return {"sentiment": "neutral", "score": 0, "topics": [], "outcome": "ongoing",
                "follow_up_needed": False, "risk_flags": [], "treatment_score": 50,
                "summary": "Parse error."}


async def main():
    async with httpx.AsyncClient(timeout=60) as c:
        log.info("Loading data from Supabase...")

        # All leads on maqsam channel
        leads = await fetch_all(c, "leads", {"channel": "eq.maqsam", "select": "id"})
        lead_ids = {l["id"] for l in leads}
        log.info(f"  {len(lead_ids)} maqsam leads")

        # Already analyzed lead IDs
        analyzed = await fetch_all(c, "ai_analysis", {"select": "lead_id"})
        analyzed_ids = {a["lead_id"] for a in analyzed}
        log.info(f"  {len(analyzed_ids)} already analyzed")

        # Calls with duration > 30s
        calls_raw = await fetch_all(c, "calls",
                                    {"select": "lead_id,duration_seconds,outcome,agent_name,called_at",
                                     "duration_seconds": "gt.30"})
        calls_by_lead: dict[str, list] = defaultdict(list)
        for call in calls_raw:
            lid = call.get("lead_id")
            if lid:
                calls_by_lead[lid].append(call)
        log.info(f"  {len(calls_raw)} calls with duration > 30s across {len(calls_by_lead)} leads")

        # Leads to analyze: maqsam, has calls > 30s, not yet analyzed
        to_analyze = [
            lid for lid in lead_ids
            if lid not in analyzed_ids and lid in calls_by_lead
        ]
        log.info(f"  {len(to_analyze)} leads to analyze")

        if not to_analyze:
            log.info("Nothing to do — all eligible leads already analyzed.")
            return

        ok = 0
        err = 0
        for i, lead_id in enumerate(to_analyze, 1):
            calls = calls_by_lead[lead_id]
            try:
                result = analyze(calls)
                # Insert analysis
                r = await c.post(f"{SB}/rest/v1/ai_analysis",
                                 headers={**SB_H, "Prefer": "return=minimal"},
                                 content=_json.dumps({
                                     "lead_id": lead_id,
                                     "source": "maqsam",
                                     **result,
                                 }))
                r.raise_for_status()
                # Update lead score
                r2 = await c.patch(f"{SB}/rest/v1/leads",
                                   headers=SB_H,
                                   params={"id": f"eq.{lead_id}"},
                                   content=_json.dumps({"score": result.get("score")}))
                r2.raise_for_status()
                ok += 1
                if ok % 10 == 0:
                    log.info(f"  Progress: {i}/{len(to_analyze)} — {ok} OK, {err} errors")
                # Small delay to stay within Claude rate limits
                time.sleep(0.5)
            except Exception as e:
                log.error(f"  Lead {lead_id}: {e}")
                err += 1
                time.sleep(1)

        log.info(f"AI backfill complete — {ok} analyzed, {err} errors out of {len(to_analyze)}")

        # Final DB counts
        r = await c.get(f"{SB}/rest/v1/calls", headers=SB_H_COUNT, params={"select": "id", "limit": 1})
        log.info(f"DB calls: {r.headers.get('content-range', '?')}")
        r = await c.get(f"{SB}/rest/v1/leads", headers=SB_H_COUNT, params={"select": "id", "limit": 1})
        log.info(f"DB leads: {r.headers.get('content-range', '?')}")
        r = await c.get(f"{SB}/rest/v1/ai_analysis", headers=SB_H_COUNT, params={"select": "id", "limit": 1})
        log.info(f"DB ai_analysis: {r.headers.get('content-range', '?')}")


if __name__ == "__main__":
    asyncio.run(main())
