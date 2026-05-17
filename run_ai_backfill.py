"""Force-reanalyze the top N leads with the updated AI prompt.

Usage:
    py run_ai_backfill.py --force-reanalyze --limit 20
    py run_ai_backfill.py --force-reanalyze --limit 20 --dry-run
"""
import argparse
import asyncio
import json
import os
from collections import defaultdict

import httpx
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SB_URL        = os.getenv("SUPABASE_URL")
SB_KEY        = os.getenv("SUPABASE_SERVICE_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL  = "claude-haiku-4-5-20251001"

supabase = create_client(SB_URL, SB_KEY)

# Matches the updated prompt in main.py — enforces "Fiper" company name
SYSTEM_PROMPT = """You are a quality assurance analyst for Fiper, a trading broker in Arabic-speaking markets.
Analyze the call transcript(s) between Fiper agents and leads.
Maqsam has already determined the sentiment — do NOT re-evaluate it; it is passed to you separately.

IMPORTANT — ROLES: The AGENT is the Fiper employee making the outbound call. The CUSTOMER or LEAD is the person being called by Fiper. Always evaluate agent quality from the Fiper employee's perspective.
IMPORTANT — COMPANY NAME: The company is called FIPER (فايبر in Arabic). It is a trading broker.
Always write "Fiper" in the summary. Never write "Viber", "Fighter", "Faiber", or "financial brokerage company". Never say "financial brokerage" — always say "Fiper".
Write the summary in the same language as the conversation: Arabic if the conversation is in Arabic, English if in English.

Respond ONLY in valid JSON. No explanation, no markdown, no extra text.

{
  "score": 0-100,
  "topics": [],
  "outcome": "converted"|"callback"|"not_interested"|"no_answer"|"ongoing",
  "follow_up_needed": true|false,
  "risk_flags": [],
  "treatment_score": 0-100,
  "summary": "Max 2 sentences in the conversation language. Use the company name Fiper."
}

topics: pricing|product_fit|competitor|technical|follow_up|not_decision_maker|trading_education|account_info|greetings|profit_expectations
risk_flags: unanswered|profit_expectations|beginner_risk|stale_callback|negative_sentiment|slow_response
treatment_score: 90-100 excellent, 70-89 good, 50-69 average, 30-49 poor, 0-29 very poor"""


async def call_claude(user_message: str) -> dict:
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            content=json.dumps({
                "model": CLAUDE_MODEL,
                "max_tokens": 512,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_message}],
            }),
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        s, e = raw.find("{"), raw.rfind("}") + 1
        if s != -1:
            return json.loads(raw[s:e])
        return {}


async def reanalyze_lead(lead: dict, calls_by: dict, msgs_by: dict, dry_run: bool) -> str:
    lid     = lead["id"]
    channel = lead.get("channel", "")
    phone   = lead.get("phone") or lid[:8]

    if channel == "maqsam":
        calls = sorted(calls_by.get(lid, []), key=lambda c: c.get("called_at") or "")
        if not calls or not any(c.get("transcript") for c in calls):
            return f"  SKIP  {phone} — no transcript"

        blocks, sentiments = [], []
        for i, c in enumerate(calls, 1):
            transcript = (c.get("transcript") or "").strip()
            sentiment  = c.get("maqsam_sentiment")
            if sentiment:
                sentiments.append(sentiment)
            block = (
                f"=== CALL {i} | Duration: {c.get('duration_seconds')}s | "
                f"State: {c.get('outcome')} | Agent: {c.get('agent_name') or 'unknown'} ==="
            )
            if sentiment:
                block += f"\nMaqsam Sentiment: {sentiment}"
            if c.get("summary_en"):
                block += f"\nSummary: {c['summary_en']}"
            if transcript:
                block += f"\n\nTranscript:\n{transcript[:3000]}"
            blocks.append(block)

        mq_sentiment = (
            "negative" if "negative" in sentiments else
            "positive" if "positive" in sentiments else
            (sentiments[0] if sentiments else None)
        )
        user_msg = (
            f"Maqsam sentiment (pre-determined): {mq_sentiment or 'unknown'}\n\n"
            f"Analyze these calls:\n\n" + "\n\n".join(blocks)
        )
        source = "maqsam"

    elif channel == "whatsapp":
        msgs = sorted(msgs_by.get(lid, []), key=lambda m: m.get("sent_at") or "")
        if not msgs:
            return f"  SKIP  {phone} — no messages"
        lines = []
        for m in msgs:
            role = "Agent" if m.get("direction") == "outbound" else "Customer"
            lines.append(f"{role}: {(m.get('body') or '')[:300]}")
        user_msg = (
            "Maqsam sentiment (pre-determined): unknown\n\n"
            "Analyze this WhatsApp conversation:\n\n" + "\n".join(lines)
        )
        mq_sentiment = None
        source = "whatsapp"

    else:
        return f"  SKIP  {phone} — unknown channel '{channel}'"

    if dry_run:
        return f"  DRY   {phone} (channel={channel}, score={lead.get('score')})"

    result = await call_claude(user_msg)
    if not result:
        return f"  ERROR {phone} — empty Claude response"

    if mq_sentiment:
        result["sentiment"] = mq_sentiment

    # Overwrite existing ai_analysis row, or insert new one
    existing = (
        supabase.table("ai_analysis").select("id").eq("lead_id", lid).execute().data or []
    )
    if existing:
        supabase.table("ai_analysis").update(result).eq("lead_id", lid).execute()
    else:
        supabase.table("ai_analysis").insert({"lead_id": lid, "source": source, **result}).execute()

    supabase.table("leads").update({"score": result.get("score")}).eq("id", lid).execute()

    summary_preview = (result.get("summary") or "")[:80]
    return f"  OK    {phone} score={result.get('score')} — {repr(summary_preview)}"


async def main(limit: int, dry_run: bool):
    print(f"Fetching top {limit} leads by score...")
    leads = (
        supabase.table("leads")
        .select("id,phone,channel,score")
        .gt("score", 0)
        .order("score", desc=True)
        .limit(limit)
        .execute()
        .data or []
    )
    if not leads:
        print("No scored leads found.")
        return

    lead_ids = [l["id"] for l in leads]
    print(f"Found {len(leads)} leads. Fetching calls and messages...")

    all_calls = (
        supabase.table("calls")
        .select("lead_id,agent_name,duration_seconds,outcome,called_at,transcript,maqsam_sentiment,summary_en")
        .in_("lead_id", lead_ids)
        .gt("duration_seconds", 0)
        .execute()
        .data or []
    )
    all_msgs = (
        supabase.table("messages")
        .select("lead_id,direction,body,sent_at")
        .in_("lead_id", lead_ids)
        .execute()
        .data or []
    )

    calls_by: dict = defaultdict(list)
    msgs_by:  dict = defaultdict(list)
    for c in all_calls:
        if c.get("lead_id"):
            calls_by[c["lead_id"]].append(c)
    for m in all_msgs:
        if m.get("lead_id"):
            msgs_by[m["lead_id"]].append(m)

    print(f"Reanalyzing {len(leads)} leads (dry_run={dry_run})...\n")
    ok = skip = err = 0
    for lead in leads:
        line = await reanalyze_lead(lead, calls_by, msgs_by, dry_run)
        print(line)
        if "OK"    in line: ok   += 1
        elif "SKIP" in line: skip += 1
        elif "DRY"  in line: skip += 1
        else:                err  += 1
        if not dry_run:
            await asyncio.sleep(0.5)  # stay within Claude rate limits

    print(f"\nDone — {ok} updated, {skip} skipped, {err} errors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-reanalyze", action="store_true",
                        help="Required flag to actually run (safety guard)")
    parser.add_argument("--limit", type=int, default=20,
                        help="Number of top leads to reanalyze (default 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without writing to DB")
    args = parser.parse_args()

    if not args.force_reanalyze:
        print("Add --force-reanalyze to run. Use --dry-run to preview.")
    else:
        asyncio.run(main(args.limit, args.dry_run))
