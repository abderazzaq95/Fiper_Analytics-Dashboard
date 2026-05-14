"""
Improved AI backfill — last 7 days only, using real Maqsam call transcripts.

Flow:
  1. Fetch 7-day calls from Maqsam API (includes full callTranscription, summary, sentiment)
  2. Update calls table with transcript / summary / sentiment / auto_tags
  3. Group calls by lead_id; skip leads already analyzed in this run's window
  4. Pass actual Arabic transcript to Claude for: score, treatment_score, risk_flags,
     follow_up_needed, topics, outcome, summary
  5. Use Maqsam's sentiment directly (skip Claude sentiment to save tokens)
  6. Skip Claude entirely if call has no transcript (no_answer / abandoned / 0s)

Usage:  py run_ai_backfill.py
"""
import asyncio
import base64
import json as _json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ai_backfill")

# ── Supabase ──────────────────────────────────────────────────────────────────
SB = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_KEY")
SB_H = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
}
SB_H_COUNT = {**SB_H, "Prefer": "count=exact"}

# ── Maqsam ────────────────────────────────────────────────────────────────────
_mq_key    = os.getenv("MAQSAM_ACCESS_KEY", "")
_mq_secret = os.getenv("MAQSAM_ACCESS_SECRET", "")
_mq_token  = base64.b64encode(f"{_mq_key}:{_mq_secret}".encode()).decode()
MQ_HEADERS = {"Authorization": f"Basic {_mq_token}"}
MQ_BASE    = "https://api.maqsam.com/v2"

# ── Claude ────────────────────────────────────────────────────────────────────
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL   = "claude-haiku-4-5-20251001"   # fast + cheap for bulk

SYSTEM_PROMPT = """You are a quality assurance analyst for Fiper, a trading broker in Arabic-speaking markets.
You will receive one or more call transcripts between Fiper agents and leads.
Maqsam has already determined the sentiment — do NOT re-evaluate sentiment; it is passed separately.

Analyze the conversation and respond ONLY in valid JSON. No explanation, no markdown, no extra text.

{
  "score": 0-100,
  "topics": [],
  "outcome": "converted"|"callback"|"not_interested"|"no_answer"|"ongoing",
  "follow_up_needed": true|false,
  "risk_flags": [],
  "treatment_score": 0-100,
  "summary": "Max 2 sentences. Arabic if the conversation is Arabic, English if English."
}

topics options: pricing|product_fit|competitor|technical|follow_up|not_decision_maker|trading_education|account_info|greetings|profit_expectations
risk_flags options: unanswered|profit_expectations|beginner_risk|stale_callback|negative_sentiment|slow_response

treatment_score rubric (0-100):
  90-100: Excellent — professional greeting, clear value prop, handled objections, agreed next step
  70-89:  Good — mostly professional, minor gaps
  50-69:  Average — some unprofessional moments or missed opportunities
  30-49:  Poor — multiple issues, disorganized, or pushy
  0-29:   Very poor — rude, scripted errors, or zero engagement"""


# ── Helpers ───────────────────────────────────────────────────────────────────

async def sb_fetch_all(c: httpx.AsyncClient, path: str, params: dict) -> list:
    """Paginate Supabase REST in 1000-row chunks."""
    rows, offset = [], 0
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


async def mq_fetch_calls(c: httpx.AsyncClient, date_from: str, date_to: str) -> list[dict]:
    """Fetch all Maqsam calls for a date range (paginated, 100/page)."""
    calls, page = [], 1
    while True:
        r = await c.get(f"{MQ_BASE}/calls", headers=MQ_HEADERS,
                        params={"date_from": date_from, "date_to": date_to, "page": page},
                        timeout=30)
        r.raise_for_status()
        data = r.json()
        batch = data.get("message", []) if isinstance(data, dict) else data
        if not batch:
            break
        calls.extend(batch)
        log.info(f"  Maqsam page {page}: {len(batch)} calls (total so far: {len(calls)})")
        if len(batch) < 100:
            break
        page += 1
    return calls


def build_transcript_text(call: dict) -> str:
    """Convert callTranscription list to readable text."""
    lines = call.get("callTranscription") or []
    if not lines:
        return ""
    parts = []
    for seg in lines:
        party = "Agent" if seg.get("party") == "agent" else "Customer"
        content = seg.get("content", "").strip()
        if content:
            parts.append(f"{party}: {content}")
    return "\n".join(parts)


def build_claude_prompt(calls: list[dict]) -> str | None:
    """
    Build the full prompt text for one lead's calls.
    Returns None if no call has a transcript (nothing for Claude to analyze).
    """
    blocks = []
    for i, call in enumerate(sorted(calls, key=lambda x: x.get("timestamp") or 0), 1):
        ts = call.get("timestamp")
        dt_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if ts else "unknown"
        duration = call.get("duration") or 0
        state    = call.get("state", "unknown")
        agents   = call.get("agents") or []
        agent_name = agents[0].get("name") if agents else "unknown"
        sentiment  = call.get("sentiment") or "unknown"
        summary_en = (call.get("summary") or {}).get("en") or ""
        auto_tags  = ", ".join(call.get("callAutoTags") or []) or "none"
        transcript = build_transcript_text(call)

        block = [
            f"=== CALL {i} — {dt_str} | Agent: {agent_name} | Duration: {duration}s | State: {state} ===",
            f"Maqsam Sentiment: {sentiment}",
            f"Auto Tags: {auto_tags}",
        ]
        if summary_en:
            block.append(f"Summary: {summary_en}")
        if transcript:
            block.append(f"\nTranscript:\n{transcript}")
        else:
            block.append("(No transcript — call was not answered or too short)")
        blocks.append("\n".join(block))

    if not blocks:
        return None

    # Only proceed with Claude if at least one call has a real transcript
    has_transcript = any(build_transcript_text(c) for c in calls)
    if not has_transcript:
        return None

    return "\n\n".join(blocks)


def call_claude(prompt: str, maqsam_sentiment: str | None) -> dict:
    """Call Claude via raw httpx. Returns parsed JSON dict."""
    user_msg = f"Maqsam sentiment (pre-determined, do not change): {maqsam_sentiment or 'unknown'}\n\nAnalyze these calls:\n\n{prompt}"
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 512,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_msg}],
    }
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        content=_json.dumps(payload),
        timeout=30,
    )
    resp.raise_for_status()
    raw = resp.json()["content"][0]["text"].strip()
    try:
        return _json.loads(raw)
    except _json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}") + 1
        if start != -1:
            return _json.loads(raw[start:end])
        return {"score": 0, "topics": [], "outcome": "ongoing",
                "follow_up_needed": False, "risk_flags": [],
                "treatment_score": 50, "summary": "Parse error."}


async def main():
    # ── Date range: last 7 days ───────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    date_to   = now.strftime("%Y-%m-%d")
    log.info(f"Backfill window: {date_from} → {date_to}")

    async with httpx.AsyncClient(timeout=60) as c:

        # ── 1. Fetch 7-day calls from Maqsam API ──────────────────────────────
        log.info("Fetching calls from Maqsam API...")
        mq_calls = await mq_fetch_calls(c, date_from, date_to)
        log.info(f"  {len(mq_calls)} total Maqsam calls in window")

        # Filter to calls with duration >= 30s
        qualified = [call for call in mq_calls if (call.get("duration") or 0) >= 30]
        log.info(f"  {len(qualified)} calls with duration >= 30s")

        with_transcript = [c2 for c2 in qualified if c2.get("callTranscription")]
        log.info(f"  {len(with_transcript)} calls WITH transcripts")
        log.info(f"  {len(qualified) - len(with_transcript)} calls without transcripts")

        if not qualified:
            log.info("Nothing to process.")
            return

        # ── 2. Build maqsam_id → call lookup ─────────────────────────────────
        mq_id_to_call = {str(call["id"]): call for call in qualified}
        mq_ids = list(mq_id_to_call.keys())

        # ── 3. Look up matching DB call rows (get lead_id + maqsam_id) ────────
        log.info("Looking up call records in Supabase...")
        db_calls = await sb_fetch_all(c, "calls",
                                      {"select": "id,maqsam_id,lead_id",
                                       "maqsam_id": f"in.({','.join(mq_ids)})"})
        log.info(f"  {len(db_calls)} matching rows found in DB")

        # Map maqsam_id → db row
        mq_id_to_db = {row["maqsam_id"]: row for row in db_calls if row.get("maqsam_id")}

        # ── 4. Update calls table with transcript / summary / sentiment ────────
        log.info("Updating calls table with transcript data...")
        updated = skipped_no_col = 0
        for mq_id, call in mq_id_to_call.items():
            if mq_id not in mq_id_to_db:
                continue  # call not in DB yet (will be ingested on next poll)

            transcript_text = build_transcript_text(call)
            summary_obj = call.get("summary") or {}
            sentiment   = call.get("sentiment")
            auto_tags   = call.get("callAutoTags") or []

            patch_data = {}
            if transcript_text:
                patch_data["transcript"] = transcript_text
            if summary_obj.get("en"):
                patch_data["summary_en"] = summary_obj["en"]
            if summary_obj.get("ar"):
                patch_data["summary_ar"] = summary_obj["ar"]
            if sentiment:
                patch_data["maqsam_sentiment"] = sentiment
            if auto_tags:
                patch_data["auto_tags"] = auto_tags

            if not patch_data:
                continue

            try:
                r = await c.patch(
                    f"{SB}/rest/v1/calls",
                    headers=SB_H,
                    params={"maqsam_id": f"eq.{mq_id}"},
                    content=_json.dumps(patch_data),
                )
                if r.status_code == 400 and "column" in r.text.lower():
                    skipped_no_col += 1
                    if skipped_no_col == 1:
                        log.warning("Column missing — run migrate_calls_columns.py first! Continuing without saving transcript to DB.")
                else:
                    r.raise_for_status()
                    updated += 1
            except Exception as e:
                log.warning(f"  Failed to update call {mq_id}: {e}")

        log.info(f"  {updated} call rows updated with transcript data")
        if skipped_no_col:
            log.warning(f"  {skipped_no_col} skipped (missing columns — run SQL migration)")

        # ── 5. Group calls by lead_id ──────────────────────────────────────────
        calls_by_lead: dict[str, list] = defaultdict(list)
        for mq_id, call in mq_id_to_call.items():
            db_row = mq_id_to_db.get(mq_id)
            if db_row and db_row.get("lead_id"):
                calls_by_lead[db_row["lead_id"]].append(call)

        log.info(f"  {len(calls_by_lead)} unique leads with qualifying calls")

        # ── 6. Get leads already analyzed (window: last 7 days) ───────────────
        since_iso = (now - timedelta(days=7)).isoformat()
        already_done = await sb_fetch_all(c, "ai_analysis",
                                          {"select": "lead_id",
                                           "created_at": f"gte.{since_iso}"})
        done_ids = {a["lead_id"] for a in already_done}
        log.info(f"  {len(done_ids)} leads already analyzed in this window")

        to_analyze = [lid for lid in calls_by_lead if lid not in done_ids]
        log.info(f"  {len(to_analyze)} leads to analyze")

        if not to_analyze:
            log.info("All leads in window already analyzed.")
        else:
            # ── 7. Run Claude per lead ─────────────────────────────────────────
            ok = err = skipped_no_transcript = 0

            for i, lead_id in enumerate(to_analyze, 1):
                calls = calls_by_lead[lead_id]

                # Determine dominant Maqsam sentiment across calls
                sentiments = [c2.get("sentiment") for c2 in calls if c2.get("sentiment")]
                mq_sentiment = (
                    "negative" if "negative" in sentiments else
                    "positive" if "positive" in sentiments else
                    sentiments[0] if sentiments else None
                )

                prompt = build_claude_prompt(calls)
                if prompt is None:
                    skipped_no_transcript += 1
                    continue

                try:
                    result = call_claude(prompt, mq_sentiment)

                    # Override sentiment with Maqsam's value
                    result["sentiment"] = mq_sentiment or result.get("sentiment", "neutral")

                    # Upsert ai_analysis (delete old + insert, or just insert)
                    r = await c.post(
                        f"{SB}/rest/v1/ai_analysis",
                        headers={**SB_H, "Prefer": "return=minimal"},
                        content=_json.dumps({
                            "lead_id": lead_id,
                            "source": "maqsam",
                            **result,
                        }),
                    )
                    if r.status_code == 409:
                        # Already exists — update instead
                        r2 = await c.patch(
                            f"{SB}/rest/v1/ai_analysis",
                            headers=SB_H,
                            params={"lead_id": f"eq.{lead_id}"},
                            content=_json.dumps(result),
                        )
                        r2.raise_for_status()
                    else:
                        r.raise_for_status()

                    # Update lead score
                    r3 = await c.patch(
                        f"{SB}/rest/v1/leads",
                        headers=SB_H,
                        params={"id": f"eq.{lead_id}"},
                        content=_json.dumps({"score": result.get("score")}),
                    )
                    r3.raise_for_status()

                    ok += 1
                    if ok % 10 == 0:
                        log.info(f"  Progress: {i}/{len(to_analyze)} processed — {ok} analyzed, {err} errors, {skipped_no_transcript} skipped (no transcript)")

                    time.sleep(0.3)  # stay within Claude rate limits

                except Exception as e:
                    log.error(f"  Lead {lead_id}: {e}")
                    err += 1
                    time.sleep(1)

            log.info(f"\nAI backfill complete:")
            log.info(f"  Analyzed:              {ok}")
            log.info(f"  Skipped (no transcript): {skipped_no_transcript}")
            log.info(f"  Errors:                {err}")
            log.info(f"  Total leads processed: {len(to_analyze)}")

        # ── 8. Final DB counts ────────────────────────────────────────────────
        log.info("\nFinal DB counts:")
        for table in ("calls", "leads", "ai_analysis"):
            r = await c.get(f"{SB}/rest/v1/{table}", headers=SB_H_COUNT,
                            params={"select": "id", "limit": 1})
            log.info(f"  {table}: {r.headers.get('content-range', '?')}")

        log.info("\nTranscript coverage summary (last 7 days):")
        log.info(f"  Maqsam API calls fetched:  {len(mq_calls)}")
        log.info(f"  Duration >= 30s:            {len(qualified)}")
        log.info(f"  With full transcript:       {len(with_transcript)}")
        log.info(f"  Without transcript:         {len(qualified) - len(with_transcript)}")


if __name__ == "__main__":
    asyncio.run(main())
