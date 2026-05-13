"""
Batch AI analysis — one-shot run.
Uses bulk Supabase queries to avoid N+1 round-trips.

WhatsApp leads  → analyze messages from messages table
Maqsam leads    → synthesize call context from calls table
Skips leads that already have an ai_analysis entry.
After analysis, re-runs alert_engine on fresh data.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

from supabase import create_client
from pipeline import ai_analyzer, alert_engine

sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

DELAY   = 0.35   # seconds between Claude calls
SINCE   = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()


def calls_to_messages(calls: list[dict]) -> list[dict]:
    """Convert Maqsam call records into the message format ai_analyzer expects."""
    msgs = []
    for c in sorted(calls, key=lambda x: x.get("called_at") or ""):
        dur     = c.get("duration_seconds") or 0
        outcome = c.get("outcome") or "unknown"
        agent   = c.get("agent_name") or "agent"
        ts      = c.get("called_at") or ""
        body    = f"Phone call — Duration: {dur}s, Outcome: {outcome}, Agent: {agent}"
        msgs.append({"direction": "outbound", "body": body, "sent_at": ts})
    return msgs


def save(lead_id: str, source: str, result: dict):
    sb.table("ai_analysis").insert({"lead_id": lead_id, "source": source, **result}).execute()
    if result.get("score") is not None:
        sb.table("leads").update({"score": result["score"]}).eq("id", lead_id).execute()


def run():
    print(f"\n{'='*60}")
    print(f"  Fiper Batch AI Analysis — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    # ── 1. Bulk-fetch everything needed ──────────────────────────────────────
    print("Loading data from Supabase...")

    leads = sb.table("leads").select("id,channel,phone,assigned_agent") \
              .gte("created_at", SINCE).execute().data or []

    # Set of lead_ids that already have analysis
    analyzed_ids = {
        r["lead_id"]
        for r in (sb.table("ai_analysis").select("lead_id").execute().data or [])
    }

    # All messages grouped by lead_id
    all_msgs_raw = sb.table("messages").select("lead_id,direction,body,sent_at") \
                     .execute().data or []
    msgs_by_lead = defaultdict(list)
    for m in all_msgs_raw:
        msgs_by_lead[m["lead_id"]].append(m)

    # All calls grouped by lead_id
    all_calls_raw = sb.table("calls").select("lead_id,duration_seconds,outcome,agent_name,called_at") \
                      .execute().data or []
    calls_by_lead = defaultdict(list)
    for c in all_calls_raw:
        if c.get("lead_id"):
            calls_by_lead[c["lead_id"]].append(c)

    print(f"  Leads (30d)      : {len(leads)}")
    print(f"  Already analyzed : {len(analyzed_ids)}")
    print(f"  Messages rows    : {len(all_msgs_raw)}")
    print(f"  Call rows        : {len(all_calls_raw)}")

    # ── 2. Filter to-do lists ─────────────────────────────────────────────────
    wa_todo = [l for l in leads if l["channel"] == "whatsapp" and l["id"] not in analyzed_ids]
    mq_todo = [l for l in leads if l["channel"] == "maqsam"   and l["id"] not in analyzed_ids]

    # Only process WhatsApp leads that actually have messages
    wa_with_msgs  = [l for l in wa_todo if msgs_by_lead.get(l["id"])]
    wa_no_msgs    = len(wa_todo) - len(wa_with_msgs)

    # Only process Maqsam leads that have at least one call linked
    mq_with_calls = [l for l in mq_todo if calls_by_lead.get(l["id"])]
    mq_no_calls   = len(mq_todo) - len(mq_with_calls)

    print(f"\nTo analyze:")
    print(f"  WhatsApp with messages  : {len(wa_with_msgs)}  (skipping {wa_no_msgs} with no msgs)")
    print(f"  Maqsam   with calls     : {len(mq_with_calls)} (skipping {mq_no_calls} with no call link)")

    total_to_run = len(wa_with_msgs) + len(mq_with_calls)
    est_minutes  = round(total_to_run * DELAY / 60, 1)
    print(f"  Total Claude calls      : {total_to_run}  (~{est_minutes} min at {DELAY}s/call)\n")

    stats = {"ok": 0, "errors": 0}
    sample_results = []

    # ── 3. WhatsApp leads ─────────────────────────────────────────────────────
    if wa_with_msgs:
        print(f"── WhatsApp ({len(wa_with_msgs)} leads) ──")
    for i, lead in enumerate(wa_with_msgs, 1):
        lid  = lead["id"]
        msgs = sorted(msgs_by_lead[lid], key=lambda m: m.get("sent_at") or "")
        try:
            result = ai_analyzer.analyze_conversation(msgs)
            save(lid, "whatsapp", result)
            stats["ok"] += 1
            print(f"  [{i}/{len(wa_with_msgs)}] score={result.get('score'):>3} "
                  f"sentiment={result.get('sentiment'):<8} "
                  f"outcome={result.get('outcome'):<15} {lead.get('phone','')}")
            if len(sample_results) < 3:
                sample_results.append({"ch": "WA", **result, "phone": lead.get("phone")})
            time.sleep(DELAY)
        except Exception as e:
            stats["errors"] += 1
            print(f"  [{i}/{len(wa_with_msgs)}] ERROR: {e}")

    # ── 4. Maqsam leads ───────────────────────────────────────────────────────
    if mq_with_calls:
        print(f"\n── Maqsam ({len(mq_with_calls)} leads) ──")
    for i, lead in enumerate(mq_with_calls, 1):
        lid   = lead["id"]
        calls = calls_by_lead[lid]
        try:
            synthetic = calls_to_messages(calls)
            result    = ai_analyzer.analyze_conversation(synthetic)
            save(lid, "maqsam", result)
            stats["ok"] += 1
            # Print every 25 or first
            if i == 1 or i % 25 == 0 or i == len(mq_with_calls):
                print(f"  [{i}/{len(mq_with_calls)}] score={result.get('score'):>3} "
                      f"sentiment={result.get('sentiment'):<8} "
                      f"outcome={result.get('outcome'):<15} {lead.get('phone','')}")
            if len(sample_results) < 6:
                sample_results.append({"ch": "MQ", **result, "phone": lead.get("phone")})
            time.sleep(DELAY)
        except Exception as e:
            stats["errors"] += 1
            print(f"  [{i}/{len(mq_with_calls)}] ERROR: {e}")

    # ── 5. Alert engine ───────────────────────────────────────────────────────
    print(f"\n── Re-running alert engine ──")
    try:
        alert_engine.run_all_checks()
        print("  Done.")
    except Exception as e:
        print(f"  Error: {e}")

    # ── 6. Summary ────────────────────────────────────────────────────────────
    ai_total    = sb.table("ai_analysis").select("id", count="exact").execute().count or 0
    alerts_open = sb.table("alerts").select("id", count="exact").eq("resolved", False).execute().count or 0

    print(f"\n{'='*60}  RESULTS")
    print(f"  Analyzed successfully : {stats['ok']}")
    print(f"  Errors                : {stats['errors']}")
    print(f"  ai_analysis rows now  : {ai_total}")
    print(f"  Open alerts now       : {alerts_open}")

    if sample_results:
        print(f"\n── Sample results ──")
        for s in sample_results:
            print(f"\n  [{s['ch']}] {s.get('phone','')}")
            print(f"    score={s.get('score')} | sentiment={s.get('sentiment')} | "
                  f"treatment={s.get('treatment_score')} | follow_up={s.get('follow_up_needed')}")
            print(f"    topics    : {s.get('topics')}")
            print(f"    risk_flags: {s.get('risk_flags')}")
            print(f"    summary   : {s.get('summary')}")
    print()


if __name__ == "__main__":
    run()
