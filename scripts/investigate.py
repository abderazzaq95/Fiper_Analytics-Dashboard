import os, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")
from supabase import create_client
from collections import Counter
from pipeline import ai_analyzer

sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

# ── 1. Treatment score distribution ──────────────────────────────────────────
rows = sb.table("ai_analysis").select("lead_id,treatment_score,source,sentiment,score,topics,risk_flags,summary").execute().data
scores = [r["treatment_score"] for r in rows if r.get("treatment_score") is not None]

print("=== 1. Treatment Score Distribution ===")
print(f"Total ai_analysis rows : {len(rows)}")
print(f"treatment_score =  0   : {sum(1 for s in scores if s == 0)}")
print(f"treatment_score 1-49   : {sum(1 for s in scores if 0 < s < 50)}")
print(f"treatment_score 50-100 : {sum(1 for s in scores if s >= 50)}")
print(f"by source: {dict(Counter(r['source'] for r in rows))}")

# ── 2. Sample ai_analysis rows ────────────────────────────────────────────────
print("\n=== 2. Sample ai_analysis rows (first 5) ===")
for r in rows[:5]:
    lid = r["lead_id"][:8] if r.get("lead_id") else "?"
    print(f"  lead={lid}... source={r['source']} score={r['score']} "
          f"treatment={r['treatment_score']} sentiment={r['sentiment']}")
    print(f"  topics={r.get('topics')}  risk={r.get('risk_flags')}")
    print(f"  summary: {r.get('summary')}")
    print()

# ── 3. What data is being sent to Claude for a Maqsam lead ───────────────────
print("=== 3. Sample data sent to Claude (Maqsam lead) ===")
# Pick a Maqsam lead that has a call
mq_lead = sb.table("leads").select("id,phone,assigned_agent").eq("channel", "maqsam").limit(1).execute().data
if mq_lead:
    lid = mq_lead[0]["id"]
    calls = sb.table("calls").select("duration_seconds,outcome,agent_name,called_at") \
               .eq("lead_id", lid).order("called_at").execute().data
    print(f"Lead: {mq_lead[0]['phone']} — {len(calls)} call(s)")
    synthetic = []
    for c in calls:
        dur     = c.get("duration_seconds") or 0
        outcome = c.get("outcome") or "unknown"
        agent   = c.get("agent_name") or "agent"
        ts      = c.get("called_at") or ""
        body    = f"Phone call — Duration: {dur}s, Outcome: {outcome}, Agent: {agent}"
        synthetic.append({"direction": "outbound", "body": body, "sent_at": ts})
    print("Synthetic messages sent to Claude:")
    for m in synthetic:
        print(f"  [{m['sent_at']}] {m['body']}")

    # Show what format_conversation produces
    print("\nFormatted conversation text Claude receives:")
    print(ai_analyzer.format_conversation(synthetic))

# ── 4. What data for a WA lead with a real message ───────────────────────────
print("\n=== 4. Sample data sent to Claude (WhatsApp lead with message) ===")
msgs = sb.table("messages").select("lead_id,direction,body,sent_at,agent_name").execute().data
if msgs:
    lid = msgs[0]["lead_id"]
    lead_msgs = [m for m in msgs if m["lead_id"] == lid]
    print(f"Lead: {lid[:8]}... — {len(lead_msgs)} message(s)")
    print("Formatted conversation text Claude receives:")
    print(ai_analyzer.format_conversation(lead_msgs))
else:
    print("  No WhatsApp messages found.")

# ── 5. Current open alerts ────────────────────────────────────────────────────
print("\n=== 5. Current open alerts ===")
alert_count = sb.table("alerts").select("id", count="exact").eq("resolved", False).execute().count
print(f"Total open: {alert_count}")
alert_rows = sb.table("alerts").select("type,severity,message").eq("resolved", False).limit(10).execute().data
by_type = Counter(r["type"] for r in
    sb.table("alerts").select("type").eq("resolved", False).execute().data)
for t, c in by_type.most_common():
    print(f"  {t}: {c}")
print("\nSample messages:")
for r in alert_rows[:3]:
    print(f"  [{r['severity']}] {r['type']}: {r['message']}")
