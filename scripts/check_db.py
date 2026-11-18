import os, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")
from supabase import create_client
from collections import Counter

sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

leads  = sb.table("leads").select("id,channel,status,assigned_agent").execute().data
calls  = sb.table("calls").select("id,duration_seconds").execute().data
msgs   = sb.table("messages").select("id,direction").execute().data
alerts = sb.table("alerts").select("id,severity,type,resolved").eq("resolved", False).execute().data
ai     = sb.table("ai_analysis").select("id").execute().data

channels = Counter(l.get("channel") for l in leads)
statuses = Counter(l.get("status") for l in leads)
agents   = Counter(l.get("assigned_agent") for l in leads if l.get("assigned_agent"))
durs     = [c["duration_seconds"] for c in calls if c.get("duration_seconds")]

print(f"LEADS  total    : {len(leads)}")
print(f"  by channel    : {dict(channels)}")
print(f"  by status     : {dict(statuses)}")
print(f"CALLS  total    : {len(calls)}")
print(f"  avg duration  : {round(sum(durs)/len(durs),1) if durs else 0}s")
print(f"MESSAGES total  : {len(msgs)}")
print(f"  inbound       : {sum(1 for m in msgs if m['direction']=='inbound')}")
print(f"  outbound      : {sum(1 for m in msgs if m['direction']=='outbound')}")
print(f"AGENTS ({len(agents)} unique):")
for a, c in sorted(agents.items(), key=lambda x: -x[1]):
    print(f"  {a}: {c} leads")
print(f"OPEN ALERTS     : {len(alerts)}")
for a in alerts:
    print(f"  [{a['severity']}] {a['type']}")
print(f"AI ANALYSES     : {len(ai)}")
