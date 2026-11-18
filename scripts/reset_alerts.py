import os, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")
from supabase import create_client
from pipeline import alert_engine

sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

before = sb.table("alerts").select("id", count="exact").eq("resolved", False).execute().count
sb.table("alerts").update({"resolved": True}).eq("resolved", False).execute()
print(f"Resolved {before} old alerts")

alert_engine.run_all_checks()

rows = sb.table("alerts").select("severity,type,agent_name,message").eq("resolved", False).execute().data
print(f"Fresh open alerts: {len(rows)}")
for r in rows:
    print(f"  [{r['severity']}] {r['type']} — {r['agent_name']} — {r['message']}")
