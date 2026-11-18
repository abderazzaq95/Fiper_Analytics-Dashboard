import os, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")
from supabase import create_client

sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

alerts = sb.table("alerts").select("agent_name,severity,type,message,lead_id,resolved").execute().data
open_a = [a for a in alerts if not a.get("resolved")]
print(f"Open alerts: {len(open_a)}")
for a in open_a:
    print(f"  [{a['severity']}] {a['type']} | agent={a['agent_name']} | lead={a['lead_id']}")
    print(f"    {a['message']}")
