import sys, os
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_KEY'))

rows = sb.table('calls').select('outcome').execute().data or []
counts = {}
for r in rows:
    k = r.get('outcome') or 'NULL'
    counts[k] = counts.get(k, 0) + 1
print('=== calls.outcome ===')
for k, v in sorted(counts.items(), key=lambda x: -x[1]):
    print(f'  {repr(k):35s} {v}')
print(f'  Total: {len(rows)}')

leads = sb.table('leads').select('assigned_agent,channel').execute().data or []
with_agent = [l for l in leads if l.get('assigned_agent')]
print(f'\n=== leads.assigned_agent ===')
print(f'  Total: {len(leads)}  With agent: {len(with_agent)}')
by_ch = {}
for l in leads:
    ch = l.get('channel','?')
    by_ch.setdefault(ch, {'total':0,'with_agent':0})
    by_ch[ch]['total'] += 1
    if l.get('assigned_agent'): by_ch[ch]['with_agent'] += 1
for ch, d in sorted(by_ch.items()):
    print(f'  {ch}: {d["with_agent"]}/{d["total"]} have assigned_agent')

aa = sb.table('ai_analysis').select('lead_id,sentiment,treatment_score,source').execute().data or []
nonzero = [a for a in aa if (a.get('treatment_score') or 0) > 0]
print(f'\n=== ai_analysis ===')
print(f'  Total: {len(aa)}  With score>0: {len(nonzero)}')
print(f'  Sample: {[{"src":a["source"],"score":a["treatment_score"],"sent":a["sentiment"]} for a in nonzero[:5]]}')
