import json

from run_runtime import ROOT, api

executions=json.loads((ROOT/'cdc-executions.json').read_text())
rows=[]
for execution in executions:
    eid=execution['id']
    outputs=json.loads(api('/executions/'+eid+'/actions/eval','POST',
        b'{{ outputs.apply_changes.row | toJson }}','text/plain')['result'])
    rows.append({'execution_id':eid,'state':execution['state']['current'],**outputs})
assert sum(r['events'] for r in rows)>=15
assert sum(r['upserted'] for r in rows)>=14
assert sum(r['deleted'] for r in rows)>=1
(ROOT/'cdc-applied-events.json').write_text(json.dumps(rows,indent=2)+'\n')
print('CDC events verified:',sum(r['events'] for r in rows),
      'upserts:',sum(r['upserted'] for r in rows),
      'deletes:',sum(r['deleted'] for r in rows))
