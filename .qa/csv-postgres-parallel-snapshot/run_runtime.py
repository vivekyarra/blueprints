import csv
import io
import re
import json
import sys
import subprocess
import time
from pathlib import Path

import requests
import yaml

ROOT=Path(__file__).parent
def sql(statement):
    r=subprocess.run(['docker','exec','-i','kafka-state-postgres','psql','-U','postgres',
        '-v','ON_ERROR_STOP=1','-tA'],input=statement,text=True,encoding='utf-8',capture_output=True,check=True)
    return r.stdout.strip()

FLOW_PATH=next(p for parent in ROOT.parents for p in (
    parent/'flows/csv-postgres-parallel-snapshot.yaml',
    parent/'csv-blueprint/flows/csv-postgres-parallel-snapshot.yaml') if p.exists())
FLOW=yaml.safe_load(FLOW_PATH.read_text()); FLOW.pop('extend')
BASE='http://localhost:18082/api/v1/main'
SESSION=requests.Session(); SESSION.auth=('admin@kestra.local','Kestra-local-1')
HEADER=['order_id','customer_name','customer_email','product_id','price','quantity','total']

def api(path,method='GET',**kwargs):
    r=SESSION.request(method,BASE+path,timeout=40,**kwargs)
    assert r.ok,(path,r.status_code,r.text[:2500])
    return r.json() if r.content else None

def deploy():
    data=yaml.safe_dump(FLOW,sort_keys=False)
    (ROOT/'deployed-flow.yaml').write_text(data)
    r=SESSION.put(BASE+'/flows/'+FLOW['namespace']+'/'+FLOW['id'],data=data,
        headers={'Content-Type':'application/x-yaml'},timeout=40)
    if r.status_code==404:
        r=SESSION.post(BASE+'/flows',data=data,headers={'Content-Type':'application/x-yaml'},timeout=40)
    assert r.ok,r.text
    print('Flow accepted by Kestra',flush=True)

def fixture(name,rows):
    p=ROOT/'fixtures'/name
    p.parent.mkdir(exist_ok=True)
    with p.open('w',encoding='utf-8',newline='') as f:
        writer=csv.writer(f); writer.writerow(HEADER); writer.writerows(rows)
    return 'http://csv-snapshot-files:8000/'+name

def snapshot():
    return sql("SELECT md5(COALESCE(jsonb_agg(t ORDER BY order_id)::text,'')) FROM csv_snapshot.orders t;")

def execute(label,source,expected,rows=None,chunk=3,allow_empty=False,failed_task=None):
    execution=api('/executions/'+FLOW['namespace']+'/'+FLOW['id'],'POST',files={
        k:(None,str(v).lower() if isinstance(v,bool) else str(v)) for k,v in
        {'source_url':source,'rows_per_chunk':chunk,'allow_empty':allow_empty}.items()})
    eid=execution['id']; deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        execution=api('/executions/'+eid)
        if execution['state']['current'] in ('SUCCESS','FAILED','WARNING','KILLED'):
            break
        time.sleep(1)
    logs=api('/logs/'+eid)
    (ROOT/f'{label}-execution.json').write_text(json.dumps(execution,indent=2))
    (ROOT/f'{label}-logs.json').write_text(json.dumps(logs,indent=2))
    for child in set(re.findall(r'execution="([^\"]+)"',json.dumps(logs).replace('\\"','"'))):
        child_logs=api('/logs/'+child)
        (ROOT/f'{label}-child-{child}-logs.json').write_text(json.dumps(child_logs,indent=2))
    if execution['state']['current']!=expected:
        print(json.dumps(logs,indent=2)[-6000:],flush=True)
    assert execution['state']['current']==expected,(label,eid,execution['state'])
    if failed_task:
        assert any(t['taskId']==failed_task and t['state']['current']=='FAILED' for t in execution['taskRunList'])
    outputs=None
    if expected=='SUCCESS':
        outputs=api('/outputs/executions/'+eid)
        assert int(outputs['published_rows'])==rows,outputs
        assert int(sql('SELECT count(*) FROM csv_snapshot.orders;'))==rows
        (ROOT/f'{label}-outputs.json').write_text(json.dumps(outputs,indent=2))
    result={'case':label,'execution':eid,'state':execution['state']['current'],'outputs':outputs}
    with (ROOT/'runtime-results.jsonl').open('a') as f: f.write(json.dumps(result)+'\n')
    print(json.dumps(result),flush=True)
    return execution,outputs

if __name__=='__main__':
    deploy()
    healthy=[[i, '東京, Customer\n'+str(i),'customer@example.test',5,'12.25',2,'24.50'] for i in range(1,12)]
    source=fixture('healthy.csv',healthy)
    execute('quoted-multiline',source,'SUCCESS',rows=11)
    names=json.loads(sql('SELECT jsonb_agg(customer_name ORDER BY order_id) FROM csv_snapshot.orders;'))
    assert names==[r[1] for r in healthy],'Quoted commas, Unicode or multiline values changed'
    before=snapshot()
    execute('repeat-same-source',source,'SUCCESS',rows=11)
    assert snapshot()==before
    sql('ALTER TABLE csv_snapshot.orders ADD CONSTRAINT qa_reject CHECK (order_id <> 99999);')
    try:
        bad=fixture('publish-error.csv',[[99999,'Rejected','no@example.test',1,1,1,1]])
        execute('publish-rollback',bad,'FAILED',failed_task='publish_snapshot')
        assert snapshot()==before,'Failed publication changed the last good data'
    finally:
        sql('ALTER TABLE csv_snapshot.orders DROP CONSTRAINT qa_reject;')
    duplicate=fixture('duplicate.csv',healthy+[healthy[0]])
    execute('duplicate-keys',duplicate,'FAILED',failed_task='validate_snapshot')
    assert snapshot()==before
    invalid=fixture('invalid-type.csv',healthy+[['bad-id','X','x@example.test',5,1,1,1]])
    execute('invalid-number',invalid,'FAILED',failed_task='stage_chunks')
    assert snapshot()==before
    negative=fixture('invalid-quantity.csv',[[8888,'X','x@example.test',5,1,-1,1]])
    execute('negative-quantity',negative,'FAILED',failed_task='stage_chunks')
    assert snapshot()==before
    empty=fixture('empty.csv',[])
    execute('empty-blocked',empty,'FAILED',failed_task='validate_snapshot')
    assert snapshot()==before
    execute('empty-approved',empty,'SUCCESS',rows=0,allow_empty=True)
    execute('restore-snapshot',source,'SUCCESS',rows=11,chunk=2)
    big=fixture('large.csv',[[i,'C '+str(i),'x@example.test',5,1,2,2] for i in range(1,20004)])
    execute('large-bounded-load',big,'SUCCESS',rows=20003,chunk=5000)
    assert sql("SELECT count(*) FROM csv_snapshot.orders WHERE customer_name <> 'C '||order_id OR price <> 1 OR quantity <> 2 OR total <> 2;")=='0'
    assert sql('SELECT min(order_id)||\',\'||max(order_id) FROM csv_snapshot.orders;')=='1,20003'
    execute('public-sample', 'https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv',
            'SUCCESS',rows=100,chunk=20)
    print('All snapshot runtime assertions passed',flush=True)
