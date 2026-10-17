import base64
import json
import time
import urllib.parse
from pathlib import Path

import requests
import yaml
from setup_runtime import docker, sql

ROOT=Path(__file__).parent
FLOW_PATH=next(p for parent in ROOT.parents for p in (
    parent/'flows/kafka-compacted-postgres-state.yaml',
    parent/'kafka-blueprint/flows/kafka-compacted-postgres-state.yaml') if p.exists())
FLOW=yaml.safe_load(FLOW_PATH.read_text())
FLOW.pop('extend')
BASE='http://localhost:18081/api/v1/main'
SESSION=requests.Session()
SESSION.auth=('admin@kestra.local','Kestra-local-1')

def api(path,method='GET',**kwargs):
    response=SESSION.request(method,BASE+path,timeout=40,**kwargs)
    if not response.ok:
        raise RuntimeError(f'{method} {path}: {response.status_code} {response.text[:2500]}')
    return response.json() if response.content else None

def deploy():
    data=yaml.safe_dump(FLOW,sort_keys=False)
    (ROOT/'deployed-flow.yaml').write_text(data)
    response=SESSION.put(BASE+'/flows/'+FLOW['namespace']+'/'+FLOW['id'],
        data=data,headers={'Content-Type':'application/x-yaml'},timeout=40)
    if response.status_code==404:
        response=SESSION.post(BASE+'/flows',data=data,headers={'Content-Type':'application/x-yaml'},timeout=40)
    assert response.ok, response.text
    (ROOT/'deployment.json').write_text(json.dumps(response.json(),indent=2))
    print('Flow accepted by Kestra',flush=True)

def execute(label,topic,expected,limit=1000,consumed=None,live=None):
    data={'topic':topic,'max_records':str(limit),'poll_seconds':'2','docker_network':'kafka-state-qa'}
    execution=api('/executions/'+FLOW['namespace']+'/'+FLOW['id'],'POST',
        files={k:(None,v) for k,v in data.items()})
    eid=execution['id']; deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        execution=api('/executions/'+eid)
        if execution['state']['current'] in ('SUCCESS','FAILED','WARNING','KILLED','CANCELLED'):
            break
        time.sleep(2)
    logs=api('/logs/'+eid)
    (ROOT/f'{label}-execution.json').write_text(json.dumps(execution,indent=2))
    (ROOT/f'{label}-logs.json').write_text(json.dumps(logs,indent=2))
    if execution['state']['current']!=expected:
        print(json.dumps(logs,indent=2)[-6000:],flush=True)
    assert execution['state']['current']==expected,(label,eid,execution['state'])
    outputs=None
    if expected=='SUCCESS':
        outputs=api('/outputs/executions/'+eid)
        (ROOT/f'{label}-outputs.json').write_text(json.dumps(outputs,indent=2))
        if consumed is not None:
            assert outputs['consumed']==consumed,outputs
        if live is not None:
            assert outputs['live_keys']==live,outputs
        report=api('/executions/'+eid+'/file?'+urllib.parse.urlencode({'path':outputs['batch_report']}))
        (ROOT/f'{label}-report.json').write_text(json.dumps(report,indent=2))
    result={'case':label,'execution':eid,'state':execution['state']['current'],'outputs':outputs}
    with (ROOT/'runtime-results.jsonl').open('a') as f:
        f.write(json.dumps(result)+'\n')
    print(json.dumps(result),flush=True)
    return outputs

def client(code):
    return docker('exec','kafka-state-client','python','-c',code)

def produce(topic,records):
    client('from sys import path; path.insert(0,"/qa"); from test_materializer import produce; produce('+repr(topic)+','+repr(records)+')')

def snap(topic):
    escaped=topic.replace("'","''")
    return sql("SELECT jsonb_build_object('values',(SELECT jsonb_agg(r ORDER BY record_key) FROM kafka_state.records r WHERE topic='"+escaped+"'),'positions',(SELECT next_offsets FROM kafka_state.positions WHERE topic='"+escaped+"'));")

if __name__=='__main__':
    deploy()
    topic=client('from sys import path; path.insert(0,"/qa"); from test_materializer import create_topic; print(create_topic())')
    (ROOT/'runtime-topic.txt').write_text(topic)
    produce(topic,[(0,'a','{"v":1}'),(0,'a','{"v":2}'),(1,'b','{}'),(0,'gone','{}'),(0,'gone',None),(1,'json-null','null')])
    execute('initial',topic,'SUCCESS',consumed=6,live=3)
    before=snap(topic)
    execute('idle-rerun',topic,'SUCCESS',consumed=0,live=3)
    assert snap(topic)==before
    produce(topic,[(0,'a',None),(1,'b','{"v":3}')])
    execute('update-delete',topic,'SUCCESS',consumed=2,live=2)
    before=snap(topic)
    produce(topic,[(0,'new','{}'),(0,'bad','invalid-json')])
    execute('poison-rollback',topic,'FAILED')
    assert snap(topic)==before,'Poison batch changed durable values or read positions'
    execute('poison-retry',topic,'FAILED')
    assert snap(topic)==before
    topic2=client('from sys import path; path.insert(0,"/qa"); from test_materializer import create_topic; print(create_topic(1))')
    produce(topic2,[(0,'same',str(i)) for i in range(3)])
    for n in range(3):
        execute(f'bounded-{n+1}',topic2,'SUCCESS',limit=1,consumed=1,live=1)
    execute('bounded-idle',topic2,'SUCCESS',limit=1,consumed=0,live=1)
    print('All Kestra runtime assertions passed',flush=True)
