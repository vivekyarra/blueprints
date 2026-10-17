import hashlib
import json
from pathlib import Path
import threading
import time
import uuid

import requests
import yaml

ROOT=Path(__file__).parent
FLOW_PATH=next(p for parent in ROOT.parents for p in (
    parent/'flows/opensearch-reindex-alias-cutover.yaml',
    parent/'search-blueprint/flows/opensearch-reindex-alias-cutover.yaml') if p.exists())
FLOW=yaml.safe_load(FLOW_PATH.read_text()); FLOW.pop('extend')
BASE='http://localhost:18083/api/v1/main'
SEARCH='http://localhost:19200'
SESSION=requests.Session(); SESSION.auth=('admin@kestra.local','Kestra-local-1')

def api(path,method='GET',**kwargs):
    r=SESSION.request(method,BASE+path,timeout=40,**kwargs)
    assert r.ok,(path,r.status_code,r.text[:2500])
    return r.json() if r.content else None

def search(path,method='GET',expected=200,**kwargs):
    r=requests.request(method,SEARCH+path,timeout=60,**kwargs)
    assert r.status_code==expected,(method,path,r.status_code,r.text[:2500])
    return r.json()

def deploy():
    data=yaml.safe_dump(FLOW,sort_keys=False)
    (ROOT/'deployed-flow.yaml').write_text(data)
    r=SESSION.put(BASE+'/flows/'+FLOW['namespace']+'/'+FLOW['id'],data=data,
        headers={'Content-Type':'application/x-yaml'},timeout=40)
    if r.status_code==404:
        r=SESSION.post(BASE+'/flows',data=data,headers={'Content-Type':'application/x-yaml'},timeout=40)
    assert r.ok,r.text
    print('Flow accepted by Kestra',flush=True)

def fixture(count=200,frozen=True,alias_mode='plain'):
    stem='qa-search-'+uuid.uuid4().hex[:10]
    source=stem+'-v1'; alias=stem+'-live'
    search('/'+source,'PUT',json={'settings':{'number_of_shards':2,'number_of_replicas':0},
        'mappings':{'properties':{'sku':{'type':'keyword'},'title':{'type':'text'},'price':{'type':'double'}}}})
    docs={str(i):{'sku':'sku-'+str(i),'title':('Wireless mouse 東京 ' if i%2==0 else 'Keyboard Grüße ')+str(i),
                  'price':round(i*.25,2)} for i in range(count)}
    for start in range(0,count,1000):
        lines=[]
        for i in range(start,min(count,start+1000)):
            lines += [json.dumps({'index':{'_index':source,'_id':str(i)}}),json.dumps(docs[str(i)])]
        response=search('/_bulk?refresh=true','POST',data='\n'.join(lines)+'\n',
            headers={'Content-Type':'application/x-ndjson'})
        assert response['errors'] is False
    options={'filter':{'term':{'sku':'sku-0'}}} if alias_mode=='filtered' else {}
    search('/'+source+'/_alias/'+alias,'PUT',json=options)
    if alias_mode=='multiple':
        other=source+'-other'
        search('/'+other,'PUT',json={'settings':{'number_of_replicas':0}})
        search('/'+other+'/_alias/'+alias,'PUT',json={})
    if frozen:
        search('/'+source+'/_settings','PUT',json={'index.blocks.write':True})
    return source,alias,docs

def alias_state(alias):
    return search('/_alias/'+alias)

def all_docs(index):
    response=search('/'+index+'/_search?scroll=1m','POST',json={'size':1000,'sort':['_doc'],'query':{'match_all':{}}})
    result={}
    scroll=response.get('_scroll_id')
    try:
        while response['hits']['hits']:
            result.update({d['_id']:d['_source'] for d in response['hits']['hits']})
            response=search('/_search/scroll','POST',json={'scroll':'1m','scroll_id':scroll})
            scroll=response.get('_scroll_id',scroll)
    finally:
        if scroll: search('/_search/scroll','DELETE',json={'scroll_id':[scroll]})
    return result

def execute(label,source,alias,expected,inputs=None,failed_task=None,watcher=None,execution_id=None):
    values={'source_index':source,'read_alias':alias,'replicas':0,**(inputs or {})}
    execution=api('/executions/'+execution_id) if execution_id else api(
        '/executions/'+FLOW['namespace']+'/'+FLOW['id'],'POST',files={
        k:(None,json.dumps(v) if isinstance(v,(dict,list)) else str(v)) for k,v in values.items()})
    eid=execution['id']; candidate=alias+'-kestra-'+eid.lower()
    thread=None
    if watcher:
        thread=threading.Thread(target=watcher,args=(candidate,),daemon=True); thread.start()
    deadline=time.monotonic()+300
    while time.monotonic()<deadline:
        execution=api('/executions/'+eid)
        if execution['state']['current'] in ('SUCCESS','FAILED','WARNING','KILLED'): break
        time.sleep(.5)
    if thread: thread.join(timeout=30); assert not thread.is_alive()
    logs=api('/logs/'+eid)
    (ROOT/f'{label}-execution.json').write_text(json.dumps(execution,indent=2))
    (ROOT/f'{label}-logs.json').write_text(json.dumps(logs,indent=2))
    if execution['state']['current']!=expected:
        print([(r.get('taskId'),r['message'][:1500]) for r in logs if r['level']=='ERROR'],flush=True)
    assert execution['state']['current']==expected,(label,eid,execution['state'])
    if failed_task:
        assert any(t['taskId']==failed_task and t['state']['current']=='FAILED' for t in execution['taskRunList'])
    outputs=None
    if expected=='SUCCESS':
        outputs=api('/outputs/executions/'+eid)
        assert outputs['new_index']==candidate,outputs
        assert list(alias_state(alias))==[candidate]
        assert alias_state(alias)[candidate]['aliases'][alias]['is_write_index'] is False
        (ROOT/f'{label}-outputs.json').write_text(json.dumps(outputs,indent=2))
    result={'case':label,'execution':eid,'state':execution['state']['current'],'outputs':outputs}
    with (ROOT/'runtime-results.jsonl').open('a') as f: f.write(json.dumps(result)+'\n')
    print(json.dumps(result),flush=True)
    return candidate,outputs

if __name__=='__main__':
    deploy()
    source,alias,docs=fixture()
    candidate,outputs=execute('copy-and-cutover',source,alias,'SUCCESS')
    assert outputs['copied_documents']==len(docs)
    assert all_docs(source)==all_docs(candidate)==docs
    assert search('/'+candidate+'/_settings?flat_settings=true')[candidate]['settings']['index.blocks.write']=='true'
    search('/'+candidate+'/_doc/rejected','PUT',expected=403,json={'sku':'x','title':'x','price':1})
    # The retained old index can be selected again with an atomic alias operation.
    search('/_aliases','POST',json={'actions':[
        {'remove':{'index':candidate,'alias':alias,'must_exist':True}},
        {'add':{'index':source,'alias':alias,'is_write_index':False}}]})
    assert list(alias_state(alias))==[source]
    assert all_docs(alias)==docs
    print('Verified full document equality, write blocking and rollback',flush=True)

    for label,kwargs,overrides,task in [
        ('source-not-frozen',{'frozen':False},{},'require_frozen_source'),
        ('empty-source',{'count':0},{},'require_nonempty_source'),
        ('filtered-alias',{'alias_mode':'filtered'},{},'require_single_plain_alias'),
        ('multiple-targets',{'alias_mode':'multiple'},{},'require_single_plain_alias'),
        ('canary-no-matches',{}, {'canary_query':{'term':{'sku':'absent'}}},'require_searchable_copy'),
        ('canary-too-few',{}, {'canary_min_hits':999999},'require_searchable_copy'),
        ('incompatible-mapping',{}, {'mappings':{'dynamic':'strict','properties':{'sku':{'type':'keyword'}}}},'require_complete_copy'),
    ]:
        source,alias,docs=fixture(**kwargs); before=alias_state(alias)
        execute(label,source,alias,'FAILED',inputs=overrides,failed_task=task)
        assert alias_state(alias)==before
        assert all_docs(source)==docs

    source,alias,docs=fixture(count=10003)
    candidate,outputs=execute('large-mapping-migration',source,alias,'SUCCESS')
    assert all_docs(source)==all_docs(candidate)==docs
    assert outputs['copied_documents']==10003
    (ROOT/'large-copy-hash.json').write_text(json.dumps({'documents':len(docs),
        'sha256':hashlib.sha256(json.dumps(docs,sort_keys=True).encode()).hexdigest()},indent=2))
    print('All OpenSearch runtime assertions passed',flush=True)
