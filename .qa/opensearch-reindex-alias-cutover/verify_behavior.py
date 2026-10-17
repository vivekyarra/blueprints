import json
from pathlib import Path
import time

import requests

from run_runtime import ROOT, BASE, FLOW, api, search, fixture, execute, alias_state, all_docs

source,alias,docs=fixture()
before=alias_state(alias)
execute('reject-index-selector',source+'*',alias,'FAILED',failed_task='validate_names')
execute('reject-alias-path',source,alias+'/path','FAILED',failed_task='validate_names')
assert alias_state(alias)==before and all_docs(source)==docs

source,alias,docs=fixture(count=2000)
foreign=source+'-foreign'
search('/'+foreign,'PUT',json={'settings':{'number_of_replicas':0}})
errors=[]; changed=[]
def move_alias(candidate):
    try:
        deadline=time.monotonic()+120
        while requests.head('http://localhost:19200/'+candidate,timeout=10).status_code!=200:
            assert time.monotonic()<deadline
            time.sleep(.05)
        response=search('/_aliases','POST',json={'actions':[
            {'remove':{'index':source,'alias':alias,'must_exist':True}},
            {'add':{'index':foreign,'alias':alias}}]})
        changed.append(response)
    except Exception as error:
        errors.append(str(error))

execute('alias-changed-during-copy',source,alias,'FAILED',watcher=move_alias)
race=json.loads((ROOT/'alias-changed-during-copy-execution.json').read_text())
assert any(t['taskId'] in ('require_same_source','promote_candidate') and t['state']['current']=='FAILED' for t in race['taskRunList'])
assert not errors and changed,(errors,changed)
assert list(alias_state(alias))==[foreign]
assert all_docs(source)==docs
(ROOT/'external-alias-change.json').write_text(json.dumps({'response':changed,'final_alias':alias_state(alias)},indent=2))

# Exercise the server's atomic failure behavior, rather than assuming HTTP 200 is required.
before=alias_state(alias)
response=search('/_aliases','POST',expected=404,json={'actions':[
    {'remove':{'index':source,'alias':alias,'must_exist':True}},
    {'add':{'index':source,'alias':alias,'is_write_index':False}}]})
assert alias_state(alias)==before
(ROOT/'atomic-alias-failure.json').write_text(json.dumps({'error':response,'alias_unchanged':True},indent=2))

# A real webhook invocation uses the blueprint's default inputs and replica count.
default_docs={str(i):{'sku':'sku-'+str(i),'title':'Wireless mouse '+str(i),'price':i+.25} for i in range(3)}
if requests.head('http://localhost:19200/catalog-v1',timeout=10).status_code==404:
    search('/catalog-v1','PUT',json={'settings':{'number_of_shards':2,'number_of_replicas':0},
        'mappings':{'properties':{'sku':{'type':'keyword'},'title':{'type':'text'},'price':{'type':'double'}}}})
    lines=[]
    for key,doc in default_docs.items():
        lines.extend([json.dumps({'index':{'_index':'catalog-v1','_id':key}}),json.dumps(doc)])
    assert search('/_bulk?refresh=true','POST',data='\n'.join(lines)+'\n',headers={'Content-Type':'application/x-ndjson'})['errors'] is False
    search('/catalog-v1/_alias/catalog','PUT',json={})
    search('/catalog-v1/_settings','PUT',json={'index.blocks.write':True})
else:
    assert all_docs('catalog-v1')==default_docs and list(alias_state('catalog'))==['catalog-v1'], 'Unexpected existing default fixture'
webhook=BASE+'/executions/webhook/'+FLOW['namespace']+'/'+FLOW['id']+'/fixture_reindex_approval'
for label,expected,task in [('webhook-cutover','SUCCESS',None),('webhook-replay','FAILED','require_single_plain_alias')]:
    response=requests.post(webhook,json={},timeout=40)
    assert response.ok,response.text
    payload=response.json()
    (ROOT/f'{label}-response.json').write_text(json.dumps(payload,indent=2))
    candidate,outputs=execute(label,'catalog-v1','catalog',expected,failed_task=task,execution_id=payload['id'])
    if expected=='SUCCESS':
        promoted=candidate
        assert all_docs(candidate)==default_docs
        assert outputs['copied_documents']==3
    else:
        assert list(alias_state('catalog'))==[promoted]
print('Name guards, external alias change, atomic API failure, webhook and replay verified',flush=True)
