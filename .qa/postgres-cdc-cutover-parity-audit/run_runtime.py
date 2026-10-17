"""Run the real catalog flow in Kestra and assert persisted outputs and failure paths."""
import base64
import copy
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import yaml
from setup_runtime import sql, docker

ROOT = Path(__file__).parent
FLOW_PATH = next(p for parent in ROOT.parents for p in
    (parent / 'flows/postgres-cdc-cutover-parity-audit.yaml',
     parent / 'blueprints/flows/postgres-cdc-cutover-parity-audit.yaml') if p.exists())
FLOW = yaml.safe_load(FLOW_PATH.read_text())
FLOW.pop('extend')  # Catalog metadata is not part of Kestra's runtime flow schema.
AUTH = 'Basic '+base64.b64encode(b'admin@kestra.local:Kestra-local-1').decode()
BASE = 'http://localhost:18080/api/v1/main'

def api(path, method='GET', data=None, content='application/json'):
    req = urllib.request.Request(BASE+path, data=data, method=method,
        headers={'Authorization': AUTH, 'Content-Type': content})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'{method} {path}: {e.code} {e.read().decode()[:3000]}') from e

def deploy(flow=FLOW):
    body = yaml.safe_dump(flow, sort_keys=False).encode()
    if flow is FLOW:
        (ROOT/'deployed-flow.yaml').write_bytes(body)
    # PUT creates/replaces this isolated QA flow, preserving the source task definitions.
    try:
        return api('/flows/'+flow['namespace']+'/'+flow['id'], 'PUT', body, 'application/x-yaml')
    except RuntimeError as e:
        if ': 404 ' not in str(e):
            raise
        return api('/flows', 'POST', body, 'application/x-yaml')

def reset(n=12):
    sql('parity-source', 'TRUNCATE public.customers;', 'parity')
    sql('parity-replica', 'TRUNCATE cdc.customers;', 'parity')
    common = f'''SELECT i, 'customer-'||i||'@example.test', 'Grüße 東京 '||i,
        CASE WHEN i%2=0 THEN 'pro' ELSE 'free' END,
        CASE WHEN i%3=0 THEN NULL ELSE repeat('private-notes-',1000) END,
        '2026-10-17 10:00:00+00'::timestamptz + i * interval '1 second'
        FROM generate_series(1,{n}) i'''
    sql('parity-source', 'INSERT INTO public.customers '+common+';', 'parity')
    sql('parity-replica', 'INSERT INTO cdc.customers '+common.replace('FROM generate_series',
        ', 12345::bigint, false FROM generate_series')+';', 'parity')

def execute(label, expected_state, expected=None, inputs=None, failed_task=None):
    boundary = 'parity-qa-boundary'
    payload = b''
    for key, value in {'cutover_ready':'true', **(inputs or {})}.items():
        payload += (f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n').encode()
    payload += f'--{boundary}--\r\n'.encode()
    execution = api('/executions/'+FLOW['namespace']+'/'+FLOW['id'], 'POST', payload,
                    'multipart/form-data; boundary='+boundary)
    eid = execution['id']
    deadline = time.monotonic()+300
    while time.monotonic() < deadline:
        execution = api('/executions/'+eid)
        if execution['state']['current'] in ('SUCCESS','FAILED','WARNING','KILLED','CANCELLED'):
            break
        time.sleep(2)
    (ROOT / f'{label}-execution.json').write_text(json.dumps(execution, indent=2))
    logs = api('/logs/'+eid)
    (ROOT / f'{label}-logs.json').write_text(json.dumps(logs, indent=2))
    state = execution['state']['current']
    if state != expected_state:
        print(json.dumps(logs, indent=2)[-7000:], flush=True)
    assert state == expected_state, (label, eid, state, expected_state)
    if failed_task:
        assert any(t['taskId'] == failed_task and t['state']['current']=='FAILED'
                   for t in execution['taskRunList']), (label, failed_task)
    report = None
    if expected is not None:
        evaluated = api('/executions/'+eid+'/actions/eval', 'POST',
                        b'{{ outputs.compare_rows | toJson }}', 'text/plain')
        outputs = json.loads(evaluated['result'])
        (ROOT/f'{label}-outputs.json').write_text(json.dumps(outputs, indent=2))
        uri = outputs['outputFiles']['cdc-parity-report.json']
        report = api('/executions/'+eid+'/file?'+urllib.parse.urlencode({'path':uri}))
        (ROOT/f'{label}-report.json').write_text(json.dumps(report, indent=2))
        for key, value in expected.items():
            assert report[key] == value, (label, key, report[key], value)
        assert len(report['examples']) <= int((inputs or {}).get('max_examples',20))
        encoded = json.dumps(report)
        assert 'private-notes' not in encoded and '@example.test' not in encoded
        assert len(encoded) < 50000
    result = {'case':label,'execution_id':eid,'state':state,
              'report':report,'failed_task':failed_task}
    print(label, eid, state, report and report['status'], flush=True)
    with (ROOT/'runtime-results.jsonl').open('a') as f:
        f.write(json.dumps(result)+'\n')
    return result

def main():
    deploy()
    execute('unconfirmed-cutover', 'FAILED', inputs={'cutover_ready':'false'}, failed_task='stop_unconfirmed_cutover')
    reset()
    execute('equal', 'SUCCESS', {'status':'PASS','source_rows':12,'replica_rows':12,'matched':12,'difference_count':0})
    sql('parity-replica', 'DELETE FROM cdc.customers WHERE id=3;', 'parity')
    execute('missing', 'FAILED', {'status':'BLOCK','missing':1,'extra':0,'changed':0}, failed_task='block_cutover')
    reset()
    sql('parity-replica', "INSERT INTO cdc.customers VALUES(999,'private@example.test','extra','pro',NULL,now(),12345,false);", 'parity')
    execute('extra', 'FAILED', {'status':'BLOCK','missing':0,'extra':1,'changed':0}, failed_task='block_cutover')
    reset()
    sql('parity-replica', "UPDATE cdc.customers SET notes='changed-private-notes', email=NULL WHERE id=4;", 'parity')
    execute('changed', 'FAILED', {'status':'BLOCK','missing':0,'extra':0,'changed':1}, failed_task='block_cutover')
    reset()
    sql('parity-replica', "INSERT INTO cdc.customers VALUES(999,'deleted@example.test','tombstone','pro',NULL,now(),12345,true);", 'parity')
    execute('tombstone', 'SUCCESS', {'status':'PASS','matched':12,'difference_count':0})
    reset(0)
    execute('empty-block', 'FAILED', {'status':'BLOCK','reason':'EMPTY_DATASET'}, failed_task='block_cutover')
    execute('empty-allowed', 'SUCCESS', {'status':'PASS','source_rows':0,'replica_rows':0}, inputs={'allow_empty':'true'})
    reset()
    sql('parity-replica','UPDATE cdc.customers SET _cdc_deleted=NULL WHERE id=1;', 'parity')
    execute('invalid-metadata', 'FAILED', failed_task='stop_unsafe_scan')
    reset()
    execute('row-limit', 'FAILED', inputs={'max_rows':'10'}, failed_task='stop_unsafe_scan')
    execute('invalid-limit', 'FAILED', inputs={'max_rows':'0'}, failed_task='stop_unconfirmed_cutover')
    reset(10000)
    sql('parity-replica', 'UPDATE cdc.customers SET full_name=NULL;', 'parity')
    execute('bounded-report', 'FAILED', {'status':'BLOCK','changed':10000,'examples_truncated':True},
            inputs={'max_examples':'3'}, failed_task='block_cutover')
    reset()
    sql('parity-replica', '''CREATE OR REPLACE VIEW cdc.customers_live AS
      SELECT id,email,full_name,plan,notes,updated_at FROM cdc.customers WHERE NOT _cdc_deleted
      UNION ALL SELECT id,email,full_name,plan,notes,updated_at FROM cdc.customers WHERE id=1;''', 'parity')
    execute('duplicate-view-keys', 'FAILED', failed_task='compare_rows')
    sql('parity-replica', '''CREATE OR REPLACE VIEW cdc.customers_live AS
      SELECT id,email,full_name,plan,notes,updated_at FROM cdc.customers WHERE NOT _cdc_deleted;''', 'parity')
    sql('parity-replica', 'REVOKE SELECT ON cdc.customers_live FROM parity_reader;', 'parity')
    execute('permission-denied', 'FAILED', failed_task='compare_rows')
    sql('parity-replica', 'GRANT SELECT ON cdc.customers_live TO parity_reader;', 'parity')
    execute('recovered', 'SUCCESS', {'status':'PASS','matched':12,'difference_count':0})
    print('All runtime assertions passed. YAML SHA256:', hashlib.sha256(FLOW_PATH.read_bytes()).hexdigest())

if __name__ == '__main__':
    main()
