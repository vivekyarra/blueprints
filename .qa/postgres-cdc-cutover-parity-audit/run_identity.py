"""Inject a misconfigured replica DSN; use the submitted task script unchanged."""
import copy
import json

from run_runtime import FLOW, ROOT, api, deploy, execute

bad = copy.deepcopy(FLOW)
next(t for t in bad['tasks'] if t['id']=='compare_rows')['env']['REPLICA_DSN'] = "{{ secret('CDC_SOURCE_DSN') }}"
try:
    deploy(bad)
    result = execute('same-database', 'FAILED', failed_task='compare_rows')
    logs = api('/logs/'+result['execution_id'])
    assert any('source and replica must be separate databases' in l.get('message','') for l in logs)
    print('Same database DSN rejected by the actual embedded script.')
finally:
    deploy()
