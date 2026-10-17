"""Consolidate only completed, asserted cases and bind evidence to the source bytes."""
import hashlib
import json

import yaml
from run_runtime import ROOT, FLOW_PATH, FLOW

expected = {
    'unconfirmed-cutover':'FAILED','equal':'SUCCESS','missing':'FAILED',
    'extra':'FAILED','changed':'FAILED','tombstone':'SUCCESS',
    'empty-block':'FAILED','empty-allowed':'SUCCESS','invalid-metadata':'FAILED',
    'row-limit':'FAILED','invalid-limit':'FAILED','bounded-report':'FAILED',
    'duplicate-view-keys':'FAILED','permission-denied':'FAILED','recovered':'SUCCESS',
    'same-database':'FAILED','debezium-snapshot':'SUCCESS','debezium-mixed-dml':'SUCCESS',
}
latest = {}
for line in (ROOT/'runtime-results.jsonl').read_text().splitlines():
    case=json.loads(line)
    latest[case['case']]=case
results=[]
for name, state in expected.items():
    case=latest[name]
    execution=json.loads((ROOT/f'{name}-execution.json').read_text())
    assert case['state']==state==execution['state']['current']
    assert case['execution_id']==execution['id']
    if report:=case['report']:
        assert report['matched']+report['missing']+report['changed']==report['source_rows']
        assert report['matched']+report['extra']+report['changed']==report['replica_rows']
        assert report['missing']+report['extra']+report['changed']==report['difference_count']
    results.append(case)
assert yaml.safe_load((ROOT/'deployed-flow.yaml').read_text())==FLOW
script=next(t['script'] for t in FLOW['tasks'] if t['id']=='compare_rows')
assert 'Ran 15 tests' in (ROOT/'offline-tests.log').read_text()
assert '\nOK\n' in (ROOT/'offline-tests.log').read_text()
payload={'source_commit':'059ab9effa49bd1de20d817646abd58f7dea0000',
    'yaml_sha256':hashlib.sha256(FLOW_PATH.read_bytes()).hexdigest(),
    'yaml_lf_sha256':hashlib.sha256(FLOW_PATH.read_bytes().replace(b'\r\n',b'\n')).hexdigest(),
    'checkout_line_endings':'CRLF' if b'\r\n' in FLOW_PATH.read_bytes() else 'LF',
    'embedded_python_sha256':hashlib.sha256(script.encode()).hexdigest(),
    'runtime_cases':len(results),'offline_test_methods':15,'randomized_cases':500,
    'results':results}
(ROOT/'verified-results.json').write_text(json.dumps(payload,indent=2)+'\n')
print('Verified',len(results),'runtime cases; source and deployed flow match.')
