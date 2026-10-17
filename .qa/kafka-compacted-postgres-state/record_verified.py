"""Write provenance only after all expected runtime evidence has passed."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import yaml

root=Path(sys.argv[1]).resolve()
flow_path=Path(sys.argv[2]).resolve()
expected=json.loads(sys.argv[3])
source=yaml.safe_load(flow_path.read_text(encoding='utf-8')); source.pop('extend')
assert yaml.safe_load((root/'deployed-flow.yaml').read_text(encoding='utf-8'))==source
results=[json.loads(s) for s in (root/'runtime-results.jsonl').read_text().splitlines()]
by_name={r['case']:r for r in results}
assert set(by_name)==set(expected),(set(by_name),set(expected))
for name,state in expected.items():
    result=by_name[name]
    execution=json.loads((root/f'{name}-execution.json').read_text())
    assert execution['id']==result['execution']
    assert execution['state']['current']==state==result['state']
    assert (root/f'{name}-logs.json').exists()
    if state=='SUCCESS':
        assert json.loads((root/f'{name}-outputs.json').read_text())==result['outputs']
canonical=flow_path.read_bytes().replace(b'\r\n',b'\n')
blob=subprocess.check_output(['git','-C',str(flow_path.parent.parent),'rev-parse',
    'HEAD:flows/'+flow_path.name],text=True).strip()
commit=subprocess.check_output(['git','-C',str(flow_path.parent.parent),'rev-parse','HEAD'],text=True).strip()
report={'flow':flow_path.name,'source_commit':commit,'git_blob':blob,
        'source_lf_sha256':hashlib.sha256(canonical).hexdigest(),
        'expected_cases':len(expected),'verified_cases':[by_name[n] for n in expected],
        'deployed_task_definitions_match_source':True}
(root/'verified-results.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k!='verified_cases'}))
