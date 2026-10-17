import ast
import hashlib
import re
import sys
from pathlib import Path

import yaml

def validate(path):
    flow=yaml.safe_load(path.read_text(encoding='utf-8'))
    assert flow['id']==path.stem
    metadata=flow['extend']
    assert set(metadata)=={'title','metaTitle','shortDescription','metaDescription','description','tags','subTags','ee','demo'}
    assert 1500<=len(metadata['description'])<=3000
    assert len(metadata['metaDescription'])<=160
    assert re.findall(r'^## (.+)$',metadata['description'],re.M)==[
        'How it works','What you get',"Who it's for",'Why orchestrate this with Kestra',
        'Prerequisites','Quick start','How to extend','Links']
    assert '### Secrets' in metadata['description']
    assert metadata['ee'] is False and metadata['demo'] is False
    assert metadata['tags']==['Data']
    assert set(metadata['subTags'])<={'Realtime & Streaming','ELT & Warehouse Loading','File & Data Processing Basics'}
    assert len(flow['tasks'])>=3
    assert flow['triggers'] and all(t.get('disabled') is True for t in flow['triggers'])
    assert any(t['type'].startswith('io.kestra.plugin.jdbc.') for t in flow['tasks'])
    assert any(t['type'] in ('io.kestra.plugin.core.flow.Loop','io.kestra.plugin.core.flow.If') for t in flow['tasks'])
    for t in flow['tasks']:
        if t.get('script'): ast.parse(t['script'])
    print(path.name,'PASS',hashlib.sha256(path.read_bytes().replace(b'\r\n',b'\n')).hexdigest())

if __name__=='__main__':
    for argument in sys.argv[1:]: validate(Path(argument))
