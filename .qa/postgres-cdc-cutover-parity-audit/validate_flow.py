import ast
import hashlib
import re

from test_comparator import FLOW, flow, script

assert FLOW.stem == flow['id']
assert len(flow['tasks']) >= 3
metadata = flow['extend']
assert set(metadata) == {'title','metaTitle','shortDescription','metaDescription',
                         'description','tags','subTags','ee','demo'}
assert len(metadata['metaDescription']) <= 160
assert 1500 <= len(metadata['description']) <= 3000
headings = re.findall(r'^## (.+)$', metadata['description'], re.M)
assert headings == ['How it works','What you get',"Who it's for",
    'Why orchestrate this with Kestra','Prerequisites','Quick start','How to extend','Links']
assert metadata['tags'] == ['Data']
assert set(metadata['subTags']) <= {'Testing & Quality','Day 2 Operations'}
assert metadata['ee'] is False and metadata['demo'] is False
assert flow['triggers'] and all(t['disabled'] is True for t in flow['triggers'])
assert any(t['type'] == 'io.kestra.plugin.core.flow.If' for t in flow['tasks'])
assert any(t['type'].startswith('io.kestra.plugin.jdbc.') for t in flow['tasks'])
assert all('type' in t and 'id' in t for t in flow['tasks'])
assert len(set(re.findall(r"secret\('([^']+)'\)", FLOW.read_text()))) == 8
compile(script, str(FLOW), 'exec')
print('YAML, embedded Python syntax, catalog metadata, taxonomy, structure, and secrets: PASS')
print('YAML SHA256:',hashlib.sha256(FLOW.read_bytes()).hexdigest())
print('YAML LF SHA256:',hashlib.sha256(FLOW.read_bytes().replace(b'\r\n',b'\n')).hexdigest())
print('Embedded Python SHA256:',hashlib.sha256(script.encode()).hexdigest())
