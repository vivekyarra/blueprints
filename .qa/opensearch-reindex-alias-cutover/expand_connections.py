from pathlib import Path
import re

p=Path(__file__).parent.parent/'search-blueprint/flows/opensearch-reindex-alias-cutover.yaml'
text=p.read_text()
if 'pluginDefaults:\n' in text:
    text=text.split('pluginDefaults:\n')[0]+'tasks:\n'+text.split('\ntasks:\n',1)[1]
def connection(match):
    indent=match[1]
    return match[0]+'\n'+('\n'.join(indent+line for line in [
        'connection:','  hosts:',"    - \"{{ secret('OPENSEARCH_URL') }}\"",'  basicAuth:',
        "    username: \"{{ secret('OPENSEARCH_USER') }}\"",
        "    password: \"{{ secret('OPENSEARCH_PASSWORD') }}\"" ]))
text=re.sub(r'(?m)^( *)type: io\.kestra\.plugin\.opensearch\.Request$',connection,text)
p.write_text(text,newline='\n')
