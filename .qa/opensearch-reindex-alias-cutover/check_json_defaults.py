import json
from run_runtime import SESSION, BASE

cases=['3hCBjUL6TjBn1JU5oEwzAq','2AJ7p0n7qfOhOObq8UJdQ8']
results=[]
for eid in cases:
    for name in ('mappings','canary_query'):
        expression='{{ (inputs.'+name+' is map ? inputs.'+name+' : fromJson(inputs.'+name+')) | toJson }}'
        response=SESSION.post(BASE+'/executions/'+eid+'/actions/eval',data=expression,
                              headers={'Content-Type':'text/plain'},timeout=20)
        assert response.ok,response.text
        value=json.loads(response.json()['result'])
        assert isinstance(value,dict),(eid,name,value)
        results.append({'execution':eid,'input':name,'resolved_as_object':True})
print(json.dumps(results,indent=2))
