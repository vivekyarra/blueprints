"""Feed the replica through the merged Debezium blueprint, then audit real CDC output."""
import json
import subprocess
import time

import yaml
from run_runtime import ROOT, FLOW_PATH, api, deploy, reset, execute
from setup_runtime import docker, sql

def wait_replica(expected_count, expected_plan=None):
    deadline = time.monotonic()+240
    while time.monotonic()<deadline:
        count=sql('parity-replica','SELECT count(*) FROM cdc.customers_live;', 'parity')
        plan=sql('parity-replica','SELECT plan FROM cdc.customers_live WHERE id=1;', 'parity')
        if count == str(expected_count) and (expected_plan is None or plan==expected_plan):
            return
        time.sleep(3)
    raise AssertionError('CDC did not converge before the deadline')

def main():
    reset()
    sql('parity-replica','TRUNCATE cdc.customers; ALTER TABLE cdc.customers ADD COLUMN IF NOT EXISTS _cdc_applied_at timestamptz NOT NULL DEFAULT now();','parity')
    sql('parity-source', "ALTER SYSTEM SET wal_level='logical';")
    docker('restart','parity-source')
    for _ in range(30):
        try:
            docker('exec','parity-source','pg_isready','-U','postgres')
            break
        except subprocess.CalledProcessError:
            pass
        time.sleep(1)
    host=json.loads(docker('inspect','parity-source'))[0]['NetworkSettings']['Networks']['bridge']['IPAddress']
    flow=yaml.safe_load((FLOW_PATH.parent/'debezium-postgres-cdc-replica-merge.yaml').read_text())
    flow.pop('extend')
    flow.pop('errors')  # QA uses no external Slack service.
    flow['id']='parity-cdc-fixture'
    for task in flow['tasks']:
        if task['type'].startswith('io.kestra.plugin.jdbc.'):
            task['username']='postgres'
            task['password']='parity_fixture_admin'
        if task['id']=='stage_changes':
            task['from']="{{ trigger.uris['parity.customers'] }}"
    trigger=flow['triggers'][0]
    trigger.update(hostname=host, username='postgres', password='parity_fixture_admin',
        database='parity',slotName='parity_audit_test',publicationName='parity_audit_test',interval='PT5S')
    (ROOT/'cdc-fixture.yaml').write_text(yaml.safe_dump(flow,sort_keys=False))
    deploy(flow)
    wait_replica(12)
    execute('debezium-snapshot', 'SUCCESS', {'status':'PASS','source_rows':12,'replica_rows':12,'difference_count':0})
    sql('parity-source', """UPDATE public.customers SET plan='enterprise', updated_at=now() WHERE id=1;
      DELETE FROM public.customers WHERE id=3;
      INSERT INTO public.customers VALUES(1000,'cdc@example.test','CDC insert','free',NULL,now());""", 'parity')
    wait_replica(12,'enterprise')
    # A matching row count alone is insufficient: wait for the newly inserted key as well.
    deadline=time.monotonic()+120
    while sql('parity-replica','SELECT count(*) FROM cdc.customers_live WHERE id=1000;','parity')!='1':
        assert time.monotonic()<deadline
        time.sleep(2)
    trigger['disabled']=True
    deploy(flow)  # Freeze capture after the source writes have stopped and the batch drained.
    execute('debezium-mixed-dml', 'SUCCESS', {'status':'PASS','source_rows':12,'replica_rows':12,'difference_count':0})
    executions=api('/executions/search?namespace=company.team&size=200')['results']
    events=[]
    for e in executions:
        if e['flowId']==flow['id']:
            detail=api('/executions/'+e['id'])
            events.append(detail)
    (ROOT/'cdc-executions.json').write_text(json.dumps(events,indent=2))
    assert len(events)>=2 and all(e['state']['current']=='SUCCESS' for e in events)
    print('Real Debezium snapshot and INSERT/UPDATE/DELETE audits passed.',flush=True)

if __name__=='__main__':
    main()
