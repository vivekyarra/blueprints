"""Create isolated disposable QA databases; never touches existing CDC containers."""
import base64
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent

def docker(*args, sql=None):
    return subprocess.run(['docker', *args], input=sql, text=True, encoding='utf-8',
                          capture_output=True, check=True).stdout.strip()

def sql(container, statement, db='postgres'):
    return docker('exec', '-i', container, 'psql', '-U', 'postgres', '-d', db,
                  '-v', 'ON_ERROR_STOP=1', '-tA', sql=statement)

if __name__ == '__main__':
    for name in ('parity-source', 'parity-replica'):
        sql(name, "CREATE DATABASE parity; CREATE ROLE parity_reader LOGIN PASSWORD 'parity_fixture_reader';")
    cols = 'id BIGINT PRIMARY KEY, email TEXT, full_name TEXT, plan TEXT, notes TEXT, updated_at TIMESTAMPTZ'
    sql('parity-source', f'CREATE TABLE public.customers ({cols});', 'parity')
    sql('parity-replica', f'''CREATE SCHEMA cdc;
        CREATE TABLE cdc.customers ({cols}, _cdc_lsn BIGINT, _cdc_deleted BOOLEAN);
        CREATE VIEW cdc.customers_live AS SELECT id, email, full_name, plan, notes, updated_at
          FROM cdc.customers WHERE NOT _cdc_deleted;
        GRANT USAGE ON SCHEMA cdc TO parity_reader;
        GRANT SELECT ON ALL TABLES IN SCHEMA cdc TO parity_reader;''', 'parity')
    sql('parity-source', 'GRANT SELECT ON public.customers TO parity_reader;', 'parity')
    for name in ('parity-source', 'parity-replica'):
        sql(name, 'ALTER ROLE parity_reader SET default_transaction_read_only=on;', 'parity')
    ips = {n: json.loads(docker('inspect', n))[0]['NetworkSettings']['Networks']['bridge']['IPAddress']
           for n in ('parity-source', 'parity-replica')}
    secrets = {}
    for side in ('SOURCE', 'REPLICA'):
        host = ips['parity-' + side.lower()]
        secrets.update({f'CDC_{side}_JDBC_URL': f'jdbc:postgresql://{host}:5432/parity',
                        f'CDC_{side}_DSN': f'postgresql://{host}:5432/parity',
                        f'CDC_{side}_USER': 'parity_reader',
                        f'CDC_{side}_PASSWORD': 'parity_fixture_reader'})
    (ROOT / 'runtime.env').write_text('\n'.join('SECRET_'+k+'='+base64.b64encode(v.encode()).decode()
                                               for k, v in secrets.items())+'\n')
    print('Isolated databases and SELECT-only roles ready:', ips)
