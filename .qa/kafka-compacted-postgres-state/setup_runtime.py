import base64
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).parent
def docker(*args, data=None):
    r = subprocess.run(['docker', *args], input=data, text=True, encoding='utf-8', capture_output=True)
    if r.returncode:
        raise RuntimeError(r.stderr or r.stdout)
    return r.stdout.strip()

def sql(text):
    return docker('exec', '-i', 'kafka-state-postgres', 'psql', '-U', 'postgres',
                  '-v', 'ON_ERROR_STOP=1', '-tA', data=text)

if __name__ == '__main__':
    secrets = {
        'KAFKA_STATE_PROPERTIES': json.dumps({'bootstrap.servers': 'kafka-state-broker:9092'}),
        'KAFKA_STATE_JDBC_URL': 'jdbc:postgresql://kafka-state-postgres:5432/postgres',
        'KAFKA_STATE_DSN': 'postgresql://kafka-state-postgres:5432/postgres',
        'KAFKA_STATE_USER': 'postgres',
        'KAFKA_STATE_PASSWORD': 'kafka_state_fixture',
    }
    (ROOT / 'runtime.env').write_text('\n'.join(
        'SECRET_'+k+'='+base64.b64encode(v.encode()).decode() for k,v in secrets.items())+'\n')
    print('Six disposable fixture secrets configured')
