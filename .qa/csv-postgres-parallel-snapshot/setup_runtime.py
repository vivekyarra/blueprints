import base64
from pathlib import Path

ROOT=Path(__file__).parent
if __name__=='__main__':
    values={'SNAPSHOT_JDBC_URL':'jdbc:postgresql://kafka-state-postgres:5432/postgres',
            'SNAPSHOT_USER':'postgres','SNAPSHOT_PASSWORD':'kafka_state_fixture'}
    (ROOT/'runtime.env').write_text('\n'.join(
        'SECRET_'+k+'='+base64.b64encode(v.encode()).decode() for k,v in values.items())+'\n')
    print('Disposable snapshot credentials configured')
