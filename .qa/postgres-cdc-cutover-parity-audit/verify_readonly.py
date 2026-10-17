"""Verify database GRANTs, rather than relying only on a session read-only flag."""
import json
import subprocess
from pathlib import Path

results=[]
for side,table in [('source','public.customers'),('replica','cdc.customers')]:
    proc=subprocess.run(['docker','exec','-i','parity-'+side,'psql','-U','parity_reader',
        '-d','parity','-v','ON_ERROR_STOP=1','-tA'],
        input=f"BEGIN READ WRITE; UPDATE {table} SET email='should-not-write' WHERE id=1;",
        encoding='utf-8',capture_output=True)
    assert proc.returncode != 0 and 'permission denied' in proc.stderr, (side,proc.stdout,proc.stderr)
    results.append({'database':side,'write_rejected':True,'error':proc.stderr.strip()})
Path(__file__).with_name('readonly-results.json').write_text(json.dumps(results,indent=2))
print('Both SELECT-only roles rejected UPDATE even after BEGIN READ WRITE.')
