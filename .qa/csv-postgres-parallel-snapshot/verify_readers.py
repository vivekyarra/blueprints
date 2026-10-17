"""A real reader must see either the old complete snapshot or the new one."""
import json
from pathlib import Path
import threading
import time

from run_runtime import execute, sql, ROOT

execute('reader-baseline','http://csv-snapshot-files:8000/healthy.csv','SUCCESS',rows=11)
sql('''CREATE OR REPLACE FUNCTION csv_snapshot.qa_pause() RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN IF NEW.order_id=1 THEN PERFORM pg_sleep(3); END IF; RETURN NEW; END $$;
       CREATE TRIGGER qa_pause BEFORE INSERT ON csv_snapshot.orders
       FOR EACH ROW EXECUTE FUNCTION csv_snapshot.qa_pause();''')
observed=[]; stop=threading.Event(); errors=[]
def reader():
    try:
        while not stop.is_set():
            observed.append({'time':time.monotonic(),'rows':int(sql('SELECT count(*) FROM csv_snapshot.orders;'))})
            stop.wait(.1)
    except Exception as error:
        errors.append(str(error))

thread=threading.Thread(target=reader,daemon=True)
thread.start()
try:
    deadline=time.monotonic()+20
    while not observed:
        assert time.monotonic()<deadline
        time.sleep(.1)
    execute('reader-publication','http://csv-snapshot-files:8000/large.csv','SUCCESS',rows=20003,chunk=5000)
    deadline=time.monotonic()+20
    while not any(row['rows']==20003 for row in observed):
        assert time.monotonic()<deadline
        time.sleep(.1)
finally:
    stop.set(); thread.join(timeout=20)
    sql('DROP TRIGGER qa_pause ON csv_snapshot.orders; DROP FUNCTION csv_snapshot.qa_pause();')
assert not thread.is_alive() and not errors, errors
assert {r['rows'] for r in observed}=={11,20003},observed
(ROOT/'reader-observations.json').write_text(json.dumps(observed,indent=2))
print('Concurrent reader observed only complete snapshots:',len(observed),'samples')
