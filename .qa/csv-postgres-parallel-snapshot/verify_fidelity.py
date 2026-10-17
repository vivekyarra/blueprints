import json
from run_runtime import execute, sql

execute('payload-fidelity','http://csv-snapshot-files:8000/healthy.csv','SUCCESS',rows=11)
assert json.loads(sql('SELECT jsonb_agg(customer_name ORDER BY order_id) FROM csv_snapshot.orders;'))==[
    '東京, Customer\n'+str(i) for i in range(1,12)]
execute('large-fidelity','http://csv-snapshot-files:8000/large.csv','SUCCESS',rows=20003,chunk=5000)
assert sql("SELECT count(*) FROM csv_snapshot.orders WHERE customer_name <> 'C '||order_id OR price <> 1 OR quantity <> 2 OR total <> 2;")=='0'
assert sql("SELECT min(order_id)||','||max(order_id) FROM csv_snapshot.orders;")=='1,20003'
print('Full payload fidelity assertions passed')
