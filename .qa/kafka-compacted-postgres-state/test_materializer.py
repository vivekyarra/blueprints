"""Integration checks against real Kafka and PostgreSQL, using the submitted script."""
import json
import hashlib
import os
import random
import subprocess
import sys
import time
import unittest
import uuid
from pathlib import Path

import psycopg
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

FLOW = yaml.safe_load(Path('/flows/kafka-compacted-postgres-state.yaml').read_text())
SCRIPT = FLOW['tasks'][1]['script']
Path('/qa/materialize.py').write_text(SCRIPT)
DSN = 'postgresql://postgres:kafka_state_fixture@kafka-state-postgres:5432/postgres'
CONFIG = {'bootstrap.servers': 'kafka-state-broker:9092'}
ADMIN = AdminClient(CONFIG)

def sql(statement, params=()):
    with psycopg.connect(DSN) as conn:
        cursor = conn.execute(statement, params)
        return cursor.fetchall() if cursor.description else []

def create_topic(partitions=2, compact=True):
    topic = 'qa-' + uuid.uuid4().hex[:12]
    ADMIN.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1,
        config={'cleanup.policy': 'compact' if compact else 'delete'})])[topic].result(30)
    return topic

def produce(topic, records):
    producer = Producer(CONFIG)
    errors = []
    def delivered(error, message):
        if error:
            errors.append(str(error))
    for partition, key, value in records:
        producer.produce(topic, partition=partition, key=key, value=value, on_delivery=delivered)
    assert producer.flush(30) == 0
    assert not errors, errors

def run(topic, limit=1000, seconds=2, ok=True):
    env = {**os.environ, 'KAFKA_PROPERTIES': json.dumps(CONFIG), 'PG_DSN': DSN,
           'TOPIC': topic, 'MAX_RECORDS': str(limit), 'POLL_SECONDS': str(seconds)}
    result = subprocess.run([sys.executable, '/qa/materialize.py'], env=env,
        cwd='/qa', capture_output=True, text=True, timeout=90)
    if ok:
        assert result.returncode == 0, result.stderr + result.stdout
        return json.loads(Path('/qa/batch-report.json').read_text())
    assert result.returncode != 0, result.stdout
    return result.stderr

def snapshot(topic):
    return (sql('SELECT record_key,payload,deleted,kafka_partition,kafka_offset FROM kafka_state.records WHERE topic=%s ORDER BY record_key',(topic,)),
            sql('SELECT next_offsets FROM kafka_state.positions WHERE topic=%s',(topic,)))

class MaterializerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sql(FLOW['tasks'][0]['sql'])

    def test_latest_tombstone_unicode_empty_key_and_json_null(self):
        topic = create_topic()
        produce(topic, [(0,'alice','{"v":1}'),(0,'alice','{"v":2}'),(1,'bob','{}'),
            (0,'gone','{}'),(0,'gone',None),(1,'','{"a":"東京"}'),(1,'json-null','null')])
        report = run(topic)
        self.assertEqual((report['consumed'],report['tombstones'],report['live_keys']),(7,1,4))
        rows = {r[0]:r[1:] for r in snapshot(topic)[0]}
        self.assertEqual(rows['alice'][:2],({'v':2},False))
        self.assertEqual(rows['gone'][:2],(None,True))
        self.assertEqual(rows['json-null'][:2],(None,False))
        self.assertEqual(rows[''][0],{'a':'東京'})
        before=snapshot(topic)
        self.assertEqual(run(topic)['consumed'],0)
        self.assertEqual(snapshot(topic),before)
        produce(topic,[(0,'gone','{"restored":true}'),(1,'bob',None)])
        self.assertEqual(run(topic)['consumed'],2)
        rows = {r[0]:r[1:] for r in snapshot(topic)[0]}
        self.assertEqual(rows['gone'][:2],({'restored':True},False))
        self.assertEqual(rows['bob'][:2],(None,True))

    def test_bounded_batches_resume_without_skipping(self):
        topic=create_topic(1)
        produce(topic,[(0,'k',json.dumps(i)) for i in range(7)])
        offsets=[]
        for i in range(7):
            report=run(topic,limit=1)
            self.assertEqual(report['consumed'],1)
            offsets.append(report['next_offsets']['0'])
        self.assertEqual(offsets,list(range(1,8)))
        self.assertEqual(snapshot(topic)[0][0][1],6)

    def test_invalid_json_rolls_back_prior_valid_record(self):
        topic=create_topic(1)
        produce(topic,[(0,'baseline','{}')]); run(topic)
        before=snapshot(topic)
        produce(topic,[(0,'new','{"v":1}'),(0,'bad','not json')])
        self.assertIn('JSONDecodeError',run(topic,ok=False))
        self.assertEqual(snapshot(topic),before)
        run(topic,ok=False)
        self.assertEqual(snapshot(topic),before)

    def test_database_failure_rolls_back_values_and_positions(self):
        topic=create_topic(1)
        produce(topic,[(0,'baseline','{}')]); run(topic)
        before=snapshot(topic)
        sql("ALTER TABLE kafka_state.records ADD CONSTRAINT qa_reject CHECK (record_key <> 'reject-qa')")
        try:
            produce(topic,[(0,'first','{}'),(0,'reject-qa','{}')])
            self.assertIn('CheckViolation',run(topic,ok=False))
            self.assertEqual(snapshot(topic),before)
        finally:
            sql('ALTER TABLE kafka_state.records DROP CONSTRAINT qa_reject')
        self.assertEqual(run(topic)['consumed'],2)

    def test_key_partition_change_rolls_back(self):
        topic=create_topic()
        produce(topic,[(0,'k','{}')]); run(topic)
        before=snapshot(topic)
        produce(topic,[(1,'k','{"v":2}')])
        self.assertIn('key moved partitions',run(topic,ok=False))
        self.assertEqual(snapshot(topic),before)

    def test_null_key_rolls_back(self):
        topic=create_topic(1,False)
        produce(topic,[(0,'ok','{}'),(0,None,'{}')])
        self.assertIn('Null Kafka key',run(topic,ok=False))
        self.assertEqual(snapshot(topic),([],[]))

    def test_nonfinite_json_rolls_back(self):
        topic=create_topic(1)
        produce(topic,[(0,'k','{"v":NaN}')])
        self.assertIn('Non-finite JSON',run(topic,ok=False))
        self.assertEqual(snapshot(topic),([],[]))

    def test_lock_excludes_second_writer(self):
        topic=create_topic(1)
        with psycopg.connect(DSN) as conn:
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',('kestra-kafka-state:'+topic,))
            self.assertIn('topic lock',run(topic,ok=False))
        self.assertEqual(run(topic)['consumed'],0)

    def test_partition_addition_fails(self):
        topic=create_topic(1); run(topic)
        before=snapshot(topic)
        ADMIN.create_partitions([NewPartitions(topic,2)])[topic].result(30)
        self.assertIn('Partition set changed',run(topic,ok=False))
        self.assertEqual(snapshot(topic),before)

    def test_missing_positions_cannot_reuse_existing_values(self):
        topic=create_topic(1); produce(topic,[(0,'k','{}')]); run(topic)
        sql('DELETE FROM kafka_state.positions WHERE topic=%s',(topic,))
        before=snapshot(topic)
        self.assertIn('Values exist without',run(topic,ok=False))
        self.assertEqual(snapshot(topic),before)

    def test_offsets_outside_retention_are_not_reset(self):
        topic=create_topic(1); produce(topic,[(0,'k','{}')]); run(topic)
        for offset in (-1,999999):
            sql('UPDATE kafka_state.positions SET next_offsets=%s::jsonb WHERE topic=%s',
                (json.dumps({'0':offset}),topic))
            before=snapshot(topic)
            self.assertIn('outside the retained log',run(topic,ok=False))
            self.assertEqual(snapshot(topic),before)

    def test_aborted_transaction_not_materialized(self):
        topic=create_topic(1)
        producer=Producer({**CONFIG,'transactional.id':'qa-'+uuid.uuid4().hex})
        producer.init_transactions(30); producer.begin_transaction()
        producer.produce(topic,key='aborted',value='{}',partition=0)
        producer.flush(30); producer.abort_transaction(30)
        produce(topic,[(0,'committed','{}')])
        self.assertEqual(run(topic)['consumed'],1)
        self.assertEqual([r[0] for r in snapshot(topic)[0]],['committed'])

    def test_cluster_identity_change_fails(self):
        topic=create_topic(1); run(topic)
        sql('UPDATE kafka_state.positions SET cluster_id=%s WHERE topic=%s',('another-cluster',topic))
        before=snapshot(topic)
        self.assertIn('Cluster or topic ID changed',run(topic,ok=False))
        self.assertEqual(snapshot(topic),before)

    def test_recreated_topic_with_same_name_fails(self):
        topic=create_topic(1); produce(topic,[(0,'old','{}')]); run(topic)
        before=snapshot(topic)
        ADMIN.delete_topics([topic])[topic].result(30)
        deadline=time.monotonic()+30
        while topic in ADMIN.list_topics(timeout=10).topics:
            assert time.monotonic()<deadline
            time.sleep(.25)
        ADMIN.create_topics([NewTopic(topic,num_partitions=1,replication_factor=1,
            config={'cleanup.policy':'compact'})])[topic].result(30)
        produce(topic,[(0,'new','{}'),(0,'new','{"v":2}')])
        self.assertIn('Cluster or topic ID changed',run(topic,ok=False))
        self.assertEqual(snapshot(topic),before)

    def test_process_kill_does_not_commit_partial_batch(self):
        topic=create_topic(1); produce(topic,[(0,'block','0')]); run(topic)
        before=snapshot(topic)
        produce(topic,[(0,'new','1'),(0,'block','2')])
        env={**os.environ,'KAFKA_PROPERTIES':json.dumps(CONFIG),'PG_DSN':DSN,
             'TOPIC':topic,'MAX_RECORDS':'1000','POLL_SECONDS':'10',
             'PGAPPNAME':'qa-kill-'+topic}
        with psycopg.connect(DSN) as conn:
            conn.execute('SELECT 1 FROM kafka_state.records WHERE topic=%s FOR UPDATE',(topic,))
            child=subprocess.Popen([sys.executable,'/qa/materialize.py'],env=env,cwd='/qa',
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                deadline=time.monotonic()+30
                while not sql("SELECT 1 FROM pg_stat_activity WHERE application_name=%s AND wait_event_type='Lock'",(env['PGAPPNAME'],)):
                    assert time.monotonic()<deadline,'Materializer never reached locked UPDATE'
                    assert child.poll() is None,child.communicate()
                    time.sleep(.1)
                child.kill(); child.communicate(timeout=10)
            finally:
                if child.poll() is None:
                    child.kill(); child.communicate(timeout=10)
        deadline=time.monotonic()+20
        while sql('SELECT 1 FROM pg_stat_activity WHERE application_name=%s',(env['PGAPPNAME'],)):
            assert time.monotonic()<deadline,'Database session did not roll back'
            time.sleep(.1)
        self.assertEqual(snapshot(topic),before)
        self.assertEqual(run(topic)['consumed'],2)

    def test_randomized_3500_records_matches_independent_oracle(self):
        topic=create_topic(3)
        rng=random.Random(67819); records=[]; expected={}; counts=[0,0,0]
        for i in range(3500):
            key='key-'+str(rng.randrange(201))
            partition=int.from_bytes(hashlib.sha256(key.encode()).digest()[:2],'big')%3
            value=None if rng.randrange(7)==0 else json.dumps({'n':i,'text':'Grüße 東京'})
            expected[key]=(None if value is None else json.loads(value),value is None,
                           partition,counts[partition])
            counts[partition]+=1
            records.append((partition,key,value))
        produce(topic,records)
        total=0
        for _ in range(20):
            report=run(topic,limit=333)
            self.assertLessEqual(report['consumed'],333)
            total+=report['consumed']
            if not report['consumed']:
                break
        self.assertEqual(total,3500)
        self.assertEqual({r[0]:r[1:] for r in snapshot(topic)[0]},expected)
        self.assertEqual(report['next_offsets'],{str(p):n for p,n in enumerate(counts)})

if __name__ == '__main__':
    unittest.main(verbosity=2)
