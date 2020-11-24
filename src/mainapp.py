import logging
import os
import json
import threading
import time
from multiprocessing import Process
from queue import Queue
from kafka import KafkaConsumer
from time import sleep
from typing import List
from datetime import datetime
from math import ceil

logging.basicConfig(
    # level=getattr(logging, os.getenv('LOGLEVEL', '').upper(), 'INFO'),
    level = logging.INFO,
    format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s',
    filename = 'C:\\Users\\Admin\\work\\main.log'
    # filemode = 'w'
)

epoch = datetime(1970, 1, 1)

def currentbatchtime(ti: str, batchinterval: int) -> int:
    # dtime = datetime.strptime("2016-04-15T08:27:18-0500", "%Y-%m-%dT%H:%M:%S%z")
    # print(datetime.strptime("2016-04-15T08:27:18-0500", "%Y-%m-%dT%H:%M:%S%z"))
    dtime = datetime.strptime(ti, "%Y-%m-%dT%H:%M:%S")
    ttime = (dtime - epoch).total_seconds() 
    batchinterval=batchinterval*60
    return int(ceil(ttime/batchinterval)*batchinterval)

def _process_batch(q: Queue[List[dict]]) -> None:
    # while True:
    batch = q.get(timeout=60)  # Set timeout to care for POSIX<3.0 and Windows.
    threadid =  threading.get_ident()
    logging.info(
        '#%sT%s - Processed batch: %s',
        os.getpid(), threadid, batch[0]['batchtime']
    )
    time.sleep(0.01)
    q.task_done()


def _consume(config: dict) -> None:
    logging.info(
        '#%s - Starting consumer group=%s, topic=%s',
        os.getpid(), config['group.id'], config['topic'],
    )
    c = KafkaConsumer(
        'topic_test',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    q: Queue[List[dict]] = Queue(maxsize=config['MaxQueueSize'])
    batch: List[dict] = []
    for event in c:
        try:
            msg = event.value
            if msg is None:
                continue
            batchtime = currentbatchtime(msg['timestamp'], config['BatchInterValMinutes'])
            msg['batchtime'] = batchtime
            logging.info('#%s - Message timestamp=%s, Batchtime = %s',
            os.getpid(), msg['timestamp'], batchtime)
            if len(batch)==0:
                batch.append(msg)
            elif batchtime == batch[-1]['batchtime']:
                batch.append(msg)
            else:
                ids = [b['id'] for b in batch]
                for ii in set(ids):
                    b = [x for i, x in enumerate(batch) if ids[i]==ii]
                    q.put(b)
                    t = threading.Thread(target=_process_batch, args=(q, ))
                    t.start()
                batch = []
                batch.append(msg)

        except Exception:  
            logging.exception('#%s - Worker terminated.', os.getpid())
            c.close()

def main(config: dict) -> None:
    """
    Simple program that consumes messages from Kafka topic and prints to
    STDOUT.
    """
    workers: List[Process] = []
    while True:
        num_alive = len([w for w in workers if w.is_alive()])
        if config['num_workers'] == num_alive:
            continue
        for _ in range(config['num_workers']-num_alive):
            p = Process(target=_consume, daemon=True, args=(config,))
            p.start()
            workers.append(p)
            logging.info('Starting worker #%s', p.pid)

if __name__ == '__main__':

    main(config={
        # At most, this should be the total number of Kafka partitions on
        # the topic.
        'topic': 'topic_test',
        'num_workers': 1,
        'num_threads': 4,
        'group.id': 'my-group-id',
        'auto.offset.reset': 'earliest',
        # Commit manually to care for abrupt shutdown.
        'enable.auto.commit': False,
        'MaxQueueSize': 200,
        'BatchInterValMinutes': 5
        })
