import json
from time import sleep
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
for i in range(6):

    for uid in range(10):

        # datetime(year, month, day, hour, minute, second, microsecond)
        time = datetime(2020, 11, 20, 7, i*5, uid, 0)

        data = { 
            'id' : uid,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'tag ': 'blah',
            'name' : 'sam',
            'index' : i,
            'score': { 
                    'row1': 300,
                    'row2' : 200
                }
            }
        producer.send('topic_test', key = b'key', value = data)
        print(data)
        sleep(0.001)
