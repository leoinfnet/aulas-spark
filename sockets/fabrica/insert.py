import json
import random
import string
import time
import uuid
import sys

import boto3

client = boto3.client(
    'kinesis',
    aws_access_key_id='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    region_name='us-east-2',
)

for _ in range(10000):
    data = {
        'client' : sys.argv[1],
        'id': str(uuid.uuid4()),
        'random_number': random.randint(32, 200),
        'random_string': ''.join(random.choice(string.ascii_lowercase) for i in range(10))
    }
    print(data)

    response = client.put_record(
        StreamName='aula_spark',
        Data=json.dumps(data),
        PartitionKey="partitionkey")

    time.sleep(0.3)