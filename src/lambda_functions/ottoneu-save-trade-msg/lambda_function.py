import json
import os
from pymongo import MongoClient
from datetime import datetime

client = MongoClient(host=os.environ.get('ATLAS_URI'))
ottoneu_db = client.ottoneu


def lambda_handler(event, context):
    payload = json.loads(event['payload'])
    metadata = payload['view']['private_metadata'].split(',')

    data = {}
    data['ts'] = event['ts']
    data['token_lookup'] = f'{event["stage"]}_{metadata[3]}_token'
    data['user_id'] = payload['user']['id']
    data['channel'] = metadata[2]

    data['creation'] = datetime.now().timestamp()
    data['reactions'] = json.dumps({'one': 1, 'two': 1, 'scales': 1})

    tr_msg_col = ottoneu_db.messages

    tr_msg_col.insert_one(data)

    return {'statusCode': 200}
