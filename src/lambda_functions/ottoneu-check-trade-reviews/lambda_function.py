import os
from pymongo import MongoClient, UpdateOne
import datetime
import requests
import urllib
import json

client = MongoClient(host=os.environ.get('ATLAS_URI'))
ottoneu_db = client.ottoneu


def lambda_handler(event, context):
    message_col = ottoneu_db.messages

    two_days = (datetime.datetime.now() - datetime.timedelta(days=2)).timestamp()

    results_cursor = message_col.find({'creation': {'$gte': two_days}})

    update_list = list()

    for item in results_cursor:
        trade = {}
        for key, val in item.items():
            trade[key] = val

        vote_update = message_trade(trade)

        if vote_update:
            update_list.append(UpdateOne({'_id': trade['_id']}, {'$set': {'reactions': vote_update}}, upsert=True))

    if update_list:
        message_col.bulk_write(update_list, ordered=False)

    return {'statusCode': 200}


def message_trade(trade: dict) -> str:
    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    token = os.environ[trade['token_lookup']]

    react_dict = {}
    react_dict['token'] = token
    react_dict['timestamp'] = trade['ts']
    react_dict['channel'] = trade['channel']

    response = requests.post(
        'https://slack.com/api/reactions.get',
        headers=header,
        data=urllib.parse.urlencode(react_dict),
    )

    if not response.ok:
        return None

    content = json.loads(response.content)['message']
    reactions = content['reactions']
    for r in reactions:
        name = r['name']
        count = r['count']
        if name == 'one':
            vote_1 = count
        elif name == 'two':
            vote_2 = count
        elif name == 'scales':
            scales = count

    old_votes = json.loads(trade['reactions'])
    if vote_1 <= old_votes['one'] and vote_2 <= old_votes['two'] and scales <= old_votes['scales']:
        return json.dumps({'one': vote_1, 'two': vote_2, 'scales': scales})

    link = content['permalink']

    text = f'You have new votes!\n:one:: {vote_1 - 1}\t:two:: {vote_2 - 1}\t:scales:: {scales - 1}\n{link}'

    open_dict = {}
    open_dict['token'] = token
    open_dict['users'] = trade['user_id']

    response = requests.post(
        'https://slack.com/api/conversations.open',
        headers=header,
        data=urllib.parse.urlencode(open_dict),
    )

    if not response.ok:
        return None

    channel_content = json.loads(response.content)

    message_dict = {}
    message_dict['token'] = token
    message_dict['channel'] = channel_content['channel']['id']
    message_dict['text'] = text

    response = requests.post(
        'https://slack.com/api/chat.postMessage',
        headers=header,
        data=urllib.parse.urlencode(message_dict),
    )

    return json.dumps({'one': vote_1, 'two': vote_2, 'scales': scales})
