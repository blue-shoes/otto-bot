import json
import os
from pymongo import MongoClient

client = MongoClient(host=os.environ.get('ATLAS_URI'))
ottoneu_db = client.ottoneu


def lambda_handler(event, context):
    if 'search_name' in event:
        search_name = event['search_name']
    elif 'queryStringParameters' in event:
        search_name = event['queryStringParameters'].get('search_name', None)
    else:
        search_name = None

    if search_name:
        return league_search(search_name)

    return {'statusCode': 400, 'body': json.dumps('Invalid search paramters')}


def league_search(league_id: str):

    results = []

    seen_ids = []

    if league_id.isdigit():
        league_id_regex = f'^{league_id}' + '.{0,2}$'
        print(league_id_regex)
        league_cursor = ottoneu_db.leagues.find({'_id': {'$regex': league_id_regex}}, {'name': 1})

        for lg in league_cursor:
            result = {}
            if not lg.get('name', None):
                continue
            result['name'] = lg['name']
            result['id'] = lg['_id']
            results.append(result)
            seen_ids.append(lg['_id'])

    league_name_regex = f'.*{league_id}.*'
    league_cursor = ottoneu_db.leagues.find({'name': {'$regex': league_name_regex, '$options': 'i'}}, {'name': 1})

    for lg in league_cursor:
        if lg['_id'] in seen_ids:
            continue
        result = {}
        result['name'] = lg['name']
        result['id'] = lg['_id']
        results.append(result)

    return {'statusCode': 200, 'body': json.dumps(results)}
