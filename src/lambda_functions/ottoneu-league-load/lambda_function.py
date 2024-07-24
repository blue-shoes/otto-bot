from pymongo import MongoClient
import os

import json

mongo_client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = mongo_client.ottoneu

def lambda_handler(event, context):

    try:
        if "league_id" in event:
            league_id = event['league_id']
        else:
            league_id = event["queryStringParameters"]["league_id"]
    except KeyError:
        return {
            'statusCode': 400,
            'body': json.dumps('Body does not have league_id parameter.')
        }
    if not league_id:
        return {
            'statusCode': 400,
            'body': json.dumps('Empty string passed as league_id')
        }

    roster_cursor = ottoneu_db.leagues.find({'_id': league_id})
    
    player_dict = next(roster_cursor, None)

    return {
        'statusCode': 200,
        'body': json.dumps(player_dict)
    }
