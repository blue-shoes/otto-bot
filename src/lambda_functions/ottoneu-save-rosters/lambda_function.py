import json
from pymongo import MongoClient, UpdateOne
import os
import boto3

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    leagues_col = ottoneu_db.leagues

    update_leagues = list()

    #TODO: Get league table
    league_ids = ['160']

    roster_load_version = os.environ['roster_lambda_version']

    for league_id in league_ids:
        try:
            roster_load_parameters = {
                "league_id" : league_id
            }
            
            response = lambda_client.invoke(
                FunctionName = os.environ['roster_lambda_arn'],
                InvocationType = 'RequestResponse',
                Payload = json.dumps(roster_load_parameters),
                Qualifier = roster_load_version
            )
            
            lambda_response = json.load(response['Payload'])
            
            if 'body' in lambda_response:
                league_dict = json.loads(lambda_response['body'])
            else:
                league_dict = None
                
            if not league_dict:
                continue
            update_leagues.append(UpdateOne({'_id': league_id},  {'$set': league_dict}, upsert=True))
        except Exception as e:
            print(e)

    if update_leagues:
        try:
            leagues_col.bulk_write(update_leagues, ordered=False)
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'body': json.dumps('Error writing to db.')
            }

    return {
        'statusCode': 200
    }

def get_league_ids():
    ...
