import json
from pymongo import MongoClient, UpdateOne
import os
import boto3

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    print(event)
    
    if event.get('Records', None):
        msg_map = json.loads(event['Records'][0]['body'])
    else:
        msg_map = event
    
    print(msg_map)
    
    if not msg_map.get('league_ids', None):
        return {
            'statusCode': 400,
            'body': json.dumps('league_ids not present in event')
        }
    league_ids = msg_map['league_ids']
    
    leagues_col = ottoneu_db.leagues

    update_leagues = list()

    roster_load_version = os.environ['roster_lambda_version']

    for league_id in league_ids:
        print(f'Getting league_id: {league_id}' )
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
            
            print(response)
            
            if 'body' in lambda_response:
                league_dict = json.loads(lambda_response['body'])
            else:
                league_dict = None
                
            if not league_dict:
                print(f'!!League_id {league_id} not valid')
                continue
            
            print(f'Roster length {len(league_dict)}')
            
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