from urllib import parse as urlparse
import base64
import json
import os
import requests
import boto3

client = boto3.client('lambda')

def lambda_handler(event, context):
    
    msg_map = dict(urlparse.parse_qsl(base64.b64decode(str(event['body'])).decode('ascii')))
    
    print(event['requestContext'])
    
    msg_map['stage'] = event['requestContext']['stage']
    
    print(msg_map)

    payload = msg_map.get('payload', None)
    if payload:
        search_value = payload.get('value', None)
        if search_value:
            search_parameters = {
                "search_name" : msg_map['text'],
                "stage" : msg_map['stage']
            }
            
            search_version = os.environ[f'{msg_map["stage"]}_search_version']
            
            response = client.invoke(
                FunctionName = os.environ['player_search_lambda_arn'],
                InvocationType = 'RequestResponse',
                Payload = json.dumps(search_parameters),
                Qualifier = search_version
            )
            
            lambda_response = json.load(response['Payload'])

            if 'body' in lambda_response:
                player_list = json.loads(lambda_response['body'])
                options = list()
                for player_dict in player_list:
                    name = f"{player_dict['name']}, {player_dict['positions']}, {player_dict['org']}"
                    value = int(player_dict['_id'])

                    options.append({
                        'label': name,
                        'value': value
                    })
                
                response = {
                    'options': options
                }


                return {
                    'statusCode': 200,
                    'body': json.dumps(response)
                }


            return {
                'statusCode': 200
            }



        return {
                'statusCode': 404,
                'body': json.dumps('No search value provided')
            }

    return {
                'statusCode': 404,
                'body': json.dumps('No payload provided')
            }