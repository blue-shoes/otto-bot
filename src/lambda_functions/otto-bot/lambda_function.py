import json
from urllib import parse as urlparse
import base64
import requests
import boto3
import os

client = boto3.client('lambda')

def lambda_handler(event, context):
    
    msg_map = dict(urlparse.parse_qsl(base64.b64decode(str(event['body'])).decode('ascii')))
    
    if msg_map['command'] == '/show-player':
        return show_player(msg_map)
    
    return {
        'statusCode': 404,
        'body': json.dumps('Not a valid command')
    }

def show_player(msg_map):

    search_paramters = {
        "search_name" : msg_map['text']
    }
    
    response = client.invoke(
        FunctionName = os.environ['player_search_lambda_arn'],
        InvocationType = 'RequestResponse',
        Payload = json.dumps(search_paramters)
    )
    
    lambda_response = json.load(response['Payload'])
    
    player_list = json.loads(lambda_response['body'])
    player_one = player_list[0]
    player_link = f'https://ottoneu.fangraphs.com/playercard/{player_one["ottoneu_id"]}/3'
    
    text_response = f'<{player_link}|{player_one["name"]}> {player_one["positions"]}, {player_one["org"]}'

    response_dict = {}
    response_dict['response_type'] = 'in_channel'
    response_dict['text'] = text_response
    
    header = {'Content-Type': 'application/json'}
    response = requests.post(msg_map['response_url'], headers=header, data=json.dumps(response_dict))
    
    if response.status_code != 200:
        print(f'Error getting response_url: {response.status_code}: {response.text}')
        return {
            'statusCode': 200,
            'text': 'There was an error getting the player'
        }
    
    return {
        'statusCode': 200
    }
