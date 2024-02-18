import json
from urllib import parse as urlparse
import base64
import requests

def lambda_handler(event, context):
    
    msg_map = dict(urlparse.parse_qsl(base64.b64decode(str(event['body'])).decode('ascii')))
    
    if msg_map['command'] == '/show-player':
        return show_player(msg_map)
    
    return {
        'statusCode': 404,
        'body': json.dumps('Not a valid command')
    }

def show_player(msg_map):
    
    response_dict = {}
    response_dict['response_type'] = 'in_channel'
    response_dict['text'] = msg_map['command'] + ' ' + msg_map['text']
    response_dict['attachments'] = [{'text': 'this would be a player'}]
    
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
