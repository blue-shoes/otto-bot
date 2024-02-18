import json
import urllib
from urllib import parse as urlparse
import base64
import boto3
import requests
import os

client = boto3.client('lambda')
sqs = boto3.client('sqs')
valid_commands = ['/show-player']
loading_commands = ['/show-player']

def lambda_handler(event, context):
    
    msg_map = dict(urlparse.parse_qsl(base64.b64decode(str(event['body'])).decode('ascii')))
    if msg_map.get('command', None) not in valid_commands:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Not a valid slash command: {msg_map['command']}')
        }
    
    if not msg_map.get('text', None):
        return {
            'statusCode': 400, 
            'body': json.dumps(f'No arguments given for slash command: {msg_map['command']}')
        }
    
    if msg_map['command'] in loading_commands:
        modal_res = initiate_loading_modal(msg_map)
        if modal_res['ok']:
            msg_map['view_id'] = modal_res['view']['id']
        else:
            print(modal_res)
            return {
            'statusCode': 400, 
            'body': json.dumps(f'Error creating interactive dialog')
        }
    
    queueurl = sqs.get_queue_url(QueueName='Otto-bot-queue')['QueueUrl']
    try:
        sqs.send_message(QueueUrl=queueurl, MessageBody=json.dumps(msg_map))
    except:
        return {
            'statusCode': 400, 
            'body': json.dumps(f'Error when submitting to the queue.')
        }
    
    return {
        'statusCode': 202
    }

def initiate_loading_modal(msg_map):
    post_url = 'https://slack.com/api/views.open'
    view = get_modal()
    view = view.replace('<callbackid>', '"' + msg_map['trigger_id'] + '"')
    data = urllib.parse.urlencode({
        "trigger_id": msg_map['trigger_id'],
        "view": view,
        "token": os.environ['BOT_TOKEN']
    })
    data = data.encode("utf-8")
    request = urllib.request.Request(post_url, data=data, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    res = json.loads(urllib.request.urlopen(request).read().decode('utf-8'))
    return res

def get_modal():
    return """
    {
        "type": "modal",
        "callback_id": <callbackid>,
        "title": {
            "type": "plain_text",
            "text": "Otto-bot Input Helper",
            "emoji": true
        },
        "submit": {
            "type": "plain_text",
            "text": "Submit",
            "emoji": true
        },
        "close": {
            "type": "plain_text",
            "text": "Cancel",
            "emoji": true
        },
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Loading players..."
                }
            }
        ]
    }
    """