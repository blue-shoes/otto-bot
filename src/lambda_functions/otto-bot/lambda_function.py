import json
from urllib import parse as urlparse
import base64
import boto3
import os

client = boto3.client('lambda')
sqs = boto3.client('sqs')
valid_commands = ['/show-player']

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