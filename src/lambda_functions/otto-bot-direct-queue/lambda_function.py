import json
import boto3
import os

def lambda_handler(event, context):
    client = boto3.client('lambda')
    
    msg_map = json.loads(event['Records'][0]['body'])
    
    print(msg_map)
    
    if msg_map['stage'] == 'dev':
        response = client.invoke(
            FunctionName = os.environ['queue_processor_arn'],
            InvocationType = 'RequestResponse',
            Payload = json.dumps(msg_map)
        )
    else:
        processor_version = os.environ[f'{msg_map["stage"]}_version']
        response = client.invoke(
            FunctionName = os.environ['queue_processor_arn'],
            InvocationType = 'RequestResponse',
            Payload = json.dumps(msg_map),
            Qualifier=processor_version
        )
    
    print(response)
    
    return {
        'statusCode': 200
    }
