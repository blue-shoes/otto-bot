import json
import boto3
import os

def lambda_handler(event, context):
    client = boto3.client('lambda')
    
    try:
        if 'body' in event['Records'][0]:
            print('SQS message')
            msg_map = json.loads(event['Records'][0]['body'])
        else:
            print('SNS message')
            message = event['Records'][0]['Sns']['Message']
            msg_map = json.loads(json.loads(message)['default'])
        
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
    except Exception as e:
        print(e)
    
    return {
        'statusCode': 200
    }
