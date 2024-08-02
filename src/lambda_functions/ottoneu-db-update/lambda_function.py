import json
import boto3
import os
import math

sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')
queueurl = sqs.get_queue_url(QueueName='Ottoneu-db-update-queue')['QueueUrl']

def lambda_handler(event, context):
    
    league_id_str = os.environ['league_ids_list']
    
    print(league_id_str)
    
    league_ids = json.loads(league_id_str)
    
    print(f'Total Number of Leagues: {len(league_ids)}')
    
    chunk_size = math.ceil(float(len(league_ids) / float(os.environ['number_of_chunks'])))
    
    league_id_chunks = divide_lists(league_ids, chunk_size)
    
    for id_chunk in league_id_chunks:
        print(f'length = {len(id_chunk)}')
        print(id_chunk)
        msg_map = dict()
        msg_map['league_ids'] = id_chunk
    
        try:
            sqs.send_message(QueueUrl=queueurl, MessageBody=json.dumps(msg_map))
        except Exception as e:
            print(e)
            return {
                'statusCode': 400, 
                'body': json.dumps('Error when submitting to the queue.')
            }
    
    response = lambda_client.invoke(
        FunctionName = os.environ['player_put_arn'],
        InvocationType = 'RequestResponse'
        )
        
    print(response)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Update success')
    }

def divide_lists(l, n): 
      
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 