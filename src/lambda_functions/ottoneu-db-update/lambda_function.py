import json
import boto3
import os
import math
import requests

sns = boto3.client('sns')
lambda_client = boto3.client('lambda')


def lambda_handler(event, context):
    league_response = requests.get('https://ottoneu.fangraphs.com/ajax/browseleagues')
    league_ids = [d['ID'] for d in league_response.json()]

    print(f'Total Number of Leagues: {len(league_ids)}')

    chunk_size = math.ceil(float(len(league_ids) / float(os.environ['number_of_chunks'])))

    league_id_chunks = divide_lists(league_ids, chunk_size)

    for id_chunk in league_id_chunks:
        print(f'length = {len(id_chunk)}')
        print(id_chunk)
        msg_map = dict()
        msg_map['league_ids'] = id_chunk

        try:
            target_arn = os.environ['rosterload_sns_arn']
            result = sns.publish(TargetArn=target_arn, Message=json.dumps({'default': json.dumps(msg_map)}))
            print(result)
        except Exception as e:
            print(e)
            return {'statusCode': 400, 'body': json.dumps('Error when submitting to the queue.')}

    response = lambda_client.invoke(FunctionName=os.environ['player_put_arn'], InvocationType='RequestResponse')

    print(response)

    return {'statusCode': 200, 'body': json.dumps('Update success')}


def divide_lists(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i : i + n]
