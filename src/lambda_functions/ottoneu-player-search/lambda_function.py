import json
import boto3
from boto3.dynamodb.conditions import Key
from urllib.parse import unquote

def lambda_handler(event, context):
    try:
        search_name = event["queryStringParameters"]["search_name"]
    except KeyError:
        return {
            'statusCode': 400,
            'body': json.dumps('Body does not have search_name parameter.')
        }
    if not search_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Empty string passed as search name')
        }
    search_name = unquote(search_name)
    search_name = normalize(search_name)
    print(f'search_name = {search_name}')
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ottoneu-player-db')
    
    items = table.query(
        IndexName="search_name-index",
        KeyConditionExpression=Key("search_name").eq(search_name),
    )

    results = []
    
    for item in items['Items']:
        result = {}
        for key, val in item.items():
            result[key] = val
        result['ottoneu_id'] = int(str(result['ottoneu_id']))
        results.append(result)
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

normalMap = {'À': 'A', 'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
             'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a', 'ª': 'A',
             'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
             'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
             'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
             'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
             'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O',
             'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o', 'º': 'O',
             'Ù': 'U', 'Ú': 'U', 'Û': 'U', 'Ü': 'U',
             'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
             'Ñ': 'N', 'ñ': 'n',
             'Ç': 'C', 'ç': 'c',
             '§': 'S',  '³': '3', '²': '2', '¹': '1'}


def normalize(value:str) -> str:
    """Function that removes most diacritics from strings and returns value in all caps"""
    normalize = str.maketrans(normalMap)
    val = value.translate(normalize)
    return val.upper()