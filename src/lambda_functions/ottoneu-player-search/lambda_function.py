import json
import boto3
from boto3.dynamodb.conditions import Key
from urllib.parse import unquote

def lambda_handler(event, context):
    try:
        if "search_name" in event:
            search_name = event['search_name']
        else:
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
    search_name = clean_full_name(search_name)
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ottoneu-player-db')
    
    split = search_name.split()
    if len(split) == 1:
        index = 'search_last_name'
    elif split[0] in ['DE', 'DEL', 'DI', 'VAN', 'LA', 'ST']:
        index = 'search_last_name'
    else:
        index = 'search_name'

    items = table.query(
        IndexName=f"{index}-index",
        KeyConditionExpression=Key(f"{index}").eq(search_name),
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

def clean_full_name(value:str) -> str:
    cleaned = normalize(value)
    cleaned = cleaned.replace('.', '')
    cleaned = clear_if_ends_with(cleaned, ' JR')
    cleaned = clear_if_ends_with(cleaned, ' SR')
    cleaned = clear_if_ends_with(cleaned, ' II')
    cleaned = clear_if_ends_with(cleaned, ' III')
    cleaned = clear_if_ends_with(cleaned, ' IV')
    cleaned = clear_if_ends_with(cleaned, ' V')
    return cleaned

def clear_if_ends_with(val:str, check:str) -> str:
    if val.endswith(check):
        return val[:-len(check)].strip()
    return val

def normalize(value:str) -> str:
    """Function that removes most diacritics from strings and returns value in all caps"""
    normalize = str.maketrans(normalMap)
    val = value.translate(normalize)
    return val.upper()