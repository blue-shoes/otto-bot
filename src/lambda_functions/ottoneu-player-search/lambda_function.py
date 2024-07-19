import json
from urllib.parse import unquote
import os
from pymongo import MongoClient

client = MongoClient(host=os.environ.get("ATLAS_URI"))
player_db = client.players

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
    search_name = f'.*{search_name}.*'
    
    players_col = player_db.ottoneu

    results_cursor = players_col.find({'search_name': {'$regex': search_name, '$options': 'i'}})
    
    results = list()
    for item in results_cursor:
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