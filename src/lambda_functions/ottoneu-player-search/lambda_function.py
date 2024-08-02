import json
from urllib.parse import unquote
import os
from pymongo import MongoClient

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

def lambda_handler(event, context):
    if "search_name" in event:
        search_name = event['search_name']
    elif "queryStringParameters" in event:
        search_name = event["queryStringParameters"].get("search_name", None)
    else:
        search_name = None

    if search_name:
        return player_search(search_name)
    
    if "league_id" in event:
        league_id = event['league_id']
    elif "queryStringParameters" in event:
        league_id = event["queryStringParameters"].get("league_id", None)
    else:
        league_id = None
    
    if league_id:
        return league_search(league_id)

    return {
        'statusCode': 400,
        'body': json.dumps('Invalid search paramters')
    }

def league_search(league_id: str):
    roster_cursor = ottoneu_db.leagues.find({'_id': league_id})
    
    player_dict = next(roster_cursor, None)['rosters']

    return {
        'statusCode': 200,
        'body': json.dumps(player_dict)
    }

def player_search(search_name:str):
    search_name = unquote(search_name)
    search_name = normalize(search_name)
    search_name = f'.*{search_name}.*'
    
    players_col = ottoneu_db.players

    results_cursor = players_col.find({'search_name': {'$regex': search_name, '$options': 'i'}})
    
    results = list()
    for item in results_cursor:
        result = {}
        for key, val in item.items():
            result[key] = val
        result['ottoneu_id'] = int(str(result['_id']))
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