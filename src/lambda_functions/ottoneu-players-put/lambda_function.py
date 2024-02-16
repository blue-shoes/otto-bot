import json
from bs4 import BeautifulSoup as Soup
from pandas import DataFrame
from typing import List
import requests
import boto3

def lambda_handler(event, context):
    try:
        df = get_avg_salary_df()
    except:
        return {
            'statusCode': 500,
            'body': json.dumps('Error getting player universe')
        }
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ottoneu-player-db')
    
    try:
        with table.batch_writer() as batch:
            for idx, row in df.iterrows():
                player_dict = row.to_dict()
                player_dict['ottoneu_id'] = int(idx)
                batch.put_item(Item=player_dict)
    except:
        return {
            'statusCode': 500,
            'body': json.dumps('Error writing player universe')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Player universe updated')
    }

def get_avg_salary_df(game_type : int = None) -> DataFrame:
    '''Scrapes the average salary page for the given game type (default all game types) and returns a DataFrame with the available data. Index is Ottoneu Id'''
    if game_type is None or game_type == 0:
        avg_salary_url = 'https://ottoneu.fangraphs.com/averageValues?export=xml'
    else:
        avg_salary_url = f'https://ottoneu.fangraphs.com/averageValues?export=xml&gameType={game_type}'
    response = requests.get(avg_salary_url)
    salary_soup = Soup(response.text, 'xml')
    rows = salary_soup.find_all('player')
    parsed_rows = [__parse_avg_salary_row(row) for row in rows]
    df = DataFrame(parsed_rows)
    df.columns = ['ottoneu_id','name','search_name','fg_majorleagueid','fg_minorleagueid','positions','org']
    df.set_index('ottoneu_id', inplace=True)
    df.index = df.index.astype(int, copy=False)
    
    df = df.fillna(0)
    return df
    
def __parse_avg_salary_row(row) -> List[str]:
    '''Returns a list of string describing the average salary row'''
    parsed_row = []
    parsed_row.append(row.get('ottoneu_id'))
    parsed_row.append(row.get('name'))
    parsed_row.append(normalize(row.get('name')))
    parsed_row.append(str(row.get('fg_majorleague_id')))
    parsed_row.append(row.get('fg_minorleague_id'))
    parsed_row.append(row.find('positions').text)
    parsed_row.append(row.find('mlb_org').text)
    return parsed_row

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