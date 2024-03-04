import json
from bs4 import BeautifulSoup as Soup
from pandas import DataFrame
from typing import List
import requests
import boto3

import io
import re
import zipfile

from typing import List, Iterable

import pandas as pd
import requests

def lambda_handler(event, context):
    try:
        df = get_avg_salary_df()
    except Exception as e:
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
    df.columns = ['ottoneu_id','name','search_name','search_last_name','fg_majorleagueid','fg_minorleagueid','positions','org', 'mlbamid']
    df.set_index('ottoneu_id', inplace=True)
    df.index = df.index.astype(int, copy=False)
    
    df = df.fillna(0)
    return df
    
def __parse_avg_salary_row(row) -> List[str]:
    '''Returns a list of string describing the player row'''
    parsed_row = []
    parsed_row.append(row.get('ottoneu_id'))
    parsed_row.append(row.get('name'))
    full_search_name = clean_full_name(row.get('name'))
    parsed_row.append(full_search_name)
    search_last_name = get_search_last_name(full_search_name)
    parsed_row.append(search_last_name)
    fg_major_id = str(row.get('fg_majorleague_id'))
    parsed_row.append(fg_major_id)
    parsed_row.append(row.get('fg_minorleague_id'))
    parsed_row.append(row.find('positions').text)
    parsed_row.append(row.find('mlb_org').text)
    if fg_major_id:
        p_df = playerid_reverse_lookup([int(fg_major_id)], key_type='fangraphs')
        if len(p_df) > 0:
            parsed_row.append(p_df.loc[0,'key_fangraphs'])
        else:
            parsed_row.append(None)
    else:
        parsed_row.append(None)
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

def get_search_last_name(name:str) -> str:
    name_split = name.split()
    multi_word_ln = ['DE', 'DEL', 'DI', 'VAN', 'LA', 'ST']
    for mwl in multi_word_ln:
        if mwl in name_split: 
            index = name_split.index(mwl)
            return ' '.join(name_split[index:])
    return name_split[-1]

def clean_full_name(value:str) -> str:
    cleaned = normalize(value)
    cleaned = cleaned.replace('.', '')
    cleaned = clear_if_ends_with(cleaned, ' JR')
    cleaned = clear_if_ends_with(cleaned, ' SR')
    cleaned = clear_if_ends_with(cleaned, ' II')
    cleaned = clear_if_ends_with(cleaned, ' III')
    cleaned = clear_if_ends_with(cleaned, ' IV')
    cleaned = clear_if_ends_with(cleaned, ' V')
    cleaned = ' '.join(cleaned.split())
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

# This is adapted from pybaseball to elimiate the need for a heavy-weight library w/ dependencies

url = "https://github.com/chadwickbureau/register/archive/refs/heads/master.zip"
PEOPLE_FILE_PATTERN = re.compile("/people.+csv$")

_client = None

def _extract_people_files(zip_archive: zipfile.ZipFile) -> Iterable[zipfile.ZipInfo]:
    return filter(
        lambda zip_info: re.search(PEOPLE_FILE_PATTERN, zip_info.filename),
        zip_archive.infolist(),
    )


def _extract_people_table(zip_archive: zipfile.ZipFile) -> pd.DataFrame:
    dfs = map(
        lambda zip_info: pd.read_csv(
            io.BytesIO(zip_archive.read(zip_info.filename)),
            low_memory=False
        ),
        _extract_people_files(zip_archive),
    )
    return pd.concat(dfs, axis=0)


def chadwick_register(save: bool = False) -> pd.DataFrame:
    ''' Get the Chadwick register Database '''
    print('Gathering player lookup table. This may take a moment.')
    s = requests.get(url).content
    mlb_only_cols = ['key_retro', 'key_bbref', 'key_fangraphs', 'mlb_played_first', 'mlb_played_last']
    cols_to_keep = ['name_last', 'name_first', 'key_mlbam'] + mlb_only_cols
    table = _extract_people_table(
        zipfile.ZipFile(io.BytesIO(s))
        ).loc[:, cols_to_keep]

    table.dropna(how='all', subset=mlb_only_cols, inplace=True)  # Keep only the major league rows
    table.reset_index(inplace=True, drop=True)

    table[['key_mlbam', 'key_fangraphs']] = table[['key_mlbam', 'key_fangraphs']].fillna(-1)
    # originally returned as floats which is wrong
    table[['key_mlbam', 'key_fangraphs']] = table[['key_mlbam', 'key_fangraphs']].astype(int)

    # Reorder the columns to the right order
    table = table[cols_to_keep]

    return table

def get_lookup_table(save=False):
    table = chadwick_register(save)
    # make these lowercase to avoid capitalization mistakes when searching
    table['name_last'] = table['name_last'].str.lower()
    table['name_first'] = table['name_first'].str.lower()
    return table

class _PlayerSearchClient:
    def __init__(self) -> None:
        self.table = get_lookup_table()

    def reverse_lookup(self, player_ids: List[str], key_type: str = 'mlbam') -> pd.DataFrame:
        """Retrieve a table of player information given a list of player ids

        :param player_ids: list of player ids
        :type player_ids: list
        :param key_type: name of the key type being looked up (one of "mlbam", "retro", "bbref", or "fangraphs")
        :type key_type: str

        :rtype: :class:`pandas.core.frame.DataFrame`
        """
        key_types = (
            'mlbam',
            'retro',
            'bbref',
            'fangraphs',
        )

        if key_type not in key_types:
            raise ValueError(f'[Key Type: {key_type}] Invalid; Key Type must be one of {key_types}')

        key = f'key_{key_type}'

        results = self.table[self.table[key].isin(player_ids)]
        results = results.reset_index(drop=True)

        return results

def _get_client() -> _PlayerSearchClient:
    global _client
    if _client is None:
        _client = _PlayerSearchClient()
    return _client

def playerid_reverse_lookup(player_ids: List[str], key_type: str = 'mlbam') -> pd.DataFrame:
    """Retrieve a table of player information given a list of player ids

    :param player_ids: list of player ids
    :type player_ids: list
    :param key_type: name of the key type being looked up (one of "mlbam", "retro", "bbref", or "fangraphs")
    :type key_type: str

    :rtype: :class:`pandas.core.frame.DataFrame`
    """
    client = _get_client()
    return client.reverse_lookup(player_ids, key_type)