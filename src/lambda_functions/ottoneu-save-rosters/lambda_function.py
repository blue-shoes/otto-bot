import json
from pymongo import MongoClient, UpdateOne
import os
import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup as Soup
from io import StringIO

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # TODO implement

    print(event)
    
    if event.get('Records', None):
        msg_map = json.loads(event['Records'][0]['body'])
    else:
        msg_map = event
    
    print(msg_map)
    
    if not msg_map.get('league_ids', None):
        return {
            'statusCode': 400,
            'body': json.dumps('league_ids not present in event')
        }
    league_ids = msg_map['league_ids']
    
    leagues_col = ottoneu_db.leagues

    update_leagues = list()

    print(f'All leagues: {league_ids.length}')

    #league_ids = ['1', '3', '13', '15', '16', '26', '28', '32']

    print(f'League subset: {len(league_ids)}')

    for league_id in league_ids:
        #print(f'Getting league_id: {league_id}' )
        try:
            league_dict = get_league_dict(league_id)
                
            if not league_dict:
                print(f'!!League_id {league_id} not valid')
                continue
            
            #print(f'Roster length {len(league_dict)}')
            
            update_leagues.append(UpdateOne({'_id': league_id},  {'$set': league_dict}, upsert=True))
        except Exception as e:
            print(f'Exception for league {league_id}')
            print(e)

    if update_leagues:
        try:
            leagues_col.bulk_write(update_leagues, ordered=False)
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'body': json.dumps('Error writing to db.')
            }

    return {
        'statusCode': 200
    }

def get_league_dict(lg_id: str):
    '''Scrapes the /rosterexport page for a league (in csv format) and returns a DataFrame of the information. Index is Ottoneu Id'''
    if lg_id:
        roster_export_url = f'https://ottoneu.fangraphs.com/{lg_id}/rosterexport'
        response = requests.get(roster_export_url)
        
        rost_soup = Soup(response.text, 'html.parser')
        df = pd.read_csv(StringIO(rost_soup.contents[0]))
        df.set_index("ottoneu ID", inplace=True)
        df.index = df.index.astype(str, copy = False)
        return df.to_dict('index')
    
    return dict()