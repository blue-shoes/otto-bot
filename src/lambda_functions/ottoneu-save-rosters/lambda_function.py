import json
from pymongo import MongoClient, UpdateOne
import os
import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup as Soup
from io import StringIO
import datetime

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    print(event)
    
    if event.get('Records', None):
        msg_map = list()
        for record in event['Records']:
            msg_map.append(json.loads(record['body']))
    else:
        msg_map = [event]
    
    print(msg_map)
    
    if not msg_map or not msg_map[0].get('league_ids', None):
        return {
            'statusCode': 400,
            'body': json.dumps('league_ids not present in event')
        }
    league_ids = [li for lis in msg_map for li in lis['league_ids']]
    
    leagues_col = ottoneu_db.leagues

    update_leagues = list()


    print(f'League subset: {len(league_ids)}')

    for league_id in league_ids:
        #print(f'Getting league_id: {league_id}' )
        try:
            league_dict = dict()
            league_dict['rosters'] = get_league_dict(league_id)
            
            league_dict['Last Updated'] = datetime.datetime.now()
                
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
        df = df[df['Salary'].notna()]
        df = df[['ottoneu ID', 'TeamID', 'Team Name', 'Salary']]
        df.set_index("ottoneu ID", inplace=True)
        df.index = df.index.astype(str, copy = False)
        return df.to_dict('index')
    
    return dict()