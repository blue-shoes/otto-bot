import json
from pymongo import MongoClient, UpdateOne
import os
import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup as Soup
from io import StringIO
import datetime
from typing import Any

client = MongoClient(host=os.environ.get('ATLAS_URI'))
ottoneu_db = client.ottoneu

lambda_client = boto3.client('lambda')


def lambda_handler(event, context):
    print(event)

    if 'body' in event['Records'][0]:
        print('SQS message')
        msg_map = json.loads(event['Records'][0]['body'])
    else:
        print('SNS message')
        message = event['Records'][0]['Sns']['Message']
        msg_map = json.loads(json.loads(message)['default'])

    print(msg_map)

    if not msg_map or not msg_map.get('league_ids', None):
        return {
            'statusCode': 400,
            'body': json.dumps('league_ids not present in event'),
        }

    leagues_col = ottoneu_db.leagues

    update_leagues = list()

    print(f'League subset: {len(msg_map["league_ids"])}')

    for li in msg_map['league_ids']:
        # print(f'Getting league_id: {league_id}' )
        try:
            league_id = li[0]
            league_dict = dict()
            rosters, teams = get_league_dict(league_id)
            if not rosters:
                print(f'!!League_id {league_id} not valid')
                continue

            league_dict['rosters'] = rosters
            league_dict['teams'] = teams

            league_dict['Last Updated'] = datetime.datetime.now()
            league_dict['format'] = get_scoring_format(li[1])
            league_dict['format_name'] = li[1]

            update_leagues.append(UpdateOne({'_id': league_id}, {'$set': league_dict}, upsert=True))
        except Exception as e:
            print(f'Exception for league {league_id}')
            print(e)

    if update_leagues:
        try:
            leagues_col.bulk_write(update_leagues, ordered=False)
        except Exception as e:
            print(e)
            return {'statusCode': 500, 'body': json.dumps('Error writing to db.')}

    return {'statusCode': 200}


def get_scoring_format(format_name: str) -> int:
    if format_name == 'Ottoneu Classic (4x4)':
        return 1
    if format_name == 'Old School (5x5)':
        return 2
    if format_name == 'FanGraphs Points':
        return 3
    if format_name == 'SABR Points':
        return 4
    if format_name == 'H2H FanGraphs Points':
        return 5
    if format_name == 'H2H SABR Points':
        return 6
    raise AttributeError(f'The format name {format_name} was not recognized.')


def get_league_dict(lg_id: str) -> tuple[dict[str, Any], dict[int, str]]:
    """Scrapes the /rosterexport page for a league (in csv format) and returns a DataFrame of the information. Index is Ottoneu Id"""
    if lg_id:
        roster_export_url = f'https://ottoneu.fangraphs.com/{lg_id}/rosterexport'
        response = requests.get(roster_export_url)

        rost_soup = Soup(response.text, 'html.parser')
        df = pd.read_csv(StringIO(rost_soup.contents[0]))
        df = df[df['Salary'].notna()]
        r_df = df[['ottoneu ID', 'TeamID', 'Salary']]
        r_df.set_index('ottoneu ID', inplace=True)
        r_df.index = r_df.index.astype(str, copy=False)
        rosters = r_df.to_dict('index')
        t_df = df[['TeamID', 'Team Name']].drop_duplicates()
        t_df.set_index('TeamID', inplace=True)
        t_df.index = t_df.index.astype(str, copy=False)
        teams = t_df.to_dict('index')
        return rosters, teams

    return dict(), dict()
