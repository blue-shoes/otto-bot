import json
import pandas as pd
import requests
from bs4 import BeautifulSoup as Soup
from io import StringIO

def lambda_handler(event, context):
    ''''Scrapes the /rosterexport page for a league (in csv format) and returns a DataFrame of the information. Index is Ottoneu Id'''
    try:
        if "league_id" in event:
            lg_id = event['league_id']
        else:
            lg_id = event["queryStringParameters"].get("league_id", None)
    except KeyError:
        return {
            'statusCode': 400,
            'body': json.dumps('Body does not have search_name parameter.')
        }
    if not lg_id:
        return {
            'statusCode': 400,
            'body': json.dumps('Empty string passed as search name')
        }
    
    roster_export_url = f'https://ottoneu.fangraphs.com/{lg_id}/rosterexport'
    response = requests.get(roster_export_url)
    
    rost_soup = Soup(response.text, 'html.parser')
    df = pd.read_csv(StringIO(rost_soup.contents[0]))
    df = df[df['Salary'].notna()]
    df = df[['ottoneu ID', 'TeamID', 'Team Name', 'Salary']]
    df.set_index("ottoneu ID", inplace=True)
    df.index = df.index.astype(str, copy = False)
    print(df.head())

    roster_dict = df.to_dict('index')

    return {
        'statusCode': 200,
        'body': json.dumps(roster_dict)
    }