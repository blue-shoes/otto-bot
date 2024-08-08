import fg_box_scores
import os
import datetime
from datetime import timedelta
import json
from pandas import DataFrame
from time import sleep

from pymongo import MongoClient

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

def get_yesterday_date_str() -> str:
    y_date = datetime.datetime.now() - timedelta(1)
    return y_date.strftime('%Y-%m-%d')

def lambda_handler(event, context):

    if "game_date" in event:
        game_date_str = event['game_date']
    elif "queryStringParameters" in event:
        game_date_str = event["queryStringParameters"].get("game_date", get_yesterday_date_str())
    else:
        game_date_str = get_yesterday_date_str()

    try:
        game_urls = fg_box_scores.get_game_urls(game_date_str)

        if not game_urls:
            return {
                'statusCode': 400,
                'body': json.dumps(f'Could not retrieve games for {game_date_str}')
            }
    except Exception as e:
        print(e)
        return {
                'statusCode': 400,
                'body': json.dumps(f'Could not retrieve games for {game_date_str}')
            }

    game_date = datetime.datetime.strptime(game_date_str, '%Y-%m-%d')

    inserts = list()

    for game_url in game_urls:
        sleep(5)
        try:
            if 'dh=2' in game_url:
                insert_date = game_date + timedelta(hours=1) 
            else:
                insert_date = game_date
            h_df, p_df = fg_box_scores.get_game_dfs(game_url)
            inserts.extend(get_player_inserts(h_df, insert_date))
            inserts.extend(get_player_inserts(p_df, insert_date))
        except Exception as e:
            print(f'Could not get box scores for {game_url}')
            print(e)

    if not inserts:
        print(f'No games to upload for {game_date_str}')
        return {
            'statusCode': 200,
            'body': json.dumps('No games to upload')
        }

    stats_col = ottoneu_db.point_stats

    try:
        stats_col.insert_many(inserts)
    except Exception as e:
        print(e)
        return {
            'statusCode': 400,
            'body': json.dumps('Error uploading games to database')
        }

    return {
        'statusCode': 200
    }

def get_player_inserts(df:DataFrame, insert_date:datetime) -> list[dict]:
    inserts = list()
    for idx, row in df.iterrows():
        player_dict = row.to_dict()
        player_dict['metadata'] = {'fg_id': idx}
        player_dict['timestamp'] = insert_date
        inserts.append(player_dict)
    return inserts