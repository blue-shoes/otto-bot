import fg_box_scores
import statcast_processing
import const

import os
import datetime
from datetime import timedelta
import json
import pandas as pd
from pandas import DataFrame, Series
import math
from typing import Optional

from pybaseball import pitching_stats_range, batting_stats_range, playerid_reverse_lookup

from pymongo import MongoClient

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu

def get_yesterday_date_str() -> str:
    y_date = datetime.datetime.now() - timedelta(1)
    return y_date.strftime('%Y-%m-%d')

def lambda_handler(event, context):

    if "hist_year" in event:
        hist_year = event['hist_year']
    elif "queryStringParameters" in event:
        hist_year = event['queryStringParameters'].get('hist_year', None)
    else:
        hist_year = None
    
    #if hist_year:
    #    return next_hist_year_date(hist_year)

    if "game_date" in event:
        game_date_str = event['game_date']
    elif "queryStringParameters" in event:
        game_date_str = event["queryStringParameters"].get("game_date", get_yesterday_date_str())
    else:
        game_date_str = get_yesterday_date_str()

    return save_days_stats(game_date_str)

def save_days_stats(game_date_str:str):
    sc_data, batter_data, pitcher_data = load_day_stats(game_date_str)

    inserts = process_data(game_date_str, sc_data, batter_data, pitcher_data)

    return insert_day_stats(inserts, game_date_str)

def insert_day_stats(inserts:dict, game_date_str:str) -> dict:
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

def load_day_stats(game_date_str:str) -> tuple[DataFrame, DataFrame, DataFrame]:
    sc_data = statcast_processing.get_statcast_dataframe(game_date_str)

    pitcher_data = pitching_stats_range(game_date_str)
    pitcher_data = pitcher_data.fillna(0)
    pitcher_data.set_index('mlbID', inplace=True)
    pitcher_data.index = pitcher_data.index.astype('int')
    pitcher_data['P_Points'] = pitcher_data.apply(pitching_points, axis=1)
    pitcher_data['SABR_Points'] = pitcher_data.apply(sabr_points, axis=1)

    batter_data = batting_stats_range(game_date_str)
    batter_data = batter_data.fillna(0)
    batter_data.set_index('mlbID', inplace=True)
    batter_data.index = batter_data.index.astype('int')
    batter_data['H_Points'] = batter_data.apply(hitting_points, axis=1)

    return sc_data, batter_data, pitcher_data

def process_data(game_date_str:str, sc_data:DataFrame, batter_data:DataFrame, pitcher_data:DataFrame) -> list[dict]:
    game_date = datetime.datetime.strptime(game_date_str, '%Y-%m-%d')

    game_pks = sc_data['game_pk'].unique()

    double_headers = find_double_headers(sc_data, game_date_str)
    hold_data = statcast_processing.get_holds(sc_data, pitcher_data)

    inserts = list()

    for game_pk in game_pks:
        game_df = sc_data.loc[sc_data['game_pk'] == game_pk]
        game_df.sort_values(['at_bat_number', 'pitch_number'], ascending=[True, True])

        starters = statcast_processing.get_starters(game_df)
        batting_order_and_finishers = statcast_processing.get_finishers(game_df)
        b_xwoba = statcast_processing.get_game_xwoba(game_df, 'batter')
        p_xwoba = statcast_processing.get_game_xwoba(game_df, 'pitcher')

        away_batter_sp_hand = game_df.loc[game_df['inning_topbot'] == 'Top'].iloc[0]['p_throws']
        home_batter_sp_hand = game_df.loc[game_df['inning_topbot'] == 'Bot'].iloc[0]['p_throws']

        if game_pk in double_headers:
            ids = [int(id) for id in b_xwoba.keys()]
            ids.extend([int(id) for id in p_xwoba.keys()])
            id_map = playerid_reverse_lookup(ids)
            id_map.set_index('key_mlbam', inplace=True)
            id_map.index = id_map.index.astype('int')
            id_map['key_fangraphs'] = id_map['key_fangraphs'].astype('int')
            if double_headers[game_pk][0]:
                insert_date = game_date
            else:
                insert_date = game_date + timedelta(hours=1) 
        else:
            insert_date = game_date

        for pid in game_df['batter'].unique():
            away = game_df.loc[game_df['batter'] == pid].iloc[0]['inning_topbot'] == 'Top'
            if away:
                sp_hand = away_batter_sp_hand
            else:
                sp_hand = home_batter_sp_hand
            if batter_data.loc[pid]['G'] > 1:
                dh_tuple = double_headers[game_pk]
                fg_id = get_fg_id(pid, id_map, dh_tuple[1], batter_data.loc[pid], batting_order_and_finishers[pid][0])
                if fg_id == -1:
                    # No positive match
                    # TODO: Notify somehow?
                    continue
                if dh_tuple[0]:
                    b_series = dh_tuple[1].loc[fg_id]
                else:
                    stat_dict = dict()
                    for cat in dh_tuple[1].columns:
                        if isinstance(dh_tuple[1].loc[fg_id][cat], str):
                            continue
                        if cat not in batter_data.columns:
                            continue
                        stat_dict[cat] = batter_data.loc[pid][cat] - dh_tuple[1].loc[fg_id][cat]
                    b_series = Series(stat_dict)
                b_series['H_Points'] = hitting_points(b_series)
            else:
                b_series = batter_data.loc[pid]
            
            insert = dict()
            insert['metadata'] = {'mlbam_id': pid}
            insert['timestamp'] = insert_date
            insert['PA'] = b_series['PA']
            insert['H_Points'] = b_series['H_Points']
            insert['GS'] = pid in starters
            insert['BO'] = batting_order_and_finishers[pid][0]
            insert['GF'] = batting_order_and_finishers[pid][1]
            insert['H_xwOBA'] = b_xwoba[pid]
            insert['SP_Hand'] = sp_hand

            inserts.append(insert)

        for pid in game_df['pitcher'].unique():
            if pitcher_data.loc[pid]['G'] > 1:
                dh_tuple = double_headers[game_pk]
                fg_id = get_fg_id(pid, id_map, dh_tuple[2], pitcher_data.loc[pid], None)
                if fg_id == -1:
                    # No positive match
                    # TODO: Notify somehow?
                    continue
                if dh_tuple[0]:
                    p_series = dh_tuple[2].loc[fg_id]
                else:
                    stat_dict = dict()
                    for cat in dh_tuple[2].columns:
                        if isinstance(dh_tuple[2].loc[fg_id][cat], str):
                            continue
                        if cat not in pitcher_data.columns:
                            continue
                        stat_dict[cat] = pitcher_data.loc[pid][cat] - dh_tuple[2].loc[fg_id][cat]
                    p_series = Series(stat_dict)
                p_series['P_Points'] = pitching_points(p_series)
                p_series['SABR_Points'] = sabr_points(p_series)
            else:
                p_series = pitcher_data.loc[pid]
            
            insert = dict()
            insert['metadata'] = {'mlbam_id': pid}
            insert['timestamp'] = insert_date
            insert['BF'] = p_series['BF']
            insert['GS'] = p_series['GS']
            insert['IP'] = p_series['IP']
            if p_series['SV'] == 1:
                insert['SV'] = True
            if pid in hold_data and game_pk in hold_data[pid]:
                insert['HLD'] = True
                p_series['P_Points'] = p_series['P_Points'] + 4.0
                p_series['SABR_Points'] = p_series['SABR_Points'] + 4.0
            insert['P_xwOBA'] = p_xwoba[pid]
            insert['P_Points'] = p_series['P_Points']
            insert['SABR_Points'] = p_series['SABR_Points']

            inserts.append(insert)
    return inserts

def hitting_points(row:Series) -> float:
    return -1.0*row['AB'] + 5.6*row['H'] + 2.9*row['2B'] + 5.7*row['3B'] + 9.4*row['HR']+3.0*row['BB']+3.0*row['HBP']+1.9*row['SB']-2.8*row['CS']

def pitching_points(row:Series) -> float:
    ip = (row['IP'] % 1) / 3 + math.floor(row['IP'])
    return 7.4*ip+2.0*row['SO']-2.6*row['H']-3.0*row['BB']-3.0*row['HBP']-12.3*row['HR']+5.0*row['SV']

def sabr_points(row:Series) -> float:
    ip = (row['IP'] % 1) / 3 + math.floor(row['IP'])
    return 5.0*ip+2.0*row['SO']-3.0*row['BB']-3.0*row['HBP']-13.0*row['HR']+5.0*row['SV']

def get_fg_id(mlbam_id: str, id_map: DataFrame, fg_box: DataFrame, day_line:Series, batting_order: Optional[int]) -> int:
    row = id_map.loc[mlbam_id.item()]
    # Fangraphs Id available from chadwick register (>99% use case)
    if row['key_fangraphs'] > 0:
        return row['key_fangraphs']
    
    # Next check Ottobot DB if it's populated (Hopefully >99.9% use case)
    players_col = ottoneu_db.players
    result = players_col.find_one({'mlbam_id': int(mlbam_id)})
    if result and 'fg_majorleagueid' in result:
        return result['fg_majorleagueid']
    
    # Need to match names to get FG Major League Id. Possibly populate db with MLBAM
    unmatched = [id for id in fg_box.index if id not in id_map['key_fangraphs']]
    poss = list()
    for fg_id in unmatched:
        fg_row = fg_box.loc[fg_id]
        if const.normalize(row['name_last']) in const.normalize(fg_row['Name']):
            poss.append(fg_id)
    if not fg_id:
        return -1
    if len(poss) == 1:
        return add_mlbam_id_to_db(mlbam_id, poss[0])
    # Had multiple unmatched last name matches
    first_name = list()
    for fg_id in poss:
        fg_row = fg_box.loc[fg_id]
        if const.normalize(row['name_first']) in const.normalize(fg_row['Name']):
            first_name.append(fg_id)
    if len(first_name) == 1:
        return add_mlbam_id_to_db(mlbam_id, first_name[0])
    if len(first_name) > 1:
        poss = first_name
    # See if we can eliminate by batting order position
    if batting_order:
        bo_match = list()
        for fg_id in poss:
            fg_row = fg_box.loc[fg_id]
            if fg_row['BO'] == batting_order:
                bo_match.append(fg_id)
        if len(bo_match) == 1:
            return add_mlbam_id_to_db(mlbam_id, bo_match[0])
    # Matched on name and batting order position. Try to filter by attainable stat line
    possible_statline = list()
    for fg_id in bo_match:
        possible = True
        for cat in fg_box.columns:
            if cat == 'AVG' or cat == 'ERA':
                continue
            if cat in day_line:
                if float(fg_box.loc[fg_id][cat]) > day_line[cat]:
                    possible = False
                    break
        if possible:
            possible_statline.append(fg_id)
    if len(possible_statline) == 1:
        return add_mlbam_id_to_db(mlbam_id, possible_statline[0])
    # Matched on name and batting order position and couldn't eliminate by statline. Not sure what else we can do at this point. Seems very rare.
    return -1
        
def add_mlbam_id_to_db(mlbam_id:int, fg_id:str) -> str:
    ottoneu_db.players.update_one({'fg_majorleagueid':fg_id}, { '$set': {'mlbam_id': mlbam_id.item()}})
    return fg_id

def find_double_headers(df:DataFrame, game_date:str) -> dict[int, tuple[bool, DataFrame, DataFrame]]:
    '''Returns dict with game_pk key and value of tuple(first game of dh bool, batting df for first game, pitching df for first game)'''
    # Return is dict[game_pk: tuple(first_game?, batting_df_g1, pitching_df_g1)]
    results = dict()
    unique_games = df[['game_pk', 'home_team', 'away_team']].drop_duplicates()
    double_headers = unique_games.loc[unique_games[['home_team', 'away_team']].duplicated(keep=False)]
    if len(double_headers) == 0:
        return results
    
    game_pks = double_headers['game_pk']
    game_summary = dict()
    for gpk in game_pks:
        game_df = df.loc[df['game_pk'] == gpk].sort_values(['at_bat_number', 'pitch_number'], ascending=[True, True])
        home_sp = game_df.iloc[0]['player_name']
        away_sp = game_df.loc[game_df['inning_topbot'] == 'Bot'].iloc[0]['player_name']
        last_row = game_df.tail(1).iloc[0]
        home_team = last_row['home_team']
        if home_team not in game_summary:
            game_summary[home_team] = list()
        game_summary[home_team].append((gpk,last_row['post_away_score'],last_row['post_home_score'], away_sp, home_sp))

    home_teams = double_headers['home_team'].unique()
    for team in home_teams:
        fg_team = const.get_fg_from_sc_team(team)
        url = f'https://www.fangraphs.com/boxscore.aspx?date={game_date}&team={fg_team}&dh=1&season={game_date[:4]}'
        bat_df, pitch_df, score = fg_box_scores.get_game_dfs(url)
        games = game_summary[team]
        if games[0][1] == games[1][1] and games[0][2] == games[1][2]:
            # both DH had same final score
            pitcher_names = [const.normalize(name) for name in pitch_df['Name']]            
            away_sp_ln = const.normalize(games[0][3].split(',')[0])
            home_sp_ln = const.normalize(games[0][4].split(',')[0])
            if [name for name in pitcher_names if away_sp_ln in name] and [name for name in pitcher_names if home_sp_ln in name]:
                results[games[0][0]] = (True, bat_df, pitch_df)
                results[games[1][0]] = (False, bat_df, pitch_df)
            else:
                results[games[0][0]] = (False, bat_df, pitch_df)
                results[games[1][0]] = (True, bat_df, pitch_df)
        else:
            if games[0][1] == score[0] and games[0][2] == score[1]:
                results[games[0][0]] = (True, bat_df, pitch_df)
                results[games[1][0]] = (False, bat_df, pitch_df)
            else:
                results[games[0][0]] = (False, bat_df, pitch_df)
                results[games[1][0]] = (True, bat_df, pitch_df)
    return results