import re
import pandas as pd
from pandas import DataFrame, Series
from pybaseball import statcast, pitching_stats_range, batting_stats_range

def get_starters(df:DataFrame) -> list[int]:
    '''Returns list of mlbam ids that were starters for the given game'''
    home_start = set()
    visit_start = set()
    for _, row in df.iterrows():
        if not home_start:
            home_start.update([row[f'fielder_{pos}'] for pos in range(2,10)])
        if len(visit_start) < 9:
            if row['inning_topbot'] == 'top':
                visit_start.add(row['batter'])
            else:
                visit_start.update([row[f'fielder_{pos}'] for pos in range(2,10)])
        if len(home_start) < 9 and row['inning_topbot'] == 'bot':
            home_start.add(row['batter'])
        if len(visit_start) >= 9 and len(home_start) >= 9:
            # Could technically be greater than 9 if prior to filling the starter list (which would be prior to turning the batting order over)
            # the visiting team made multiple defensive replacements. Very low likelihood.
            break

    starters = list()
    starters.extend(home_start)
    starters.extend(visit_start)

    return starters

def get_finishers(df:DataFrame) -> dict[int , tuple[int, bool]]:
    '''Returns dictionary with mlbam id key and tuple of batting order position and boolean True if finished game, False if not value '''
    finishers = dict()

    finishers.update(get_bo_and_finishers(df.loc[df['inning_topbot'] == 'Top']))
    finishers.update(get_bo_and_finishers(df.loc[df['inning_topbot'] == 'Bot']))
    
    return finishers

def get_bo_and_finishers(df:DataFrame) -> dict[int, tuple[int, bool]]:
    '''Returns dict of batters with mlbam id key and tuple of batting order position and boolean True if finished game, False if not value'''
    team_df = df[['at_bat_number','batter']].drop_duplicates()
    team_df['Team_PA_Num'] = team_df['at_bat_number'].rank()
    team_df['BO'] = team_df['Team_PA_Num'] % 9

    # We are going to say the last player to bat in each batting order position "finished" the game, which is not technically correct,
    # but probably close enough for fantasy purposes    
    finishers = team_df.tail(9)['batter'].unique()
    pids = team_df['batter'].unique()

    b_a_f = dict()
    for pid in pids:
        bo = int(team_df.loc[team_df['batter'] == pid].iloc[0]['BO'])
        b_a_f[pid] = (bo, pid in finishers)

    return b_a_f

def get_fielder_ids(pa: Series) -> list[int]:
    return [pa[f'fielder_{pos}'] for pos in range(2,9)]

def get_holds(df:DataFrame, pitchers_df:DataFrame) -> dict[int, list[int]]:
    '''Returns dict with mlbam id as key and list of game_pks where player accumulated a hold'''
    p_with_holds = dict()
    for pid in pitchers_df.index:
        p_df = df.loc[df['pitcher'] == pid]

        game_pks = p_df['game_pk'].unique()
        hold_game_pk = list()
        for game_pk in game_pks:
            g_df = p_df.loc[p_df['game_pk'] == game_pk]
            g_df = g_df.sort_values(['at_bat_number', 'pitch_number'], ascending=[True, True])
            i_row = g_df.iloc[0]
            f_row = g_df.iloc[-1]

            if i_row['inning'] <= 5:
                # "Stats provider BIS [now SIS] "will not award a hold or a blown save to a pitcher who enters a game in the fifth inning or earlier"
                # https://tht.fangraphs.com/the-unofficial-rules-of-holds-and-blown-saves/
                continue
            if i_row['fld_score'] <= i_row['bat_score']:
                # Not winning when entered
                continue
            if f_row['post_fld_score'] <= f_row['post_bat_score']:
                # Not winning when exiting
                continue
            if i_row['inning'] == f_row['inning']:
                if i_row['outs_when_up'] == f_row['outs_when_up']:
                    if 'out' not in str(f_row['events']): 
                        if not ('sac_' in str(f_row['events']) and 'out' in str(f_row['des'])):
                            # Did not record out
                            continue
            score_diff = i_row['fld_score'] - i_row['bat_score']
            if score_diff > 3:
                on_base = 0
                if not pd.isna(i_row['on_1b']):
                    on_base += 1
                if not pd.isna(i_row['on_2b']):
                    on_base += 1
                if not pd.isna(i_row['on_3b']):
                    on_base += 1
                if score_diff > (on_base + 2):
                    # Not a save situation
                    continue
            if f_row['at_bat_number'] == max(df['at_bat_number'].unique()):
                # Last batter of game; must be save or walk-off
                continue
            lost_lead = False
            for _, pitch in g_df.iterrows():
                if pitch['post_fld_score'] <= pitch['post_bat_score']:
                    # Lost and regained lead, eligible for win, not hold
                    lost_lead = True
                    break
            if lost_lead:
                continue
            hold_game_pk.append(game_pk)
        if pitchers_df.loc[pid]['W'] > 0:
            if pitchers_df.loc[pid]['W'] >= len(hold_game_pk):
                hold_game_pk.clear()
            else:
                hold_game_pk.remove(hold_game_pk[0])
        if not hold_game_pk:
            continue
        p_with_holds[pid] = hold_game_pk     
        
    return p_with_holds

def get_game_xwoba(df:DataFrame, filter_col:str) -> dict[int, float]:
    '''Returns dict with mlbam id key and xwOBA value'''
    player_ids = df[filter_col].unique()
    events = df.loc[df['estimated_woba_using_speedangle'].notna()]
    xwoba_dict = dict()
    for pid in player_ids:
        p_df = events.loc[events[filter_col] == pid]
        if p_df.empty:
            xwoba_dict[pid] = 0.0
        else:
            xwoba_dict[pid] = p_df['estimated_woba_using_speedangle'].mean().item()
    return xwoba_dict

def get_statcast_dataframe(game_date:str, end_date:str=None) -> DataFrame:
    df = statcast(game_date, end_date)
    df[['batter', 'pitcher', 'on_1b', 'on_2b', 'on_3b','fielder_2','fielder_3','fielder_4','fielder_5','fielder_6','fielder_7','fielder_8','fielder_9']] = df[['batter', 'pitcher', 'on_1b', 'on_2b', 'on_3b','fielder_2','fielder_3','fielder_4','fielder_5','fielder_6','fielder_7','fielder_8','fielder_9']].apply(pd.to_numeric)
    return df[['game_pk','game_date','player_name', 'batter', 'pitcher', 'events', 'des', 'p_throws', 'home_team', 'away_team', 'on_1b', 'on_2b', 'on_3b', 'outs_when_up', 'inning', 'inning_topbot', 
             'fielder_2','fielder_3','fielder_4','fielder_5','fielder_6','fielder_7','fielder_8','fielder_9', 'estimated_woba_using_speedangle','at_bat_number','pitch_number',
             'away_score', 'home_score', 'fld_score', 'bat_score','post_away_score', 'post_home_score', 'post_fld_score', 'post_bat_score']]
