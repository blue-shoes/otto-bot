import re
import pandas as pd
from pandas import DataFrame, Series
from bs4 import BeautifulSoup
import requests
import math

from typing import List, Dict

def get_game_urls(date:str) -> List[str]:
    url = f'https://www.fangraphs.com/scoreboard.aspx?date={date}'
    print(f'url = {url}')
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    box_scores = soup.find_all(lambda tag: tag.name == 'a' and 'Box Score' in tag.text)
    urls = list()
    for tag in box_scores:
        urls.append(f'https://www.fangraphs.com/{tag.get("href")}')
    
    return urls

def get_game_dfs(url:str) -> tuple[DataFrame, DataFrame]:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    return parse_batter_box(soup), parse_pitcher_box(soup)

def parse_pitcher_box(soup:BeautifulSoup) -> DataFrame:
    h_df = get_box_df(soup, 'WinsBox1_dg2hp')
    v_df = get_box_df(soup, 'WinsBox1_dg2ap')

    box = pd.concat([h_df, v_df])

    box['P_Points'] = box.apply(pitching_points, axis=1)
    box['SABR_Points'] = box.apply(sabr_points, axis=1)

    return box[['GS', 'IP', 'P_Points', 'SABR_Points']]

def parse_batter_box(soup:BeautifulSoup) -> DataFrame:
    h_starts = starts = get_pos_starts(soup, 'WinsBox1_dghb')
    v_starts = get_pos_starts(soup, 'WinsBox1_dgab')

    starts.update(v_starts)

    h_df = get_box_df(soup, 'WinsBox1_dg2hb')
    v_df = get_box_df(soup, 'WinsBox1_dg2ab')

    box = pd.concat([h_df, v_df])
    box['Start'] = pd.Series(starts)

    box['H_Points'] = box.apply(hitting_points, axis=1)

    return box[['PA', 'Start', 'H_Points']]

def hitting_points(row:Series) -> float:
    return -1.0*row['AB'] + 5.6*row['H'] + 2.9*row['2B'] + 5.7*row['3B'] + 9.4*row['HR']+3.0*row['BB']+3.0*row['HBP']+1.9*row['SB']-2.8*row['CS']

def pitching_points(row:Series) -> float:
    ip = (row['IP'] % 1) / 3 + math.floor(row['IP'])
    return 7.4*ip+2.0*row['SO']-2.6*row['H']-3.0*row['BB']-3.0*row['HBP']-12.3*row['HR']+5.0*row['SV']+4.0*row['HLD']

def sabr_points(row:Series) -> float:
    ip = (row['IP'] % 1) / 3 + math.floor(row['IP'])
    return 5.0*ip+2.0*row['SO']-3.0*row['BB']-3.0*row['HBP']-13.0*row['HR']+5.0*row['SV']+4.0*row['HLD']

def get_box_df(soup:BeautifulSoup, div_id:str) -> DataFrame:
    table = soup.find('div', {'id': div_id})
    header_cols = table.find('thead').find('tr').find_all('th')
    cols = [th.contents[0].strip() for th in header_cols]
    rows = list()
    for row in table.find('tbody').find_all('tr'):
        tds = row.find_all('td')
        stats = list()
        link = tds[0].find('a')
        if link:
            stats.append(get_fg_id_from_url(link.get('href')))
            for stat in tds[1:]:
                if stat.find('b'):
                    stats.append(float(stat.find('b').contents[0].strip()))
                else:
                    stats.append(float(stat.contents[0].strip()))
            rows.append(stats)
    df = DataFrame(rows)
    df.columns = cols
    df.set_index(cols[0], inplace=True)
    return df

def get_pos_starts(soup:BeautifulSoup, div_id:str) -> Dict[str, bool]:
    table = soup.find('div', {'id': div_id})
    rows = table.find('tbody').find_all('tr')
    last_order = '0'
    results_dict = dict()
    for row in rows:
        tds = row.find_all('td')
        link = tds[0].find('a')
        if link:
            fg_url = link.get('href')
            fg_id = get_fg_id_from_url(fg_url)
            bo = tds[1].text
            results_dict[fg_id] = (bo != last_order)
            last_order = bo
    return results_dict

def get_fg_id_from_url(url:str) -> str:
    try:
        return re.findall('.*playerid=(\\d+).*', url)[0]
    except IndexError:
        return '0'

def main():
    url = 'https://www.fangraphs.com/boxscore.aspx?date=2024-07-29&team=Orioles&dh=1&season=2024'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    b_box = parse_batter_box(soup)
    p_box = parse_pitcher_box(soup)

    print(b_box.head())
    print(p_box.head())

if __name__ == '__main__':
    main()