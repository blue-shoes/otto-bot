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
    if response.status_code == 500:
        #No games for date
        print(f'No games scheduled for {date}')
        return list()
    soup = BeautifulSoup(response.text, 'html.parser')
    box_scores = soup.find_all(lambda tag: tag.name == 'a' and 'Box Score' in tag.text)
    urls = list()
    for tag in box_scores:
        urls.append(f'https://www.fangraphs.com/{tag.get("href")}')
    
    return urls

def get_game_dfs(url:str) -> tuple[DataFrame, DataFrame, tuple[int, int]]:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    batter_box, score = parse_batter_box(soup)
    return batter_box, parse_pitcher_box(soup), score

def parse_pitcher_box(soup:BeautifulSoup) -> DataFrame:
    h_df = get_box_df(soup, 'WinsBox1_dg2hp')
    v_df = get_box_df(soup, 'WinsBox1_dg2ap')

    p_df = pd.concat([h_df, v_df])
    p_df.rename(columns={'TBF':'BF'})

    return 

def parse_batter_box(soup:BeautifulSoup) -> tuple[DataFrame, tuple[int, int]]:
    h_df = get_box_df(soup, 'WinsBox1_dg2hb')
    h_bo = get_bo_df(soup, 'WinsBox1_dghb_ctl00')
    h_df = pd.concat([h_df, h_bo])
    h_runs = h_df['R'].sum()

    v_df = get_box_df(soup, 'WinsBox1_dg2ab')
    v_bo = get_bo_df(soup, 'WinsBox1_dgab_ctl00')
    v_df = pd.concat([v_df, v_bo])
    v_runs = v_df['R'].sum()

    return pd.concat([h_df, v_df]), (v_runs, h_runs)

def parse_final_score(soup:BeautifulSoup) -> tuple[int, int]:
    away = int(soup.find('tr', {'class': 'team away'}).find('td', {'class': 'runs'}).contents[0].strip())
    home = int(soup.find('tr', {'class': 'team home'}).find('td', {'class': 'runs'}).contents[0].strip())
    return (away, home)

def get_box_df(soup:BeautifulSoup, div_id:str) -> DataFrame:
    table = soup.find('div', {'id': div_id})
    header_cols = table.find('thead').find('tr').find_all('th')
    cols = []
    cols.append('fg_id')
    cols.extend([th.contents[0].strip() for th in header_cols])
    rows = list()
    for row in table.find('tbody').find_all('tr'):
        tds = row.find_all('td')
        stats = list()
        link = tds[0].find('a')
        if link:
            stats.append(get_fg_id_from_url(link.get('href')))
            stats.append(link.contents[0].strip())
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

def get_bo_df(soup:BeautifulSoup, div_id:str) -> Dict[str, int]:
    table = soup.find('div', {'id': div_id})
    rows = table.find('tbody').find_all('tr')
    results = list()
    for row in rows:
        tds = row.find_all('td')
        link = tds[0].find('a')
        if link:
            fg_url = link.get('href')
            fg_id = get_fg_id_from_url(fg_url)
            bo = tds[1].text
            results.append(fg_id)
            results.append(int(bo))
    df = DataFrame(results)
    df.columns = ['fg_id', 'BO']
    df.set_index('fg_id', inplace=True)
    return df

def get_fg_id_from_url(url:str) -> str:
    try:
        return re.findall('.*playerid=(\\d+).*', url)[0]
    except IndexError:
        return '0'

def main():
    url = 'https://www.fangraphs.com/boxscore.aspx?date=2024-08-07&team=Guardians&dh=1&season=2024'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    b_box, score = parse_batter_box(soup)
    p_box = parse_pitcher_box(soup)

    print(b_box.head())
    print(p_box.head())

    print(score)

if __name__ == '__main__':
    main()