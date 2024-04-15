import os
from datetime import datetime
import json
import pandas as pd
import requests
import unidecode
import warnings
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import ast
warnings.filterwarnings('ignore')

class Dataset:
    def __init__(self, start_year:int=2007, end_year:int=datetime.now().year):
        self.start_year = start_year
        self.end_year = end_year
        self.settings = self.load_settings()
        self.contracts = self.load_contracts()
        self.players = self.load_players()
        self.stats = self.load_stats()
        self.dataset = self.merge_data()

    def load_settings(self):
        with open('code/settings.json') as f:
            return json.load(f)
        
    def load_contracts(self):
        # check if already exists
        if os.path.exists('temp/contracts.csv'):
            contracts = pd.read_csv('temp/contracts.csv')
            contracts.columns = ['playerName','-','age','position','team','date','structure','extension','type','term','value','caphit']
            contracts = contracts.drop(columns=['-'])
            contracts['position'] = contracts['position'].str.split(', ').str[0]
            contracts['position'] = contracts['position'].str.split('/').str[0]
            contracts['date'] = pd.to_datetime(contracts['date'])
            contracts['structure'] = contracts['structure'].apply(lambda x: 'ELC' if 'ELC' in x else 'RFA' if 'RFA' in x else 'UFA')
            contracts['extension'] = contracts['extension'].str.replace('✔','1').fillna(0).astype(int)
            contracts['type'] = contracts['type'].str.split('-').str[0].astype(int)
            contracts['value'] = contracts['value'].str.replace('$','').str.replace(',','').astype(float)
            contracts['caphit'] = contracts['caphit'].str.replace('$','').str.replace(',','').astype(float)
            print(f'{len(contracts)} contracts found')
            c = contracts[contracts['structure'] != 'ELC']
            print(f'{len(c)} contracts found after removing ELCs')
            c = c[c['extension'] != 1].reset_index(drop=True)
            print(f'{len(c)} contracts found after removing extensions')
            c.to_csv('temp/cleaned_contracts.csv',index=False)
            return c
        teams = ['ducks','coyotes','bruins','sabres','flames','hurricanes','blackhawks','avalanche','bluejackets','stars','redwings','oilers','panthers','kings','wild','canadiens','predators','devils','islanders','rangers','senators','flyers','penguins','sharks','blues','lightning','mapleleafs','canucks','goldenknights','capitals','jets']
        years = list(range(self.start_year,self.end_year+1))
        contracts = []
        for team in teams:
            for year in years:
                try:
                    print(f'Getting contracts for {team} in {year}{year+1}',end='\r')
                    url = f'https://www.capfriendly.com/ajax/signings/{team}/all/all/1-15/0-15000000/0101{year}-0101{year+1}'

                    l = 50
                    p = 1

                    while l == 50:
                        r = requests.get(url+f'?pc={p}')
                        data = r.json()
                        data = data['data']['html']
                        table = f'<table>{data}</table>'
                        df = pd.read_html(table)[0]
                        l = len(df)
                        p += 1
                        contracts.append(df)
                except:
                    print(f'No contracts found for {team} in {year}{year+1}')
                    pass
        contracts = pd.concat(contracts)
        contracts.to_csv('temp/contracts.csv',index=False)

        contracts.columns = ['playerName','-','age','position','team','date','structure','extension','type','term','value','caphit']
        contracts = contracts.drop(columns=['-'])
        contracts['position'] = contracts['position'].str.split(', ').str[0]
        contracts['position'] = contracts['position'].str.split('/').str[0]
        contracts['date'] = pd.to_datetime(contracts['date'])
        contracts['structure'] = contracts['structure'].apply(lambda x: 'ELC' if 'ELC' in x else 'RFA' if 'RFA' in x else 'UFA')
        contracts['extension'] = contracts['extension'].str.replace('✔','1').fillna(0).astype(int)
        contracts['type'] = contracts['type'].str.split('-').str[0].astype(int)
        contracts['value'] = contracts['value'].str.replace('$','').str.replace(',','').astype(float)
        contracts['caphit'] = contracts['caphit'].str.replace('$','').str.replace(',','').astype(float)
        print(f'{len(contracts)} contracts found')
        c = contracts[contracts['structure'] != 'ELC']
        print(f'{len(c)} contracts found after removing ELCs')
        c = c[c['extension'] != 1].reset_index(drop=True)
        print(f'{len(c)} contracts found after removing extensions')
        c.to_csv('temp/cleaned_contracts.csv',index=False)
        return c

    def load_players(self):
        all_players = []

        # check if already exists
        if os.path.exists('temp/players.csv'):
            players = pd.read_csv('temp/players.csv')
            print(f'{len(players)} players found')
            return players

        for year in range(self.start_year, self.end_year):
            season_id = f'{year}{year+1}'
            print(f'Getting players for {season_id}...', end='\r')

            teams = requests.get(f'https://api-web.nhle.com/v1/standings/{season_id[:4]}-04-01').json()
            teams = teams['standings']

            for team in teams:
                roster = requests.get(f'https://api-web.nhle.com/v1/roster/{team["teamAbbrev"]["default"]}/{season_id}')
                if roster.status_code == 200:
                    roster = roster.json()
                else:
                    print(f'No roster found for {team["teamAbbrev"]["default"]} in {season_id}')
                    roster = None
                if roster:
                    players = roster['forwards'] + roster['defensemen'] + roster['goalies']
                    all_players += players
                elif team['teamAbbrev']['default'] == 'ATL':
                    roster = requests.get(f'https://api-web.nhle.com/v1/roster/WPG/{season_id}').json()
                    players = roster['forwards'] + roster['defensemen'] + roster['goalies']
                    print(f'Found {len(players)} players for WPG instead of ATL in {season_id}')
                    all_players += players
                elif team['teamAbbrev']['default'] == 'PHX':
                    roster = requests.get(f'https://api-web.nhle.com/v1/roster/ARI/{season_id}').json()
                    players = roster['forwards'] + roster['defensemen'] + roster['goalies']
                    print(f'Found {len(players)} players for ARI instead of PHX in {season_id}')
                    all_players += players
                
        for team in teams:
            prospects = requests.get(f'https://api-web.nhle.com/v1/prospects/{team["teamAbbrev"]["default"]}').json()
            if prospects:
                prospects = prospects['forwards'] + prospects['defensemen'] + prospects['goalies']
                all_players += prospects
        
        player_ids = []
        formatted_players = []
        for player in all_players:
            if player['id'] not in player_ids:
                player_ids.append(player['id'])
                formatted_players.append({
                    'id': player['id'],
                    'firstName': player['firstName']['default'],
                    'lastName': player['lastName']['default'],
                    'playerName': f'{player["firstName"]["default"]} {player["lastName"]["default"]}'
                })
        players = pd.DataFrame(formatted_players)
        players.to_csv('temp/players.csv',index=False)
        print(f'{len(players)} players found')
        return players

    def load_stats(self):
        
        # check if already exists
        if os.path.exists('temp/stats.csv'):
            return pd.read_csv('temp/stats.csv')

        if os.path.exists('eh-stats'):            
            regular_stats = pd.read_csv('eh-stats/EH_std_sk_stats_all_regular_no_adj_2024-04-14.csv')
            gar_stats = pd.read_csv('eh-stats/EH_gar_sk_stats_regular_2024-04-14.csv')
            rapm_stats = pd.read_csv('eh-stats/EH_rapm_sk_stats_ev_regular_2024-04-14.csv')

            regular_stats['Season'] = "20" + regular_stats['Season'].astype(str).str.replace('-',"20")
            regular_stats = regular_stats[['Player', 'Season', 'GP', 'TOI', 'G/60', 'A1/60',
                'A2/60', 'Points/60', 'iSF/60', 'iFF/60', 'iCF/60', 'ixG/60', 'Sh%',
                'FSh%', 'xFSh%', 'iBLK/60', 'GIVE/60', 'TAKE/60', 'iHF/60', 'iHA/60',
                'iPENT2/60', 'iPEND2/60', 'iPENT5/60', 'iPEND5/60', 'iPEN±/60',
                'FOW/60', 'FOL/60', 'FO±/60']]

            gar_stats['Season'] = "20" + gar_stats['Season'].astype(str).str.replace('-',"20")
            gar_stats = gar_stats.drop(columns=['Team','Position','GP','TOI_All'])

            rapm_stats['Season'] = "20" + rapm_stats['Season'].astype(str).str.replace('-',"20")
            rapm_stats = rapm_stats.drop(columns=['Team','Position','GP','TOI'])

            data = regular_stats.merge(gar_stats,how='left',on=['Player','Season']).merge(rapm_stats,how='left',on=['Player','Season'])

            data.to_csv('temp/stats.csv',index=False)
        
            return data

    def merge_data(self):
        dataset = pd.DataFrame()

        contracts = self.contracts
        players = self.players
        stats = self.stats

        players['playerName'] = players['playerName'].apply(lambda x: unidecode.unidecode(x))
        players['playerName'] = players['playerName'].str.replace('.','')
        contracts['playerName'] = contracts['playerName'].apply(lambda x: unidecode.unidecode(x))
        contracts['playerName'] = contracts['playerName'].str.replace('.','')

        matches = pd.merge(contracts,players,on='playerName',how='inner')
        missing = contracts[~contracts['playerName'].isin(matches['playerName'])]
        matches = matches.drop(columns=['firstName','lastName'])
        print(f'{round((len(missing)/(len(missing)+len(matches)))*100,2)}% of players are missing an id')
        if self.settings['manual_match_players']:
            matches2 = self.manual_match_players(missing)
            matches2 = matches2[['playerName','playerId']]
            matches2.columns = ['playerName','id']
            missing = pd.merge(missing,matches2,on='playerName',how='left')
            missing = missing.dropna(subset=['id'])
            matches = pd.concat([matches,missing])

        else:
            print('Manual matching is disabled, skipping...')
        
        print(f'{len(matches)} eligible contracts found')
        
        # Check if player info is already saved
        if os.path.exists('temp/player_info.csv'):
            player_info = pd.read_csv('temp/player_info.csv')
        else:
            player_ids = matches[['id']].drop_duplicates()
            player_ids = player_ids.reset_index(drop=True)
            print(f'{len(player_ids)} unique player ids found')
            player_ids = [{'index': i, 'id': player_ids['id'].iloc[i]} for i in range(len(player_ids))]
            with ThreadPoolExecutor(max_workers=20) as executor:
                results = executor.map(self.get_player_info,player_ids)
            
            player_info = []
            for result in results:
                player_info.append(result)
            
            player_info = pd.DataFrame(player_info)
            player_info.to_csv('temp/player_info.csv',index=False)

        all_player_info = player_info.copy()
        player_info = player_info[['playerId','heightInInches','weightInPounds','birthDate','birthCountry','draftDetails']]
        player_info['birthDate'] = pd.to_datetime(player_info['birthDate'])
        player_info['draftDetails'] = player_info['draftDetails'].fillna('{}').apply(ast.literal_eval)
        player_info['draftYear'] = player_info['draftDetails'].apply(lambda x: x.get('year',np.nan))
        player_info['draftRound'] = player_info['draftDetails'].apply(lambda x: x.get('round',np.nan))
        player_info = player_info.drop(columns=['draftDetails'])
        player_info.columns = ['id','height','weight','birthDate','birthCountry','draftYear','draftRound']

        matches = pd.merge(matches,player_info,on='id',how='left')
        matches['id'] = matches['id'].astype(int)
        print(f'{len(matches)} contracts found after merging player info')
        
        skaters = matches[matches['position'] != 'G']
        goalies = matches[matches['position'] == 'G']
        print(f'{len(skaters)} skater contracts found')
        print(f'{len(goalies)} goalie contracts found')

        # Merge stats
        skaters['season'] = skaters['date'].apply(lambda x: f'{x.year-1}{x.year}' if x.month < 7 else f'{x.year}{x.year+1}')
        skaters['age'] = ((skaters['date'] - skaters['birthDate']).dt.days/365).round(2)
        skaters['position'] = skaters['position'].apply(lambda x: 'C' if x in ['C'] else 'W' if x in ['LW','RW'] else 'D')
        
        stats.columns = ['playerName', 'season', 'GP', 'TOI', 'G/60', 'A1/60', 'A2/60', 'Points/60',
                         'iSF/60', 'iFF/60', 'iCF/60', 'ixG/60', 'Sh%', 'FSh%', 'xFSh%',
                        'iBLK/60', 'GIVE/60', 'TAKE/60', 'iHF/60', 'iHA/60', 'iPENT2/60',
                        'iPEND2/60', 'iPENT5/60', 'iPEND5/60', 'iPEN±/60', 'FOW/60', 'FOL/60',
                        'FO±/60', 'EVO_GAR/60', 'EVD_GAR/60', 'PPO_GAR/60', 'SHD_GAR/60',
                        'Take_GAR/60', 'Draw_GAR/60', 'Off_GAR/60', 'Def_GAR/60', 'Pens_GAR/60',
                        'GAR/60', 'WAR/60', 'SPAR/60', 'G±/60', 'xG±/60', 'C±/60', 'GF/60',
                        'GA/60', 'xGF/60', 'xGA/60', 'CF/60', 'CA/60']
    
        stats['playerName'] = stats['playerName'].apply(lambda x: unidecode.unidecode(x))
        stats['playerName'] = stats['playerName'].str.replace('.','')
        
        # stats = stats.merge(player_info[['id','name']])
        all_player_info['firstName'] = all_player_info['firstName'].apply(lambda x: ast.literal_eval(x).get('default',''))
        all_player_info['lastName'] = all_player_info['lastName'].apply(lambda x: ast.literal_eval(x).get('default',''))
        all_player_info['playerName'] = all_player_info['firstName'] + ' ' + all_player_info['lastName']
        all_player_info['playerName'] = all_player_info['playerName'].apply(lambda x: unidecode.unidecode(x))
        all_player_info['playerName'] = all_player_info['playerName'].str.replace('.','')

        stats_merged = stats.merge(all_player_info[['playerName','playerId']],on='playerName',how='inner')
        missing_stats = stats[~stats['playerName'].isin(stats_merged['playerName'])]
        
        # Add stats to skaters

        master_dataset = []
        good = 0
        for i in range(len(skaters)):
            row = skaters.iloc[i:i+1]
            player_id = row['id'].values[0]
            season = row['season'].values[0]
            stats = stats_merged[stats_merged['playerId'] == player_id]
            eligible_seasons = [int(f'{int(season[:4])-1}{int(season[4:])-1}'), int(f'{int(season[:4])-2}{int(season[4:])-2}'), int(f'{int(season[:4])-3}{int(season[4:])-3}')]
            weights = {int(f'{int(season[:4])-1}{int(season[4:])-1}'):self.settings['season_weights']['n-1'],
                          int(f'{int(season[:4])-2}{int(season[4:])-2}'):self.settings['season_weights']['n-2'],
                          int(f'{int(season[:4])-3}{int(season[4:])-3}'):self.settings['season_weights']['n-3']}
            stats = stats[stats['season'].isin(eligible_seasons)]
            if len(stats) != 3:
                continue
            stats = stats[stats['season'].isin(eligible_seasons)]

            for col in stats.columns:
                if col in ['playerName','playerId','season']:
                    continue
                for j in range(len(stats)):
                    stats[col].iloc[j] = stats[col].iloc[j] * weights[stats['season'].iloc[j]]
                col_sum = stats[col].sum()
                row[col] = col_sum
            master_dataset.append(row)
        
        master_dataset = pd.concat(master_dataset)
        print(f'{len(master_dataset)} skaters found after merging stats')
        master_dataset.to_csv('temp/master_dataset.csv',index=False)
        return master_dataset

    def manual_match_players(self,missing):

        # check if already exists
        if os.path.exists('temp/matches.csv'):
            return pd.read_csv('temp/matches.csv')

        finds = []
        unique = missing['playerName'].unique()
        
        for i in range(len(unique)):
            print(f'{i}/{len(unique)}')
            search = unique[i].split(' ')[1:]
            search = ' '.join(search)
            r = requests.get(f'https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=20&q={search}')
            results = r.json()
            if len(results) == 1:
                player = results[0]
                player['playerName'] = unique[i]
                finds.append(player)
                continue
            if len(results) > 10:
                r = requests.get(f'https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=100&q={unique[i]}')
                results = r.json()
                if len(results) > 10:
                    results = [x for x in results if x['name'].lower().startswith(unique[i].lower()[0])]
                    if len(results) > 5:
                        results = [x for x in results if x['name'].split(' ')[-1].lower().startswith(unique[i].split(' ')[-1].lower())]
            text = ''
            text += '='*20 + '\n'
            text += f'{unique[i]}\n'
            text += '='*20 + '\n'
            ind = None
            for j, result in enumerate(results):
                if result['name'].replace('.','').lower() == unique[i].replace('.','').lower():
                    player = result
                    player['playerName'] = unique[i]
                    finds.append(player)
                    ind = j
                    break
                text += f'{j}: {result["name"]}\n'
            if ind is not None:
                continue
            print(text)
            ind = input('Enter the correct name #: ')
            if ind == '':
                continue
            ind = int(ind)
            player = results[ind]
            player['playerName'] = unique[i]
            finds.append(player)
        finds = pd.DataFrame(finds)
        finds.to_csv('temp/matches.csv',index=False)

    def get_player_info(self,player_id):
        print(player_id['index'],end='\r')
        player_id = int(player_id['id'])
        r = requests.get(f"https://api-web.nhle.com/v1/player/{player_id}/landing")
        if r.status_code == 200:
            return r.json()
        else:
            print(f"Error: {r.status_code} | {r.text} | {r.url}")
            return {}
                