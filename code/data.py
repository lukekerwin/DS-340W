import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')


class DataCollection:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
        }
        self.session.headers.update(self.headers)
        
        self.START_YEAR = 2012 
        self.END_YEAR = 2022

    # ---- Contract Data ----

    def get_contract_data(self):
        contracts = []
        months = {
            'January': {'num':'01','start':'01', 'end':'31'},
            'February': {'num':'02','start':'02', 'end':'28'},
            'March': {'num':'03','start':'03', 'end':'31'},
            'April': {'num':'04','start':'04', 'end':'30'},
            'May': {'num':'05','start':'05', 'end':'31'},
            'June': {'num':'06','start':'06', 'end':'30'},
            'July': {'num':'07','start':'07', 'end':'31'},
            'August': {'num':'08','start':'08', 'end':'31'},
            'September': {'num':'09','start':'09', 'end':'30'},
            'October': {'num':'10','start':'10', 'end':'31'},
            'November': {'num':'11','start':'11', 'end':'30'},
            'December': {'num':'12','start':'12', 'end':'31'}
        }
        for year in range(self.START_YEAR, self.END_YEAR+1):
            for month in months:
                print(f'Getting contract data for {month} {year}...', end='\r')
                data = months[month]
                string = f'{data["num"]}01{year}-{data["num"]}{data["end"]}{year}'
                url = f'https://www.capfriendly.com/ajax/signings/all/all/all/1-15/0-15000000/{string}'
                length = 50
                pc = 1
                while length == 50:
                    url = f'https://www.capfriendly.com/ajax/signings/all/all/all/1-15/0-15000000/{string}?p={pc}'
                    html = requests.get(url).json()
                    if html['data'] != None:
                        html = html['data']['html']
                        if html != '':
                            html = f'<table>{html}</table>'
                            soup = BeautifulSoup(html)
                            table = soup.find('table')
                            df = pd.read_html(str(table))[0]
                            df.columns = ['PLAYER', 'PLAYER.1', 'AGE', 'POS', 'TEAM', 'DATE', 'TYPE', 'EXTENSION', 'STRUCTURE', 'LENGTH', 'VALUE', 'CAP HIT']
                            contracts.append(df)
                            length = len(df)
                        else:
                            length = 0
                    else:
                        length = 0
                    pc += 1
        contracts = pd.concat(contracts)
        contracts.to_csv('contracts.csv', index=False)
        return contracts

    
    def clean_contract_data(self, contracts: pd.DataFrame):
        # Remove PLAYER.1
        cleaned_contracts = contracts.drop(columns=['PLAYER.1'])

        # Data formatting
        cleaned_contracts['DATE'] = cleaned_contracts['DATE'].str[-4:].astype(int)
        cleaned_contracts['VALUE'] = cleaned_contracts['VALUE'].str.replace('$', '').str.replace(',', '').astype(float)
        cleaned_contracts['CAP HIT'] = cleaned_contracts['CAP HIT'].str.replace('$', '').str.replace(',', '').astype(float)
        cleaned_contracts['EXTENSION'] = cleaned_contracts['EXTENSION'].str.replace('✔', '1').fillna(0).astype(int)

        # Removing data we dont want
        cleaned_contracts = cleaned_contracts[cleaned_contracts['TYPE'].isin(['Stnd (UFA)','35+ (UFA)'])]
        cleaned_contracts = cleaned_contracts[cleaned_contracts['STRUCTURE']=='1-way']
        cleaned_contracts = cleaned_contracts[cleaned_contracts['EXTENSION']==0].reset_index(drop=True)
        cleaned_contracts['id'] = cleaned_contracts['PLAYER'].str.replace(' ', '').str.replace("'", '') + cleaned_contracts['DATE'].astype(int).astype(str)
        cleaned_contracts = cleaned_contracts.drop_duplicates().reset_index(drop=True)
        print(cleaned_contracts)
        return cleaned_contracts
    
    def post_contract_data(self, cleaned_contracts: pd.DataFrame):
        url = 'http://127.0.0.1:5000/api/contracts'
        cleaned_contracts.columns = [col.replace(' ', '_') for col in cleaned_contracts.columns]
        print()
        print(cleaned_contracts.columns)
        print()
        data = cleaned_contracts.to_dict('records')
        r = self.session.post(url, json=data)
        if r.status_code == 200:
            print('Contracts posted successfully')
        else:
            print('Error posting contracts')

    # ---- Statistics Data ----
            
    def get_stats_data(self):
        stats = []
        for year in range(self.START_YEAR-3, self.END_YEAR+1):
            print(f'Getting stats data for {year}...', end='\r')
            url = f'https://www.hockey-reference.com/leagues/NHL_{year}_skaters.html'
            r = requests.get(url)
            table = pd.read_html(r.text)[0]
            table['year'] = year
            stats.append(table)
        
        stats = pd.concat(stats)
        return stats
    
    def clean_stats_data(self, stats: pd.DataFrame):
        stats_cleaned = stats.copy()
        stats_cleaned.columns = ['Rk', 'Player', 'Age', 'Tm', 'Pos', 'GP', 'G', 'A', 'PTS', '+/-', 'PIM',
       'PS', 'EV', 'PP', 'SH', 'GW', 'EV', 'PP', 'SH', 'S', 'S%', 'TOI',
       'ATOI', 'BLK', 'HIT', 'FOW', 'FOL', 'FO%', 'SEASON']
        stats_cleaned = stats_cleaned[['Player', 'Age', 'Tm', 'Pos', 'GP', 'G', 'A', 'PTS', '+/-', 'PIM',
            'PS', 'EV', 'PP', 'SH', 'GW', 'EV', 'PP', 'SH', 'S', 'S%', 'TOI',
            'ATOI', 'BLK', 'HIT', 'FOW', 'FOL', 'FO%', 'SEASON']]
        stats_cleaned = stats_cleaned[stats_cleaned['Player']!='Player'].reset_index(drop=True)

        new = []
        for player in stats_cleaned['Player'].unique():
            data = stats_cleaned[stats_cleaned['Player']==player]
            for season in data['SEASON'].unique():
                data_season = data[data['SEASON']==season]
                if len(data_season) > 1:
                    data_season = data_season[data_season['Tm']=='TOT']
                else:
                    pass
                new.append(data_season)

        stats_cleaned = pd.concat(new).reset_index(drop=True)
        stats_cleaned['Pos'] = np.where(stats_cleaned['Pos'].isin(['D','LD','RD']), 'D', np.where(stats_cleaned['Pos'].isin(['C']), 'C', 'W'))
        stats_cleaned['Pos'].unique()
        stats_cleaned.columns = ['PLAYER', 'AGE', 'TEAM', 'POS', 'GP', 'G', 'A', 'PTS', 'PLUSMINUS', 'PIM', 'PS',
            'EVG', 'EVA', 'PPG', 'PPA', 'EVSH', 'PPSH', 'GWG', 'EV', 'EV', 'PP', 'PP', 'SH',
            'SH', 'S', 'S%', 'TOI', 'ATOI', 'BLK', 'HIT', 'FOW', 'FOL', 'FO%',
            'SEASON']
        stats_cleaned = stats_cleaned.drop(columns=['EV', 'PP', 'SH'])
        stats_cleaned['S%'] = stats_cleaned['S%'].str.replace('%', '').astype(float)/100
        stats_cleaned['TOI'] = stats_cleaned['TOI'].astype(int) * 60
        stats_cleaned['ATOI'] = stats_cleaned['ATOI'].str.split(':').apply(lambda x: int(x[0])*60 + int(x[1]))
        stats_cleaned['FO%'] = stats_cleaned['FO%'].str.replace('%', '').astype(float)/100
        stats_cleaned = stats_cleaned[stats_cleaned['GP'].astype(int)>=20].reset_index(drop=True)

        for col in stats_cleaned.columns:
            try:
                stats_cleaned[col] = stats_cleaned[col].astype(float)
            except:
                try:
                    stats_cleaned[col] = stats_cleaned[col].astype(int)
                except:
                    try:
                        stats_cleaned[col] = stats_cleaned[col].astype(str)
                    except:
                        pass

        stats_cleaned['id'] = stats_cleaned['PLAYER'].str.replace(' ', '').str.replace("'","") + stats_cleaned['SEASON'].astype(str)
        stats_cleaned = stats_cleaned.drop_duplicates(subset=['id']).reset_index(drop=True)
        stats_cleaned['FO%'] = stats_cleaned['FO%'].fillna(0)
        return stats_cleaned

    def post_stats_data(self, stats_cleaned: pd.DataFrame):
        url = 'http://127.0.0.1:5000/api/statistics'
        stats_cleaned.columns = [col.replace('%', '_') for col in stats_cleaned.columns]
        print()
        print(stats_cleaned.columns)
        print()
        data = stats_cleaned.to_dict('records')
        r = self.session.post(url, json=data)
        if r.status_code == 200:
            print('Stats posted successfully')
        else:
            print('Error posting stats')   


if __name__ == '__main__':
    dc = DataCollection()
    contracts = dc.get_contract_data()
    cleaned_contracts = dc.clean_contract_data(contracts)
    dc.post_contract_data(cleaned_contracts)
    stats = dc.get_stats_data()
    cleaned_stats = dc.clean_stats_data(stats)
    dc.post_stats_data(cleaned_stats)