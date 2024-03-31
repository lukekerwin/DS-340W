import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import VotingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SelectKBest, f_regression

class ContractPredictor:
    def __init__(self, contract_data, statistics_data):
        self.contract_data = contract_data
        self.statistics_data = statistics_data
        self.dataset = self.__merge_data()

    def __merge_data(self):
        contract_data_ = pd.DataFrame(self.contract_data)
        stats_data_ = pd.DataFrame(self.statistics_data)
        dataset = []
        for season in sorted(contract_data_['DATE'].unique()):
            print(f'--- {season} ---')
            contract_data = contract_data_[contract_data_['DATE'].astype(int)==season]
            contract_data = contract_data[contract_data['STRUCTURE'] == '1-way']
            contract_data = contract_data[contract_data['EXTENSION'] == 0]
            stats_data = stats_data_[stats_data_['SEASON'].astype(int).isin([season-3, season-2, season-1])]

            stats_data = stats_data.groupby('PLAYER').agg({'A':'sum', 'BLK':'sum', 'EVA':'sum', 'EVG':'sum', 'EVSH':'sum', 'FOL':'sum', 'FOW':'sum',
                                                        'G':'sum', 'GP':'sum', 'GWG':'sum', 'HIT':'sum', 'PIM':'sum', 'PLUSMINUS':'sum', 'PPA':'sum',
                                                        'PPG':'sum', 'PPSH':'sum', 'PS':'mean', 'PTS':'sum', 'S':'sum', 'TOI':'sum'}).reset_index()
            for col in ['A', 'BLK', 'EVA', 'EVG', 'EVSH', 'FOL', 'FOW', 'G', 'GWG', 'HIT', 'PIM', 'PLUSMINUS', 'PPA', 'PPG', 'PPSH', 'PTS', 'S', 'TOI']:
                stats_data[col] = round(stats_data[col]/stats_data['GP'],3)

            data = pd.merge(contract_data, stats_data, on='PLAYER', how='left').dropna()
            data.drop(columns=['id', 'TEAM', 'EXTENSION', 'STRUCTURE', 'TYPE'], inplace=True)
            def get_pos(pos):
                # if C and any of LW or RW, then F
                if 'C' in pos and ('LW' in pos or 'RW' in pos):
                    return 'F'
                # if LW and RW, then W
                elif 'LW' in pos or 'RW' in pos:
                    return 'W'
                elif 'C' in pos:
                    return 'C'
                elif 'D' in pos:
                    return 'D'
                else:
                    return 'G'
            data['POS'] = data['POS'].apply(get_pos)
            data.drop(columns=['LENGTH','VALUE'], inplace=True)
            dataset.append(data)
        dataset = pd.concat(dataset)

        return dataset
    
    def predict(self):
        data = self.dataset
        data = pd.get_dummies(data, columns=['POS'])
        data['id'] = data['PLAYER'].str.replace(' ', '').str.replace("'", '') + data['DATE'].astype(str)
        X = data.drop(columns=['CAP_HIT', 'PLAYER','id','DATE'])
        y = data['CAP_HIT']
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        selector = SelectKBest(f_regression, k=5)
        X_selected = selector.fit_transform(X_scaled, y)

        # Linear Regression
        lr = LinearRegression()
        lr.fit(X_selected, y)

        # Ridge Regression
        ridge = Ridge()
        ridge.fit(X_selected, y)

        # Lasso Regression
        lasso = Lasso()
        lasso.fit(X_selected, y)

        # Voting Regressor
        vr = VotingRegressor([('lr', lr), ('ridge', ridge), ('lasso', lasso)])
        vr.fit(X_selected, y)

        # record predictions
        predictions = pd.DataFrame()
        predictions['id'] = data['id']
        predictions['PLAYER'] = data['PLAYER']
        predictions['CAP_HIT'] = data['CAP_HIT']
        predictions['PREDICTION'] = vr.predict(X_selected)
        predictions['ERROR'] = predictions['CAP_HIT'] - predictions['PREDICTION']
        predictions['ERROR'] = predictions['ERROR'].apply(lambda x: round(x, 3))

        return predictions.to_dict('records')


