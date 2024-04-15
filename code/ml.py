import pickle
import pandas as pd
from sklearn.model_selection import train_test_split
import joblib


def PredictContract(year='20212022'):
    data = 'temp/master_dataset.csv'
    df = pd.read_csv(data)
    term_data = df[['age', 'position', 'structure','term', 'date', 'GP',
       'draftYear', 'draftRound','TOI', 'G/60', 'A1/60', 'A2/60', 'Points/60', 'iSF/60',
       'iFF/60', 'iCF/60', 'ixG/60', 'Sh%', 'FSh%', 'xFSh%', 'iBLK/60',
       'GIVE/60', 'TAKE/60', 'iHF/60', 'iHA/60', 'iPENT2/60', 'iPEND2/60',
       'iPENT5/60', 'iPEND5/60', 'iPEN±/60', 'FOW/60', 'FOL/60', 'FO±/60',
       'EVO_GAR/60', 'EVD_GAR/60', 'PPO_GAR/60', 'SHD_GAR/60', 'Take_GAR/60',
       'Draw_GAR/60', 'Off_GAR/60', 'Def_GAR/60', 'Pens_GAR/60', 'GAR/60',
       'WAR/60', 'SPAR/60', 'G±/60', 'xG±/60', 'C±/60', 'GF/60', 'GA/60',
       'xGF/60', 'xGA/60', 'CF/60', 'CA/60']]
    term_data['age_tier'] = pd.cut(term_data['age'], bins=[0,22,24,26,29,34,100], labels=["Tier 1", "Tier 2", "Tier 3", "Tier 4", "Tier 5", "Tier 6"])
    term_data['yearsSinceDraft'] = pd.to_datetime(term_data['date']).dt.year - term_data['draftYear']
    term_data['yearsSinceDraft'] = term_data['yearsSinceDraft'].fillna(term_data['age'] -18)
    term_data['draftRound'] = term_data['draftRound'].fillna(8)
    term_data['draftRound'] = term_data['draftRound'].astype(int)
    term_data['draftRound'] = term_data['draftRound'].astype(str)
    term_data['TOI%'] = term_data['TOI'] / (term_data['GP'] * 60)
    term_data = term_data.drop(columns=['TOI', 'GP', 'date', 'draftYear', 'age'])
    term_data = pd.get_dummies(term_data)
    if 'draftRound_9' not in term_data.columns:
        term_data['draftRound_9'] = 0
    X = term_data.drop(columns=['term'])
    y = term_data['term']

    rf = pickle.load(open('models/rf_term_model.sav', 'rb'))
    
    df['term_pred'] = rf.predict(X).astype(int)
    df.to_csv('temp/master_dataset_term.csv', index=False)
    
    salary_cap_data = pd.read_html('https://www.capfriendly.com/salary-cap')[0]
    salary_cap_data.columns = ['season','conf','%','cap','lower','min_salary']
    salary_cap_data = salary_cap_data.drop(columns=['conf','%','lower'])
    salary_cap_data['season'] = salary_cap_data['season'].str.replace('-','20')
    salary_cap_data['cap'] = salary_cap_data['cap'].str.replace('$','').str.replace(',','').astype(int)
    salary_cap_data['min_salary'] = salary_cap_data['min_salary'].str.replace('$','').str.replace(',','').astype(int)
    salary_cap_data['season'] = salary_cap_data['season'].astype(int)
    salary_cap_data

    df['caphit_%'] = df.apply(lambda x: x['caphit']/salary_cap_data[salary_cap_data['season'] == x['season']]['cap'].values[0], axis=1)

    caphit_data = df[['position', 'date', 'structure', 'age', 'GP',
        'type',  'draftYear', 'draftRound', 'TOI', 'G/60', 'A1/60', 'A2/60', 'Points/60', 'iSF/60',
        'iFF/60', 'iCF/60', 'ixG/60', 'Sh%', 'FSh%', 'xFSh%', 'iBLK/60',
        'GIVE/60', 'TAKE/60', 'iHF/60', 'iHA/60', 'iPENT2/60', 'iPEND2/60',
        'iPENT5/60', 'iPEND5/60', 'iPEN±/60', 'FOW/60', 'FOL/60', 'FO±/60',
        'EVO_GAR/60', 'EVD_GAR/60', 'PPO_GAR/60', 'SHD_GAR/60', 'Take_GAR/60',
        'Draw_GAR/60', 'Off_GAR/60', 'Def_GAR/60', 'Pens_GAR/60', 'GAR/60',
        'WAR/60', 'SPAR/60', 'G±/60', 'xG±/60', 'C±/60', 'GF/60', 'GA/60',
        'xGF/60', 'xGA/60', 'CF/60', 'CA/60', 'term_pred', 'caphit_%']]
    caphit_data['age_tier'] = pd.cut(caphit_data['age'], bins=[0,22,24,26,29,34,100], labels=["Tier 1", "Tier 2", "Tier 3", "Tier 4", "Tier 5", "Tier 6"])
    caphit_data['yearsSinceDraft'] = pd.to_datetime(caphit_data['date']).dt.year - caphit_data['draftYear']
    caphit_data['yearsSinceDraft'] = caphit_data['yearsSinceDraft'].fillna(caphit_data['age'] -18)
    caphit_data['draftRound'] = caphit_data['draftRound'].fillna(8)
    caphit_data['draftRound'] = caphit_data['draftRound'].astype(int)
    caphit_data['draftRound'] = caphit_data['draftRound'].astype(str)
    caphit_data['TOI%'] = caphit_data['TOI'] / (caphit_data['GP'] * 60)
    caphit_data = caphit_data.drop(columns=['TOI', 'GP', 'date', 'draftYear', 'age'])
    caphit_data = pd.get_dummies(caphit_data, columns=['position', 'structure', 'age_tier', 'draftRound'])
    caphit_data.to_csv('temp/caphit_data.csv', index=False)

    # # Load model
    voting = joblib.load('models/voting_cap.pkl')

    # import metrics
    from sklearn.metrics import mean_squared_error

    # import preprocessing
    from sklearn.preprocessing import StandardScaler

    caphit_data = pd.read_csv('temp/caphit_data.csv')
    X = caphit_data.drop(columns=['caphit_%'])
    y = caphit_data['caphit_%']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=716)

    scaler = StandardScaler()
    scaler.fit_transform(X_train)

    df['caphit_pred'] = voting.predict(scaler.transform(X))
    print(df)
    results = df
    results['caphit_pred'] = results.apply(lambda x: x['caphit_pred']*salary_cap_data[salary_cap_data['season'] == x['season']]['cap'].values[0], axis=1).round(2)
    # Adjust for minimum salary
    results['caphit_pred'] = results.apply(lambda x: max(x['caphit_pred'], salary_cap_data[salary_cap_data['season'] == x['season']]['min_salary'].values[0]), axis=1)

    # Adjust for max term (8 years)
    results['term_pred'] = results['term_pred'].clip(0, 8)
    results['term_pred'] = results['term_pred'].astype(int)
    results['term'] = results['term'].clip(0, 8)
    results['term'] = results['term'].astype(int)
    results.to_csv('outputs/results.csv', index=False)
    results = results[results['season'] == year]
    results.to_csv(f'outputs/{year}-results.csv', index=False)