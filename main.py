
"""
CODE TAKEN FROM: https://stackoverflow.com/questions/63256656/how-i-can-fix-this-beautifulsoup-website-scrape-for-nhl-reference
"""

import pandas as pd

result = pd.DataFrame()
for i in range (2010,2020):
    print(i)
    year = str(i)
    url = 'https://www.hockey-reference.com/leagues/NHL_'+year+'_skaters.html'
    
    #source = requests.get('https://www.hockey-reference.com/leagues/NHL_'+year+'_skaters.html').text
    df = pd.read_html(url,header=1)[0]
    df['year'] = year
    result = result.append(df, sort=False)
    
result = result[~result['Age'].str.contains("Age")]    
result = result.reset_index(drop=True)
result.to_csv('data/NHL_2010_2019.csv', index=False)