from code.dataset import Dataset
from code.ml import PredictContract

if __name__ == '__main__':
    dataset = Dataset()
    dataset.load_contracts()
    # years = ['20102011', '20112012', '20122013', '20132014', '20142015', '20152016', '20162017', '20172018', '20182019', '20192020', '20202021', '20212022', '20222023', '20232024']
    # for year in years:
    #     print(f'Predicting contracts for {year}...')
    #     PredictContract(year)
    # print('Done! All predictions are in outputs/')