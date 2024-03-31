from flask import request
from api import app, Session, db
import json
from sqlalchemy import text
from sqlalchemy import Column, Integer, String, Float
from ml import ContractPredictor

# ---- Models ----

class Contract(db.Model):   
    __tablename__ = 'contracts'
    id = Column(String, primary_key=True)
    PLAYER = Column(String)
    AGE = Column(Integer)
    POS = Column(String)
    TEAM = Column(String)
    DATE = Column(Integer)
    TYPE = Column(String)
    EXTENSION = Column(Integer)
    STRUCTURE = Column(String)
    LENGTH = Column(Integer)
    VALUE = Column(Integer)
    CAP_HIT = Column(Integer)

    def __repr__(self):
        return '<Contract %r>' % self.PLAYER
    
class Stat(db.Model):
    __tablename__ = 'stats'
    id = Column(String, primary_key=True)
    PLAYER = Column(String)
    AGE = Column(Float)
    TEAM = Column(String)
    POS = Column(String)
    GP = Column(Float)
    G = Column(Float)
    A = Column(Float)
    PTS = Column(Float)
    PLUSMINUS = Column(Float)
    PIM = Column(Float)
    PS = Column(Float)
    EVG = Column(Float)
    EVA = Column(Float)
    PPG = Column(Float)
    PPA = Column(Float)
    EVSH = Column(Float)
    PPSH = Column(Float)
    GWG = Column(Float)
    S = Column(Float)
    S_ = Column(Float)
    TOI = Column(Float)
    ATOI = Column(Float)
    BLK = Column(Float)
    HIT = Column(Float)
    FOW = Column(Float)
    FOL = Column(Float)
    FO_ = Column(Float)
    SEASON = Column(Float)

    def __repr__(self):
        return '<Stat %r>' % self.PLAYER

class Predictions(db.Model):
    __tablename__ = 'predictions'
    id = Column(String, primary_key=True)
    PLAYER = Column(String)
    CAP_HIT = Column(Float)
    ERROR = Column(Float)
    PREDICTION = Column(Float)

# ---- Helper Functions ----

def get_len(row):
    length = 0
    for r in row:
        length += 1
    return length
    
def get_results(rows):
    output = []
    for row in rows:
        try:
            data = {}
            for column in range(get_len(row)):
                dic = row[column].__dict__
                if '_sa_instance_state' in dic:
                    del dic['_sa_instance_state']
                data.update(dic)
            output.append(data)
        except:
            data = row.__dict__
            if '_sa_instance_state' in data:
                del data['_sa_instance_state']
            output.append(data)
    return output

def get_query_filters(args):
    if isinstance(args, str):
        query = json.loads(args)
    
    if isinstance(args, dict):
        if args:
            filters = []
            for key, value in args.items():
                if value:
                    filters.append(str(key) + "='" + str(value) + "'")
            return " AND ".join(filters)
        else:
            return ""
    
    if isinstance(args, list):
        if args:
            filters = []
            for arg in args:
                if isinstance(arg, dict):
                    for key, value in arg.items():
                        if value:
                            filters.append(str(key) + "='" + str(value) + "'")
            return " AND ".join(filters)
        else:
            return ""
        
def updated_row(table, record):
    table_name = table.__tablename__
    query = f'REPLACE INTO {table_name} '
    columns = "("
    values = "("
    params = {}

    for key, value in record.items():
        columns += f"{key}, "
        values += f":{key}, "
        params[key] = value
    
    columns = columns[:-2] + ")"
    values = values[:-2] + ")"

    query += columns + " VALUES " + values
    return query, params

def request_GET(table, args, primary_keys, columns):
    pk_args = {key: args[key] for key in args if key in primary_keys}
    non_pk_args = {key: args[key] for key in args if key in columns}

    query_filters = get_query_filters(pk_args)

    if query_filters:
        rows = table.query.filter(text(query_filters)).all()
        return get_results(rows), 200
    else:
        query_filters = get_query_filters(non_pk_args)
        if query_filters:
            rows = table.query.filter(text(query_filters)).all()
            return get_results(rows), 200
        else:
            rows = table.query.all()
            return get_results(rows), 200
        
def request_POST(table, data, primary_keys):
    pk_args = {key: data[key] for key in data if key in primary_keys}

    query_filters = get_query_filters(pk_args)

    if isinstance(data, str):
        data = json.loads(data)
    
    if not isinstance(data, list):
        data = [data]
    
    for record in data:
        query_filters = get_query_filters([{key: record[key] for key in record if key in primary_keys}])
        result = table.query.filter(text(query_filters)).first()
        if result:
            query, params = updated_row(table, record)
            db.session.execute(text(query).bindparams(**params))
        else:
            db.session.add(table(**record))
    db.session.commit()
    return "Success", 200     

# ---- Endpoints ----

@app.route('/api/')
def home():
    return "Welcome to Group 21's API!", 200

@app.route('/api/contracts', methods=['GET', 'POST'])
def contracts():
    if request.method == 'GET':
        return request_GET(Contract, request.args, ['id'], Contract.__table__.columns.keys())
    elif request.method == 'POST':
        return request_POST(Contract, request.json, ['id'])

@app.route('/api/statistics', methods=['GET', 'POST'])
def statistics():
    if request.method == 'GET':
        return request_GET(Stat, request.args, ['id'], Stat.__table__.columns.keys())
    elif request.method == 'POST':
        return request_POST(Stat, request.json, ['id'])
    
@app.route('/api/ml/data', methods=['GET'])
def ml_data():
    season = request.args.get('season')
    if season:
        # get contract data from that season
        contracts = Contract.query.filter(Contract.DATE==season).all()
        # get stat data from previous 3 seasons
        stats = Stat.query.filter(Stat.SEASON.between(int(season)-3, int(season)-1)).all()
        return {'contracts': get_results(contracts), 'stats': get_results(stats)}, 200
    else:
        return "No Parameters Given", 400
    
@app.route('/api/ml/predict', methods=['GET'])
def ml_predict():
    season = request.args.get('season')
    if season:
        # get contract data from that season
        contracts = Contract.query.filter(Contract.DATE==season).all()
        # get stat data from previous 3 seasons
        stats = Stat.query.filter(Stat.SEASON.between(int(season)-3, int(season)-1)).all()
        contracts = get_results(contracts)
        stats = get_results(stats)

        predictor = ContractPredictor(contracts, stats)
        prediction = predictor.predict()
        print(prediction)
        # write predictions to db
        request_POST(Predictions, prediction, ['id'])
        return {'predictions': prediction}, 200



if __name__ == '__main__':
    app.run(debug=True)