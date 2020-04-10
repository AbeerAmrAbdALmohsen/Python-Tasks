import subprocess

from keras.models import model_from_json



import json

with open('model.json') as f:
  FJson = json.load(f)
  FJson = json.dumps(FJson)

model =  model_from_json(FJson)

model.load_weights("model.h5")

import sqlalchemy as db
con = db.create_engine('postgresql://iti:iti@localhost/dm')
con.table_names()

import pandas as pd
query = """
SELECT pregnancies,glucose,bloodpressure,skinthickness,insulin,bmi,diabetespedigreefunction,age 
FROM diabetes_unscored
EXCEPT
SELECT pregnancies,glucose,bloodpressure,skinthickness,insulin,bmi,diabetespedigreefunction,age
FROM diabetes_scored
"""

diabetes_unscored = pd.read_sql(query, con)

Scored = model.predict(diabetes_unscored)

outcome = []
for i in Scored:
    if i<0.5:
        a = 0
    else:
        a = 1
    outcome.append(a)

diabetes_unscored['outcome'] = outcome

diabetes_unscored.to_sql(name = 'diabetes_scored',
              con=con,
              if_exists='append')

