# -*- coding: utf-8 -*-
"""
Created on Tue Sep  7 09:46:24 2021

@author: burneli
"""
import urllib
import sqlalchemy as sa
import pandas as pd
import json
import petl as etl
server = 'DEVHRISSQLVW201'
database = 'SandBox'

# create connection string
connection_string = ('Driver=ODBC Driver 17 for SQL Server;'
                     'SERVER='+server+';'
                     'Database='+database+';'
                     'Trusted_Connection=Yes;')

import workday
from workday.auth import WsSecurityCredentialAuthentication
import keyring

client = workday.WorkdayClient(
    wsdls={'Workers': 'https://wd5-impl-services1.workday.com/ccx/service/republic/Human_Resources/v36.2'},
    authentication=WsSecurityCredentialAuthentication('ISU_HRDW_Recruiting_RaaS@republic', keyring.get_password("RequisitionISU","ISU_HRDW_Recruiting_RaaS@republic")),
    )

"""print(client.Workers.Get_Workers().data)"""

connection_uri = f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}'
 
# create engine
engine = sa.create_engine(connection_uri, fast_executemany=True)
 
# declare schema name and table name
schema_name = 'dbo'
table_name = 'PyWorkers'


# create schema if schema doesn't exist
if schema_name not in engine.dialect.get_schema_names(engine):
   engine.execute(sa.schema.CreateSchema(schema_name))
  
# set up dtype for each column
dtypes = {'Worker': sa.types.NVARCHAR(length=max)}

result = etl.fromjson(client.Workers.Get_Workers().data)
#print(result)
for worker in result.Worker:
  if worker.Worker_Data is not None:
    print(worker.Worker_Data.Worker.Worker_ID)
    df = pd.DataFrame(worker.Worker_Data.Worker_Data)
#with open(etl.fromjson(client.Workers.Get_Workers().data)) as f:
#    data = json.load(f)

# Create A DataFrame From the JSON Data
#df = pd.DataFrame(data)
print(df)
# upload dataframe
with engine.begin() as connection:
    df.to_sql(table_name,
               con=connection,
               schema=schema_name,
               if_exists='replace')



