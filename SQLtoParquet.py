# -*- coding: utf-8 -*-
"""
Created on Tue Sep  7 11:34:36 2021

@author: burneli
"""

#!/usr/bin/env python

import urllib
import sqlalchemy as sa
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

 
server = 'prodhrissql01'
database = 'RS_DW'

connection_string = ('Driver=ODBC Driver 17 for SQL Server;'
                     'SERVER='+server+';'
                     'Database='+database+';'
                     'Trusted_Connection=Yes;')

connection_uri = f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}'
 
# create engine
engine = sa.create_engine(connection_uri, fast_executemany=True)
 
# declare schema name and table name
schema_name = 'dbo'
table_name = 'tblKronosRSDOTDATA'
 
query = "SELECT * FROM dbo.tblKronosRSDOTDATA;"
df = pd.read_sql(query, engine)
table = pa.Table.from_pandas(df)
pq.write_table(table, 'D:\Data\HistoricaltoSnowflake\tblKronosRSDOTDATA.parquet',compression='GZIP')


metadata = pq.read_metadata('tblKronosRSDOTDATA.parquet')
print(metadata)
rows = pd.read_parquet('tblKronosRSDOTDATA.parquet')
print(rows)



