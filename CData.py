# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 10:32:18 2021

@author: burneli
"""

import petl as etl
import pandas as pd
import cdata.workday as mod
 
cnxn = mod.connect("User=ISU_HRDW_Recruiting_RaaS;Password=P87@fiaoe%;Tenant=republic;Host=https://wd5-impl-services1.workday.com")
  
sql = "SELECT Worker_Reference_WID, Legal_Name_Last_Name FROM Human_Resources.Workers WHERE Legal_Name_Last_Name = 'Morgan'"
 
table1 = etl.fromdb(cnxn,sql)
 
table2 = etl.sort(table1,'Legal_Name_Last_Name')
 
etl.tocsv(table2,'workers_data.csv')