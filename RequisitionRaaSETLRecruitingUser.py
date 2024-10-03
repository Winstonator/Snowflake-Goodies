# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 10:32:18 2021

@author: burneli
"""
import requests
import json
import keyring
from zeep import helpers
#sending data to sql

#create target connect


url = 'https://wd5-impl-services1.workday.com/ccx/service/customreport2/republic/MEHRAFA/WD790_-_HRDW_-_Job_Requisition_-_RaaS_-_V2?CF_-_HRA_-_Action_Event_-_BD_-_Date_Completed=2022-02-15T00:00:00.000-07:00&format=json'


username =   'ISU_HRDW_Recruiting_RaaS'
password =   keyring.get_password("RecruitingISU","ISU_HRDW_Recruiting_RaaS@republic")


with requests.Session() as s:
    download = s.get(url, auth=(username, password))
    decoded_content = download.content.decode('utf-8')

data_dict = helpers.serialize_object(decoded_content, dict)
print(data_dict)
# saves as json file
with open('requistionsRaaS.json', 'w') as write_file:
    json.dump(data_dict, write_file, indent=4, default=str)
