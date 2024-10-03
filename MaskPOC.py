# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 12:53:08 2022

@author: burneli
"""

import requests
import json
import keyring
from zeep import helpers
#sending data to sql

#create target connect


url = 'https://wd5-impl-services1.workday.com/ccx/service/customreport2/republic/MEHRAFA/Masking-POC?&format=json'


username =   'ISU_HRDW_Worker_RaaS'
password =   keyring.get_password("Worker","ISU_HRDW_Worker_RaaS@republic")


with requests.Session() as s:
    download = s.get(url, auth=(username, password))
    decoded_content = download.content.decode('utf-8')

data_dict = helpers.serialize_object(decoded_content, dict)
print(data_dict)
# saves as json file
with open('maskPOC.json', 'w') as write_file:
    json.dump(data_dict, write_file, indent=4, default=str)