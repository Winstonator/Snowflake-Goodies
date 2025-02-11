FROM python:3.8-slim-buster

RUN pip install awscli
RUN pip install snowflake-connector-python==2.5.0 jsonref==0.2 zeep==4.1.0 Pillow==9.2.0 awswrangler==2.18.0
RUN apt-get update && apt-get install jq -y && apt-get install p7zip-full -y 

COPY . /

RUN chmod 700 /entrypoint.sh 
RUN chmod 700 /rsi_wfa_python/*

ENTRYPOINT ["/entrypoint.sh"]