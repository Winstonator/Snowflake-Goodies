import os
import ast
from shutil import ExecError
import sys
from multiprocessing import context
import requests
from zeep import helpers
import logging
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime as dt
from datetime import timedelta
import pytz
import snowflake.connector
from custom_exceptions import *
import warnings
import math

def enable_logging():
    global verbose
    verbose = "true"
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)
    
    warnings.filterwarnings('ignore')

def print_audit_log(integration_type,integration_name,batch_mode,executed_for,raas_record_count,description,key_prefix,split_number,start_time,end_time,rass_start_time,rass_end_time,file_size):
    logging.info(f'Creating audit payload for {integration_name} for the {executed_for} execution')
    start_time=start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    end_time=end_time.strftime('%Y-%m-%d %H:%M:%S.%f')

    audit_json = {
        "integration_type":integration_type,
        "integration_name":integration_name,
        "batch_mode":batch_mode,
        "pull_dt":executed_for,
        "record_count": raas_record_count,
        "description":description,
        "key_path":key_prefix,
        "split_nb":split_number,
        "start_time":start_time,
        "end_time":end_time,
        "rass_start_time":rass_start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        "rass_end_time":rass_end_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        "file_size_mb":file_size,
        "status":"SUCCESS"
    }

    logging.info(f'AUDIT_RAAS_PULL_LOG_INFORMATION: {audit_json}')

    
    try:
        secret_from_ecs=os.environ['SNOWFLAKE_SECRET']
        secrets=ast.literal_eval(secret_from_ecs)
    except Exception as e:
        logging.info(f"Error getting secrets for snowflake connection: {e}")
    
    insert_into_audit_table=f"INSERT INTO {os.environ['RAAS_DATA_PULL_AUDIT_TABLE']} VALUES('{integration_type}','{integration_name.upper()}','{batch_mode}','{executed_for}','{raas_record_count}','{description}','{key_prefix}','{start_time}','{end_time}','SUCCESS',current_timestamp::TIMESTAMP_NTZ(9),'{split_number}','{rass_start_time}','{rass_end_time}','{file_size}')"
    logging.info(f'AUDIT DDL: {insert_into_audit_table}')

    try:
        conn = snowflake.connector.connect (
            user=secrets['user'],
            password=secrets['password'],
            account=secrets['account'],
            warehouse=secrets['warehouse'],
            database=secrets['database'],
            schema=secrets['schema'],
            role=secrets['role'],
            insecure_mode=True
        )
    except Exception as e:
        logging.info(f'Snowflake Connectivity error: {e}')

    cur = conn.cursor()
    try:
        response = cur.execute(insert_into_audit_table)
        logging.info(f"Success {response}")
    except Exception as e:
        logging.info(f'Snowflake error for {cur.sfqid} with error {e}')

def pull_data_from_workday(integration_url,integration_user_name, auth_pass,integration_name,executed_for,bucket_name):
    # Connects to workday and holds the response as a JSON String object     
    logging.info(f'Connecting to workday with {integration_url} url')
    try:
        with requests.Session() as s:
            download = s.get(integration_url, auth=(integration_user_name, auth_pass),verify=False)
            logging.info(f'Data Download Complete')
            decoded_content = download.content.decode('utf-8')
            logging.info(f'Data Decode Complete')

        data_dict = helpers.serialize_object(decoded_content, dict)
        logging.info(f'Data Serialization Complete')

        logging.info(f'Beginning eval for the data')
        
        '''
        if integration_name == 'WFA410_Payroll_Result_Detail':
            key_prefix="workday"+"/"+integration_name.split("_")[0].lower()+"/"+integration_name.split("_",1)[1].replace("_","").lower()+"/"
            file_name="DEBUG-"+integration_name+"_"+executed_for+"-"+dt.strftime((dt.now(pytz.utc) - timedelta(hours=7)),"%Y%m%d%H%M%S%f")+".json"
            key_prefix+=file_name
            
            try:
                s3 = boto3.resource('s3') 
                obj = s3.Object(bucket_name,key_prefix) 
                obj.put(Body=json.dumps(data_dict))
                logging.info(f'S3 upload success: {raas_record_count} records written to {bucket_name}/{key_prefix}')
            except Exception as e:
                logging.info(f'S3 put failed for {event} payload for {key_prefix} key with error {e}')
                raise S3PutError
            exit()
        else:
        '''
        try:
            json_string=eval(data_dict)
        except Exception as e:
            logging.info(f'eval on raas response was un-successful, Trying to load data as json string: {e}')
            try:
                json_string=json.loads(data_dict)
            except Exception as a:
                logging.info(f'Unable to load data as json string: {a}')
                try:
                    logging.info(f'Response type: {type(data_dict)}')
                    dumps=json.dumps(data_dict)
                    logging.info(f'dumps response type: {type(dumps)}')
                    json_string=json.loads(dumps)
                except Exception as b:
                    logging.info(f'Unable to load data using json.dumps: {b}')      
                raise WorkdayConnetionFailure
        logging.info(f'Data pull for workday RaaS was successful')
        return json_string
        
    except:
        logging.info(f'Workday connection failed for {integration_url} with {integration_user_name} user')
        raise WorkdayConnetionFailure
        

def parse_arguments(argument_string):

    logging.info(f'Fetching job parameters from arguments')
    
    try:
        argument_string=argument_string[1:]
        dict_str=""
        
        for each_argument in argument_string:
            dict_str+=each_argument
        
        event=ast.literal_eval(dict_str)
        return event
    except Exception as e:
        logging.info(f'Argument parsing failed with error: {e}')
        raise InvalidArgumentCount

def split_files_by_recs(scoped_json_response,split_size):
    return [scoped_json_response[i::split_size] for i in range(split_size)]

def get_json_structure(scoped_json_response):
    logging.info(f'Extracting JSON structure')
    json_structure={}

    for each_item in scoped_json_response:
        for i,j in each_item.items():
            if i in json_structure:
                if type(j) not in json_structure[i]:
                     json_structure[i].append(type(j))
            else:
                json_structure[i]=[]
                json_structure[i].append(type(j))
    return json_structure

def update_build_and_trigger_audit_table(pk_raas_hash_key,status):
    sf_table_name=os.environ['SF_BUILD_TABLE']
    update_timestamp=dt.now(pytz.utc) - timedelta(hours=7)

    logging.info(f'Updating {sf_table_name} snowflake table with {status} message')
    update_audit_table=f"UPDATE {sf_table_name} SET STATUS='{status}', UPDT_TMSP='{update_timestamp}' WHERE PK_RAAS_PULL='{pk_raas_hash_key}'"

    try:
        secret_from_ecs=os.environ['SNOWFLAKE_SECRET']
        secrets=ast.literal_eval(secret_from_ecs)
    except Exception as e:
        logging.info(f"Error getting secrets for snowflake connection: {e}")
        raise SnowflakeError        
    
    logging.info(f'DML: {update_audit_table}')

    try:
        conn = snowflake.connector.connect (
            user=secrets['user'],
            password=secrets['password'],
            account=secrets['account'],
            warehouse=secrets['warehouse'],
            database=secrets['database'],
            schema=secrets['schema'],
            role=secrets['role'],
            insecure_mode=True
        )
    except Exception as e:
        logging.info(f'Snowflake Connectivity error: {e}')
        raise SnowflakeError

    cur = conn.cursor()
    try:
        response = cur.execute(update_audit_table)
        logging.info(f"Success {response}")
    except Exception as e:
        logging.info(f'Snowflake error for {cur.sfqid} with error {e}')
        raise SnowflakeError

enable_logging()

logging.info(f'***BEGIN of RaaS data pull from workday module***')

try:
    event=parse_arguments(sys.argv)
    logging.info(f'Job parameter extract complete: {event}')

    start_time = dt.now(pytz.utc) - timedelta(hours=7)
    payload_content=event['dummy_key']

    if len(payload_content) != 6:
        logging.info(f'Invalid argument count. Check the argument count in event payload')
        raise InvalidArgumentCount

    # Assigning variables based on data from event payload
    pk_raas_hash_key=payload_content[0]
    integration_type=payload_content[1]
    integration_name=payload_content[2]
    batch_mode=payload_content[3]
    executed_for=payload_content[4]
    integration_url=payload_content[5]
    description=integration_url.split('?')[1]
    integration_user_name="ISU_"+integration_name

    logging.info(f'Variable assignment complete from event payload')


    # Assigning bucket name, fetching secrets, keypath based on intrgration type & integration name
    if integration_type=="non-pii":
        bucket_name=os.environ['RAW_NONPII_BUCKET']
        secrets=os.environ['WORKDAY_NONPII_KEYS']
    elif integration_type=="pii":
        bucket_name=os.environ['RAW_PII_BUCKET']
        secrets=os.environ['WORKDAY_PII_KEYS']

    secrets=ast.literal_eval(secrets)
    auth_pass = secrets[integration_user_name]
    logging.info(f"Workday raas credentials pulled from secrets")

    # Initiating connection to workday RaaS by passing the URL from event payload
    rass_start_time = dt.now(pytz.utc) - timedelta(hours=7)
    response_object = pull_data_from_workday(integration_url,integration_user_name, auth_pass,integration_name,executed_for,bucket_name)
    rass_end_time = dt.now(pytz.utc) - timedelta(hours=7)

    scoped_json_response=response_object["Report_Entry"]
    
    # RUN THIS ONLY FOR THE FIRST TIME OF INTEGRATION, COMMENT NEXT TIME
    # To obtain the structure of the JSON to input for raw to formatted lambda
    #json_structure=get_json_structure(scoped_json_response)
    #logging.info(f'JSON STRCTURE: {json_structure}')
    # COMMENT UNTIL HERE
    
    # Determining record count for the RaaS pull 
    raas_record_count=len(scoped_json_response)
    logging.info(f'{raas_record_count} records found from the RaaS pull')

    # Check & split the entire RaaS response by 1000 records for each file for the lambda function to process
    if raas_record_count == 0:
        logging.info(f'zero records pulled from RaaS, Skipping S3 write. Exiting job')

        end_time=dt.now(pytz.utc) - timedelta(hours=7)
        print_audit_log(integration_type,integration_name,batch_mode,executed_for,raas_record_count,description,"empty-file",0,start_time,end_time,rass_start_time,rass_end_time,0)
        
        update_build_and_trigger_audit_table(pk_raas_hash_key,'SUCCESS')

    else:
        # Getting file size in mb rounded off to 2 decimals to determine file split size
        file_size=round((sys.getsizeof(json.dumps(scoped_json_response)) - sys.getsizeof(""))/(1024*1000),2)

        if file_size <= 10:
            # Generating the S3 key path with file name
            key_prefix="workday"+"/"+integration_name.split("_")[0].lower()+"/"+integration_name.split("_",1)[1].replace("_","").lower()+"/"
            file_name=integration_name+"_"+executed_for+"-"+dt.strftime((dt.now(pytz.utc) - timedelta(hours=7)),"%Y%m%d%H%M%S%f")+".json"
            key_prefix+=file_name

            # Writing data to S3 bucket as json string object
            try:
                s3 = boto3.resource('s3') 
                obj = s3.Object(bucket_name,key_prefix) 
                obj.put(Body=json.dumps(response_object))
                logging.info(f'S3 upload success: {raas_record_count} records written to {bucket_name}/{key_prefix}')
            except Exception as e:
                logging.info(f'S3 put failed for {event} payload for {key_prefix} key with error {e}')
                raise S3PutError

            end_time=dt.now(pytz.utc) - timedelta(hours=7)
            print_audit_log(integration_type,integration_name,batch_mode,executed_for,raas_record_count,description,file_name,0,start_time,end_time,rass_start_time,rass_end_time,file_size)
            update_build_and_trigger_audit_table(pk_raas_hash_key,'SUCCESS')
        else:
            # Determining the split count if each split has to be 10 mb
            file_split_count=math.ceil(file_size/10)

            # Determining the record count for each 10 mb / split
            record_split_size=math.ceil(raas_record_count/file_split_count)

            # Determining the file split by record count
            split_size=math.ceil(raas_record_count/record_split_size)
            logging.info(f'Huge file, splitting it into {split_size} files')

            chunks=split_files_by_recs(scoped_json_response,split_size)

            for iter,each_chunk in enumerate(chunks):
                raas_record_count=len(each_chunk)
                json_output_to_s3={"Report_Entry":each_chunk}
                file_size=round((sys.getsizeof(json.dumps(json_output_to_s3)) - sys.getsizeof(""))/(1024*1000),2)
                logging.info(f'Initiating S3 put request with API response')

                # Generating the S3 key path with file name
                key_prefix="workday"+"/"+integration_name.split("_")[0].lower()+"/"+integration_name.split("_",1)[1].replace("_","").lower()+"/"
                file_name=integration_name+"_"+executed_for+"-"+dt.strftime((dt.now(pytz.utc) - timedelta(hours=7)),"%Y%m%d%H%M%S%f")+"-split"+str(iter)+".json"
                key_prefix+=file_name

                # Writing data to S3 bucket as json string object
                try:
                    s3 = boto3.resource('s3') 
                    obj = s3.Object(bucket_name,key_prefix) 
                    obj.put(Body=json.dumps(json_output_to_s3))
                    logging.info(f'S3 upload success: {raas_record_count} records written to {bucket_name}/{key_prefix}')
                except Exception as e:
                    logging.info(f'S3 put failed for {event} payload for {key_prefix} key with error {e}')
                    raise S3PutError

                end_time=dt.now(pytz.utc) - timedelta(hours=7)
                print_audit_log(integration_type,integration_name,batch_mode,executed_for,raas_record_count,description,file_name,iter,start_time,end_time,rass_start_time,rass_end_time,file_size)
            
            update_build_and_trigger_audit_table(pk_raas_hash_key,'SUCCESS')

except (SnowflakeError,InvalidArgumentCount,WorkdayConnetionFailure,S3PutError):
    update_build_and_trigger_audit_table(pk_raas_hash_key,'FAILED')
    logging.error("RAAS_DATA_PULL_ERROR, Job failed")
except Exception as e:
    update_build_and_trigger_audit_table(pk_raas_hash_key,'FAILED')
    logging.error("RAAS_DATA_PULL_ERROR, Job failed", e)