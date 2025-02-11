import ast
import logging
from datetime import datetime as dt
from datetime import timedelta
import pytz
from custom_exceptions import *
import sys
import warnings
import pandas as pd
import awswrangler as wr
import json
import os
import numpy
from datetime import date

def enable_logging():
    global verbose
    verbose = "true"
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)
    
    warnings.filterwarnings('ignore')
        
def parse_arguments(argument_string):

    logging.info(f'Fetching job parameters from arguments')

    try:
        logging.info(f'argument_string: {argument_string}')

        argument_string.pop(0)
        dict_str=""
        
        for each_argument in argument_string:
            dict_str+=each_argument
        
        event=json.loads(dict_str)
        return event
    except Exception as e:
        logging.error(f'Argument parsing failed with error: {e}')
        raise Generic_Error

def validate_format_and_rowcount(source_type_filetype, filename):
    """
    Validates that data is the expected filetype and that there are rows to process
    :param source_type_filetype: string
    :param filename: s3 filepath
    :return: has_rows: boolean, raw_df: dataframe
    """

    if filename.endswith('$folder$'):
        raise AWSFolderPlaceholder
    elif source_type_filetype == 'csv':
        try:
            raw_df = wr.s3.read_csv(path=f'{filename}')
        except Exception as e:
            logging.error(f'Error reading the {filename} file {e}')
    else:
        logging.error(f'Unknown file format to process for {filename} file')
        raise Invalid_file_format
            
    has_rows = True if raw_df.shape[0] > 0 else False
    logging.info(f'*** Processing {raw_df.shape[0]} from the raw bucket.')

    return has_rows, raw_df

def add_timestamp_partitions(df, date_column, date_format, partition_list):
    """
    Parses given column using the strftime formatted string and adds new column to dataframe for each time level in partition_list
    :param df: dataframe
    :param date_column: 'string' or 'list'
    :param date_format: strftime string see https://strftime.org/
    :param partition_list: list of which time levels to partition ex ['year','month','day']
    :return: df now with partition columns
    """

    # Adding dummy date column for partitioning in formatted bucket
    today=(dt.now(pytz.utc) - timedelta(hours=7)).strftime("%Y-%m-%d")
    df['S3PartitionDate']=today
    
    if type(date_column) == str and date_column in df.columns:
        for partition_level in partition_list:
            if date_format == 'isoformat':
                df['temp'] = pd.to_datetime(df[date_column], infer_datetime_format=True)
            else:
                df['temp'] = pd.to_datetime(df[date_column], format=date_format)
            if partition_level == 'year':
                df['year'] = pd.DatetimeIndex(df['temp']).year
            elif partition_level == 'month':
                df['month'] = pd.DatetimeIndex(df['temp']).month
            elif partition_level == 'day':
                df['day'] = pd.DatetimeIndex(df['temp']).day
            elif partition_level == 'hour':
                df['hour'] = pd.DatetimeIndex(df['temp']).hour
            elif partition_level == 'minute':
                df['minute'] = pd.DatetimeIndex(df['temp']).minute
            elif partition_level == 'second':
                df['second'] = pd.DatetimeIndex(df['temp']).second
        df = df.drop('temp',axis=1)
        return df
    else:
        raise InvalidDatePartition

def print_audit_log(start_time,end_time,source_filename,source_rowcount,insert_rowcount,status,rejected_rowcount,event,target_filename):
    """
    Prints audit log with keyword to be picked up by cloudwatch filter
    :param source_type: string
    :param start_time: datetime object
    :param end_time: datetime object
    :param source_filename: string
    :param target_filename: string
    :param table_name: string
    :param source_rowcount: integer
    :param insert_rowcount: integer
    :param rejected_rowcount: integer - default 0
    :param status: string
    """
    
    audit_json = {
        "pipeline_name":"main",
        "pipeline_step":"ecs-to-formatted",
        "audit_insert_timestamp":"", # set within audit-lambda
        "event":event,
        "start_time":start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        "end_time":end_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        "source_filename":source_filename,
        "source_table":"",
        "target_filename":target_filename,
        "target_table":"",
        "source_rowcount":source_rowcount,
        "insert_rowcount":insert_rowcount,
        "update_rowcount":0,
        "delete_rowcount":0,
        "rejected_rowcount":rejected_rowcount,
        "query_id":"",
        "batch_id":"",
        "status":status.upper()
    }
    logging.info(f'AUDIT_LOG_INFORMATION: {audit_json}')

def make_columns_strings(df):
    """
    Changes all columns of a given dataframe into strings and keeps null values as nulls
    :param df: dataframe
    :return: string_df: dataframe
    """
    string_df = df
    for column in df.columns:
        string_df[column] = df[column].astype('string')
    try:
        string_df.replace('nan',numpy.nan,inplace=True)
        string_df.replace('<NA>',numpy.nan,inplace=True)
        string_df.replace('null',numpy.nan,inplace=True)
        string_df.replace('None',numpy.nan,inplace=True)
        string_df.replace('NaT',numpy.nan,inplace=True)
        # see documentation https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        string_df.fillna(pd.NA,inplace=True)
    except Exception as e:
        logging.warning('There as been an error with the pandas value NA. Please see documentation https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html\nNull values will be written as their string counterpart so as not to break the pipeline with datatype mismatch in Athena.')
        raise Generic_Error
    logging.info('***Casting data*** casting all columns as strings')
    return string_df

def upload_files_tos3(raw_df,formatted_bucket_name,prefix,partition_columns):

    logging.info(f'Uploading files to {formatted_bucket_name} bucket')

    try:
        string_df = make_columns_strings(raw_df)
        response = wr.s3.to_parquet(df=string_df,path=f's3://{formatted_bucket_name}/{prefix}',dataset=True,partition_cols=partition_columns)
        target_file=response['paths'][0]

        logging.info(f"{target_file} uploaded to {formatted_bucket_name} bucket successfully")
        return target_file
    except Exception as e:
        logging.info(f"Failed to connect to S3 bucket with exception {e}")
        raise S3PutError

if __name__ == '__main__':

    enable_logging()

    try:
        logging.info(f'***BEGIN of Kronos module***')
        logging.info(f'Input Arguments: {sys.argv}')
        
        event=parse_arguments(sys.argv)

        logging.info(f'Job parameter extract complete: {event}')

        start_time = dt.now(pytz.utc) - timedelta(hours=7)

        # Getting the applicaiton name from the input arguments
        application_name=event['application_name']

        if application_name=='kronos':

            if len(event) != 5:
                logging.info(f'Invalid argument count. Check the argument count in event payload')
                raise InvalidArgumentCount
        
            # Generating variables from the input parameters 
            s3_file_name=event['s3_file_name']
            bucket_name=s3_file_name.split('/')[0]
            s3_path_after_bucket=s3_file_name.split('/',1)[1]
            file_format=s3_file_name.split('/')[-1].split('.')[-1]

            prefix = ('/').join(s3_path_after_bucket.split('/')[0:1+event['depth_of_folders_to_keep']])
            table_name = prefix.split('/')[event['depth_of_table_name']-1]
            s3_file_name="s3://"+s3_file_name

            # Loading data from S3 
            logging.info(f'Trying to load {s3_file_name} into the dataframe ')
            has_rows, raw_df=validate_format_and_rowcount(file_format, s3_file_name)

            if has_rows:
                logging.info(f'Adding partition key & file name to the dataframe')
                raw_df = add_timestamp_partitions(raw_df, event['partition']['column'], event['partition']['date_format'], event['partition']['partition_levels'])

                raw_df['raw_filename'] = s3_file_name
                formatted_bucket_name=os.environ['FORMATTED_NONPII_BUCKET']
                partition_columns=event['partition']['partition_levels']

                #Filter data from file based on the column TIATIM in the Kronos file
                #Data older then 30 days from current date needs to be discarded
                #Data for the last 30 days should be filtered and processed to the formatted bucket

                #Step1: Substring of TIATIM to get the date part alone in yyyymmdd format and convert type to datetime
                raw_df['TIATIM_SubStr'] = raw_df['TIATIM'].astype(str).str.slice(0, 8)
                raw_df['TIATIM_DT'] = pd.to_datetime(raw_df['TIATIM_SubStr'], format='%Y%m%d')

                #Step2: Calculate the date 30 days behind from today's date and convert to type datetime
                raw_df['REF_DT'] = date.today() - timedelta(days=30) 
                raw_df['REF_DT'] = pd.to_datetime(raw_df['REF_DT'], format='%Y-%m-%d')

                #Step3: Calculate the difference in days between Step1 and Step2
                raw_df['diff_days'] = (raw_df['TIATIM_DT'] - raw_df['REF_DT']) / numpy.timedelta64(1, 'D')

                #Step4: Calculate a flag for filtering data, if difference in days is >= 0, then yes else no
                raw_df['FLAG'] = raw_df['diff_days'].apply(lambda x: "Y" if x >= 0 else "N")

                #Step5: Filter data for Flag = Y
                raw_df = raw_df[raw_df['FLAG'] == 'Y']
                
                #Step6: Drop all calculated columns, so original dataframe is retained
                raw_df = raw_df.drop(columns=['TIATIM_SubStr', 'TIATIM_DT', 'REF_DT', 'diff_days', 'FLAG'])

                #Step7: Upload to formatted bucket
                target_file=upload_files_tos3(raw_df,formatted_bucket_name,prefix,partition_columns)
                print_audit_log(start_time,dt.now(pytz.utc) - timedelta(hours=7),s3_file_name,raw_df.shape[0],raw_df.shape[0],'SUCCESS',0,event['application_name'],target_file)
            else:
                logging.info(f'no records found in file, exiting job')
                raise EmptyFile

    except (InvalidArgumentCount):
        logging.error(f"NON_WORKDAY_KRONOS_ERROR, Invalid argument passed to the job. Argument: {event}. Exiting job")
    except InvalidDatePartition:
        logging.error(f"NON_WORKDAY_KRONOS_ERROR, Failed to add timestamp partitions. Exiting job")
    except Invalid_file_format:
        logging.error(f"NON_WORKDAY_KRONOS_ERROR, The expected file format is csv but received {file_format}. Exiting job")
    except S3PutError:
        logging.error(f"NON_WORKDAY_KRONOS_ERROR, S3 put failed. Exiting job")
    except (Generic_Error,EmptyFile):
        logging.error("NON_WORKDAY_KRONOS_ERROR")
    except Exception as e:
        logging.error("NON_WORKDAY_KRONOS_ERROR", e)