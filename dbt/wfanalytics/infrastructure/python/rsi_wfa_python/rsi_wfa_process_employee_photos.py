import os
import ast
from multiprocessing import context
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime as dt
from datetime import timedelta
import pytz
from custom_exceptions import *
import sys
import warnings
from zipfile import ZipFile
from PIL import Image
import multiprocessing
import time

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
        argument_string=argument_string[1:]
        dict_str=""
        
        for each_argument in argument_string:
            dict_str+=each_argument
        
        event=ast.literal_eval(dict_str)
        return event
    except Exception as e:
        logging.error(f'Argument parsing failed with error: {e}')
        raise Generic_Error

def print_audit_log(start_time,end_time,source_filename,source_rowcount,insert_rowcount,status,rejected_rowcount):
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
    target_filename=f"s3://rsi-wfa-data-lake-formatted-non-pii-s3-{os.environ['ENVIRONMENT']}/employeephotos/"
    audit_json = {
        "pipeline_name":"main",
        "pipeline_step":"ecs-to-formatted",
        "audit_insert_timestamp":"", # set within audit-lambda
        "event":"employeephotos",
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

def download_file_from_s3(bucket_name, s3_path_after_bucket, local_path_with_file):
       
    try:
        s3 = boto3.resource(service_name='s3')
        bucket = s3.Bucket(bucket_name)
        logging.info(f'Started file download for {s3_path_after_bucket} file from S3 bucket {bucket_name}')
        bucket.download_file(s3_path_after_bucket, local_path_with_file)
        logging.info(f'File downloaded at location: {local_path_with_file}')

        return True
    except Exception as e:
        logging.error(f'Error while downloading file from s3, for file {s3_path_after_bucket}. Error: {e}')
        raise Generic_Error

def create_folders_for_processing(current_working_directory):
    main_processsing_folder=os.path.join(current_working_directory,"main_processsing_folder")

    try:
        if not os.path.exists(main_processsing_folder):
            os.makedirs(main_processsing_folder)
            logging.info(f'Main processing folder created {main_processsing_folder}')
    except Exception as e:
        logging.error(f'Error creating main processing folder {main_processsing_folder}. Error: {e}')
        raise Generic_Error
    
    local_unzipped_filefolder=os.path.join(main_processsing_folder,"unzipped_folder")

    try:
        if not os.path.exists(local_unzipped_filefolder):
            os.makedirs(local_unzipped_filefolder)
            logging.info(f'Unzip folder created {local_unzipped_filefolder}')
    except Exception as e:
        logging.error(f'Error creating unzip folder {local_unzipped_filefolder}. Error: {e}')
        raise Generic_Error

    local_resized_images_folder=os.path.join(main_processsing_folder,"resized_images")
    
    try:
        if not os.path.exists(local_resized_images_folder):
            os.makedirs(local_resized_images_folder)
            logging.info(f'Resized image folder created {local_resized_images_folder}')
    except Exception as e:
        logging.error(f'Error creating Resized image folder {local_resized_images_folder}. Error: {e}')
        raise Generic_Error

    logging.info(f'Folder creation completed for processing')
    return main_processsing_folder,local_unzipped_filefolder,local_resized_images_folder

def resize_and_thubnail_image(input_queue,resized_images_filepath_list,failed_raw_images_list):

    try:
        if input_queue.empty():
            logging.info(f'Queue is empty, exiting process {str(os.getpid())}')
        else:
            logging.info(f'Current working directory is {os.getcwd()}')
            each_image_path=input_queue.get()
            #file_name=each_image_path.split('\\')[-1] # local testing
            file_name=each_image_path.split('/')[-1]

            logging.info(f'Processing {file_name} from {each_image_path} with process id {str(os.getpid())}')

            each_image = Image.open(each_image_path)
            
            if each_image.mode=="RGBA":
                logging.info(f'{file_name} image contains alpha channel, Converting image with RGB channels')
                each_image = each_image.convert('RGB')
            
            resized_image=each_image.resize((250,250))

            local_resized_images_filepath=os.path.join(os.getcwd(),"main_processsing_folder","resized_images",file_name)
            resized_image.save(local_resized_images_filepath)
            logging.info(f'{file_name} image thumbnailed successfully and stored to {local_resized_images_filepath}')
            
            resized_images_filepath_list.append(local_resized_images_filepath)
    except Exception as e:
        failed_raw_images_list.append(each_image_path)
        logging.error(f'Failed processing {file_name} from {each_image_path} with process id {str(os.getpid())}')

def resize_images_in_parallel(images_to_resize_list,input_queue,resized_images_filepath_list,failed_raw_images_list,local_resized_images_folder):

    logging.info(f'Invoking parallel processing module to resize and thumbnail images')
    
    start=time.perf_counter()
    processes=[]

    logging.info(f'Inserting images to process to the multiprocessing queue')

    for i in images_to_resize_list:
        input_queue.put(i)

    logging.info(f'{input_queue.qsize()} images inserted into the multiprocessing queue for processing')
    
    pool_size=10
    logging.info(f'Setting up the parallel processing pool size to {pool_size}')

    while not input_queue.empty():
        for _ in range(pool_size):
            p = multiprocessing.Process(target=resize_and_thubnail_image,args=[input_queue,resized_images_filepath_list,failed_raw_images_list])
            p.start()
            processes.append(p)

        for process in processes:
            process.join()
        
        if input_queue.empty():
            break

    finish=time.perf_counter()
    logging.info(f'Image processing completed in {round(finish-start,2)} seconds')

    return resized_images_filepath_list,failed_raw_images_list

def upload_files_tos3(s3_file_upload_queue,s3_file_uploaded_list,s3_file_upload_failed_list):

    try:
        if s3_file_upload_queue.empty():
            logging.info(f'File upload queue is empty, exiting process {str(os.getpid())}')
        else:
            logging.info(f'Current working directory is {os.getcwd()}')
            each_image_path=s3_file_upload_queue.get()
            #file_name=each_image_path.split('\\')[-1] #local testing
            file_name=each_image_path.split('/')[-1] 

            logging.info(f'Uploading {file_name} from {each_image_path} to s3 bucket with process id {str(os.getpid())}')

            try:
                s3_bucket_name=os.environ['FORMATTED_NONPII_BUCKET']
                s3 = boto3.resource('s3')
                file_key=f"employeephotos/{file_name}"
                s3.Bucket(s3_bucket_name).upload_file(each_image_path,file_key)

                s3_file_uploaded_list.append(file_name)

            except Exception as e:
                logging.info(f"Failed to connect to S3 bucket with exception {e}")
            
            s3_file_uploaded_list.append(s3_file_uploaded_list)
    except Exception as e:
        s3_file_upload_failed_list.append(each_image_path)
        logging.error(f'Unable to upload file {file_name} from {each_image_path} to s3 bucket')

def upload_files_to_s3_in_parallel(op_resized_images_filepath_list,s3_file_upload_queue,s3_file_uploaded_list,s3_file_upload_failed_list):

    logging.info(f'Invoking parallel processing module to upload files to s3 bucket')
    
    start=time.perf_counter()
    processes=[]

    logging.info(f'Inserting images to upload to the multiprocessing queue')

    for i in op_resized_images_filepath_list:
        s3_file_upload_queue.put(i)

    logging.info(f'Inserted {s3_file_upload_queue.qsize()} images to the queue')
    
    pool_size=10
    logging.info(f'Setting up the parallel processing pool size to {pool_size}')

    while not s3_file_upload_queue.empty():
        for _ in range(pool_size):
            p = multiprocessing.Process(target=upload_files_tos3,args=[s3_file_upload_queue,s3_file_uploaded_list,s3_file_upload_failed_list])
            p.start()
            processes.append(p)

        for process in processes:
            process.join()
        
        if s3_file_upload_queue.empty():
            break

    finish=time.perf_counter()
    logging.info(f'File upload to s3 bucket completed in {round(finish-start,2)} seconds')
    logging.info(f'The values and lengths of s3_file_uploaded_list and s3_file_upload_failed_list are s3_file_uploaded_list: {s3_file_uploaded_list}, len(s3_file_uploaded_list): {len(s3_file_uploaded_list)} and s3_file_upload_failed_list: {s3_file_upload_failed_list}, len(s3_file_upload_failed_list): {len(s3_file_upload_failed_list)}')
    logging.info(f'The values and lengths of resized_images_filepath_list and failed_raw_images_list are resized_images_filepath_list: {resized_images_filepath_list}, len(resized_images_filepath_list): {len(resized_images_filepath_list)} and failed_raw_images_list: {failed_raw_images_list}, len(failed_raw_images_list): {len(failed_raw_images_list)}')

    return resized_images_filepath_list,failed_raw_images_list


if __name__ == '__main__':
    
    enable_logging()
    logging.info(f'***BEGIN of ECS Container module for Employee Photos***')

    try:
        logging.info(f'Input Arguments: {sys.argv}')
        event=parse_arguments(sys.argv)

        logging.info(f'Job parameter extract complete: {event}')

        start_time = dt.now(pytz.utc) - timedelta(hours=7)
        payload_content=event['dummy_key']

        # Getting the applicaiton name from the input arguments
        application_name=payload_content[0]
        if application_name=='employeephotos':

            if len(payload_content) != 2:
                logging.info(f'Invalid argument count. Check the argument count in event payload')
                raise InvalidArgumentCount
        
            # Generating variables from the input parameters 
            s3_file_name=payload_content[1]
            bucket_name=s3_file_name.split('/')[0]
            s3_path_after_bucket=s3_file_name.split(s3_file_name.split('/')[0]+"/",1)[1]
            zip_filename=s3_file_name.split('/')[-1]
            file_format=zip_filename.split('.')[-1]

            if file_format=='zip':

                # Creating 3 folders; master folder, store unzipped files and store processed files to be written back to S3 
                logging.info(f'Creating folders to store files locally for processing')
                current_working_directory=os.getcwd()
                main_processsing_folder,local_unzipped_filefolder,local_resized_images_folder=create_folders_for_processing(current_working_directory)

                # Connecting to S3 to download the zip file containing employee photos 
                # into local folder in ECS container 
                logging.info(f'Trying to download {s3_file_name} into ECS local')
                unzip_file_path=os.path.join(main_processsing_folder,zip_filename)
                file_download_status=download_file_from_s3(bucket_name, s3_path_after_bucket, unzip_file_path)

                if file_download_status:
                    # Unzipping the file from local folder in ECS container
                    logging.info(f'Unzipping {unzip_file_path} to {local_unzipped_filefolder}')
                    try:
                        with ZipFile(unzip_file_path,'r') as zObject:
                            zObject.extractall(path=local_unzipped_filefolder)
                        logging.info(f'Files unzipped to {local_unzipped_filefolder}')
                    except Exception as e:
                        logging.error(f'Error while unzipping file {unzip_file_path}. Error: {e}')

                    file_list = os.listdir(local_unzipped_filefolder)
                    file_count_in_zipfile=len(file_list)
                    logging.info(f'{file_count_in_zipfile} files to be processed')

                    # Resizing the image and writing it to the resized folder in ECS container
                    images_to_resize_list=[]

                    logging.info(f'Generating image file name list to process')

                    for dirpath, dirnames, files in os.walk(local_unzipped_filefolder):
                        for file_name in files:
                            each_image_path=os.path.join(local_unzipped_filefolder,file_name)
                            images_to_resize_list.append(each_image_path)

                    logging.info(f'Image file list generated to be resized with {len(images_to_resize_list)} images')
                    
                    logging.info(f'--- Begin of image resizing & thumbnailing ---')

                    input_queue = multiprocessing.Queue()
                    manager = multiprocessing.Manager()
                    
                    resized_images_filepath_list=manager.list()
                    failed_raw_images_list=manager.list()
                    
                    op_resized_images_filepath_list,op_failed_raw_images=resize_images_in_parallel(images_to_resize_list,input_queue,resized_images_filepath_list,failed_raw_images_list,local_resized_images_folder)
                    
                    if len(op_failed_raw_images)==0:
                        if len(images_to_resize_list)==(len(op_resized_images_filepath_list)+len(op_failed_raw_images)):
                            logging.info(f'All {len(images_to_resize_list)} images were resized & thumbnailed')
                    else:
                        logging.info(f'Unable to process {len(op_failed_raw_images)} images out of {len(images_to_resize_list)} images')
                        logging.info(f'Image list to take action for: {op_failed_raw_images} images')
                        raise image_processing_failed
                    
                    # Writing back the resized images back to the formatted S3 bucket
                    logging.info(f'--- Begin of file upload to s3 bucket ---')
                    
                    s3_file_upload_queue = multiprocessing.Queue()
                    s3_file_upload_manager = multiprocessing.Manager()

                    s3_file_uploaded_list=manager.list()
                    s3_file_upload_failed_list=manager.list()

                    op_s3_file_uploaded_list,op_s3_file_upload_failed_list = upload_files_to_s3_in_parallel(op_resized_images_filepath_list,s3_file_upload_queue,s3_file_uploaded_list,s3_file_upload_failed_list)

                    if len(op_s3_file_upload_failed_list)==0:
                        logging.info(f'The values and lengths of op_resized_images_filepath_list, op_s3_file_uploaded_list and op_s3_file_upload_failed_list are op_resized_images_filepath_list: {op_resized_images_filepath_list}, len(op_resized_images_filepath_list): {len(op_resized_images_filepath_list)} and op_s3_file_uploaded_list: {op_s3_file_uploaded_list}, len(op_s3_file_uploaded_list): {len(op_s3_file_uploaded_list)} and op_s3_file_upload_failed_list: {op_s3_file_upload_failed_list}, len(op_s3_file_upload_failed_list): {len(op_s3_file_upload_failed_list)}')
                        if len(op_resized_images_filepath_list)==(len(op_s3_file_uploaded_list)+len(op_s3_file_upload_failed_list)):
                            logging.info(f'All {len(op_resized_images_filepath_list)} images were uploaded to s3 bucket')
                    else:
                        logging.info(f'Unable to process {len(op_s3_file_upload_failed_list)} images out of {len(op_resized_images_filepath_list)} images')
                        logging.info(f'Image list to take action for: {op_s3_file_upload_failed_list} images')
                        raise S3PutError

                    # Inserting metrics to audit table on snowflake
                    logging.info(f'Logging data audit table')
                    print_audit_log(start_time,dt.now(pytz.utc) - timedelta(hours=7),s3_file_name,file_count_in_zipfile,len(op_resized_images_filepath_list),"SUCCESS",rejected_rowcount=0)
                else:
                    logging.error(f"Error downloading from s3 bucket. Exiting job")
                logging.info(f'Processed of {len(file_list)} images successfully')
            else:
                raise Invalid_file_format
            logging.info(f'***END of processing {application_name} module***')
        else:
            raise Unknown_Application_Name
    except (InvalidArgumentCount):
        logging.error(f"NON_WORKDAY_EMPLOYEE_PHOTOS_ERROR, Invalid argument passed to the job. Argument: {payload_content}. Exiting job")
    except image_processing_failed:
        logging.error(f"NON_WORKDAY_EMPLOYEE_PHOTOS_ERROR, Failed to process images: {op_failed_raw_images}. Exiting job")
    except Unknown_Application_Name:
        logging.error(f"NON_WORKDAY_EMPLOYEE_PHOTOS_ERROR, {application_name} application name is unknown.Argument: {payload_content}. Exiting job")
    except Invalid_file_format:
        logging.error(f"NON_WORKDAY_EMPLOYEE_PHOTOS_ERROR, The expected file format is .zip but received {file_format}. Exiting job")
    except (S3PutError,Generic_Error):
        logging.error(f"NON_WORKDAY_EMPLOYEE_PHOTOS_ERROR, S3 put failed. Exiting job")
    except Exception as e:
        logging.error("NON_WORKDAY_EMPLOYEE_PHOTOS_ERROR", e)