class Error(Exception):
    pass

class InvalidParameterCount(Error):
    """
    Is invoked if the parameters are set incorrectly on the config.json for date fields
    """
    pass

class InvalidDateRange(Error):
    """
    Is invoked if the dates are incorrect while finding the delta days
    """
    pass
class SnowflakeError(Error):
    """
    Is invoked when error is faced with Snowflake
    """

class InvalidArgumentCount(Error):
    """
    Is invoked when total number of expected arguments is incorrect
    """

class WorkdayConnetionFailure(Error):
    """
    Is invoked when connectivity to workday RaaS fails
    """

class S3PutError(Error):
    """
    Is invoked when S3 Put object fails
    """

class AWSFolderPlaceholder(Error):
    """
    Is invoked when the create event in the raw bucket is from a $folder$ placeholder object and not a true record. Event should not be processed. See https://aws.amazon.com/premiumsupport/knowledge-center/emr-s3-empty-files/ for an explanation.
    """
class image_processing_failed(Error):
    """
    Is invoked when the job fails to process images
    """
class Unknown_Application_Name(Error):
    """
    Is invoked when the application name is unknown to the program
    """
class Invalid_file_format(Error):
    """
    Is invoked when the file format is invalid
    """
class Generic_Error(Error):
    """
    Is invoked for genric errors
    """
class InvalidDatePartition(Error):
    """
    Is invoked if the given column to partition by date is not present in the dataframe or if the format of the partition does not match the given time format
    """
    pass

class EmptyFile(Error):
    """
    Is invoked if the file content is empty
    """
    pass