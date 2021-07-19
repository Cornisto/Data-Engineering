from airflow.contrib.hooks.aws_hook import AwsHook
import logging
import boto3
from botocore.exceptions import ClientError


def create_bucket(bucket_name, region=None):
    """
        Create an S3 bucket in the chosen region
        
        Parameters:
            bucket_name: Bucket to create
            region: String region to create bucket in, e.g., 'us-east-1'
            KEY: The AWS access key
            SECRET: AWS secret access key
        Returns:
            s3_client upon creation, exit otherwise
    """
    try:
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
    except Exception:

    ## Creating the bucket 
    try:
        if region is None:
            s3_client = boto3.client('s3', 
                                     aws_access_key_id=credentials.access_key,
                                     aws_secret_access_key=credentials.secret_key)
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', 
                                     region_name=region,
                                     aws_access_key_id=credentials.access_key,
                                     aws_secret_access_key=credentials.secret_key)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
        logging.info('Successfully created S3 bucket')
    except ClientError as e:
        logging.error('Could not create S3 bucket')
        logging.error(e)
        exit()

    