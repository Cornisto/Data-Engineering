import logging
import boto3
from botocore.exceptions import ClientError
import configparser


def get_key_secret():
    
    """
        Getting key and secret from the config file
        
        Returns: 
            AWS Access key id, 
            AWS secrety access key
    """
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    KEY = config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
    SECRET = config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']
    return KEY, SECRET


def create_bucket(bucket_name, KEY, SECRET, region=None):
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
    ## Creating the bucket 
    try:
        if region is None:
            s3_client = boto3.client('s3', 
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', 
                                     region_name=region,
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)           
    except ClientError as e:
        logging.error(e)
        print('Could not create S3 bucket')
        exit()
        
    print('************************************')
    print('S3 Client created')
    print('************************************')
    return s3_client


if __name__ == '__main__':
    
    KEY, SECRET = get_key_secret()
    
    s3_client = create_bucket('spark-bucket-cornisto',
                              KEY,
                              SECRET)
    