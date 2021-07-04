import boto3    
from create_s3 import get_key_secret


def get_s3(region, KEY, SECRET):
    """
        Getting the created s3, both client and resource
        
        Parameters:
            region: String region to create bucket in, e.g., 'us-east-1'
            KEY: The AWS access key
            SECRET: AWS secret access key
        Returns:
            s3 resource and client
    """
    s3_resource = boto3.resource('s3', 
                                 region_name=region,
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET)
    
    s3_client = boto3.client('s3', 
                             region_name=region,
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET)
    
    return s3_resource, s3_client


def empty_and_delete_bucket(s3_resource, s3_client, bucket_name):
    """
        Emptying and Deleting the bucket
        
        Parameters:
            s3_resource: The s3 resource (required to empty bucket)
            s3_client: the s3 client (required to delete bucket)
            bucket_name: Name of the s3 bucket to be deleted
    """
    
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.all().delete()
    print('****************************')
    print('Emptied Bucket ' + bucket_name)
    print('****************************')
    s3_client.delete_bucket(Bucket = bucket_name)
    print('****************************')
    print('Deleted Bucket ' + bucket_name)
    print('****************************')

    
if __name__ == '__main__':
    KEY, SECRET = get_key_secret()
    s3_resource, s3_client = get_s3('us-east-1', KEY, SECRET)
    empty_and_delete_bucket(s3_resource, s3_client, 'spark-bucket-cornisto')
    