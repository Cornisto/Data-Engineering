from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class CreateS3Operator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 s3_credentials_id="",
                 bucket_name="",
                 region_name=None,
                 *args, **kwargs):

        super(CreateS3Operator, self).__init__(*args, **kwargs)
        self.s3_credentials_id = s3_credentials_id
        self.bucket_name = bucket_name
        self.region_name = region_name

    def execute(self, context):
        # Getting AWS credentials
        s3_hook = S3Hook(self.s3_credentials_id)
        bucket_exists = s3_hook.check_for_bucket(bucket_name=self.bucket_name)

        if bucket_exists:
            self.log.info('Bucket with specified name already exists')
        else:
            # Creating the bucket
            s3_hook.create_bucket(bucket_name=self.bucket_name, region_name=self.region_name)
            self.log.info('Bucket created successfully')