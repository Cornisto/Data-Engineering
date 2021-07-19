from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 create_stmt="",
                 s3_bucket="",
                 s3_key="",
                 json_path=None,
                 delimiter=",",
                 ignore_headers=1,
                 file_type="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.create_stmt = create_stmt
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.file_type = file_type
        
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Creating table if not exists: {self.table}")
        redshift.run(self.create_stmt)
        
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        
        if self.file_type == "csv":
            sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                REGION 'us-west-2'
                IGNOREHEADER {self.ignore_headers}
                DELIMITER '{self.delimiter}';
            """
        else:
            sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                REGION 'us-west-2'
                JSON '{self.json_path}';
            """

        redshift.run(sql)
        




