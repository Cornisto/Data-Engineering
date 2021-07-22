from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_s3 import CreateS3Operator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 19),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'catchup': False
}

dag = DAG('test_dag',
          default_args=default_args,
          description='Test DAG',
          schedule_interval='0 * * * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_s3_bucket = CreateS3Operator(
    task_id='create_s3_bucket',
    dag=dag,
    s3_credentials_id="s3_credentials",
    bucket_name='airflowbucketcornisto'
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_s3_bucket
create_s3_bucket >> end_operator
