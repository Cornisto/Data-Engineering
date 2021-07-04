from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 3),
    'end_date': datetime(2021, 7, 5),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
    'catchup'=False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='staging_events',
    create_stmt=SqlQueries.staging_events_table_create,
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path="s3://udacity-dend/log_json_path.json",
    file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='staging_songs',
    create_stmt=SqlQueries.staging_songs_table_create,
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path="auto",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    create_stmt=SqlQueries.songplays_table_create,
    sql=SqlQueries.songplays_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    create_stmt=SqlQueries.users_table_create,
    sql=SqlQueries.users_table_insert,
    insert_mode='delete-load'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    create_stmt=SqlQueries.songs_table_create,
    sql=SqlQueries.songs_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    create_stmt=SqlQueries.artists_table_create,
    sql=SqlQueries.artists_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    create_stmt=SqlQueries.time_table_create,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': 'SELECT EXISTS (SELECT 1 FROM songplays LIMIT 1)', 'expected_result': 1},
        {'check_sql': 'SELECT EXISTS (SELECT 1 FROM users LIMIT 1)', 'expected_result': 1},
        {'check_sql': 'SELECT EXISTS (SELECT 1 FROM songs LIMIT 1)', 'expected_result': 1},
        {'check_sql': 'SELECT EXISTS (SELECT 1 FROM artists LIMIT 1)', 'expected_result': 1},
        {'check_sql': 'SELECT EXISTS (SELECT 1 FROM time LIMIT 1)', 'expected_result': 1},
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE first_name IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM artists WHERE name IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM time WHERE month IS NULL', 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
