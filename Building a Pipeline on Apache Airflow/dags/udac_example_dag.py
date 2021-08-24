from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
    LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    catchup = False
    )
'''

dag = DAG(
    'example_dag3',
    start_date = datetime(2021,8,11),
    schedule_interval = '@daily',
    catchup = False
)
'''
#start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
    )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table = 'staging_events',
    s3_bucket="udacity-dend",
    #s3_key="log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-{execution_date.day}-events.json",
    s3_key = "log_data",
    json_path = "s3://udacity-dend/log_json_path.json",
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    target_table = 'staging_songs',
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path = 'auto',
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.songplay_table_insert,
    table_name = 'songplays',
    #insert_columns = 'playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    )

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql_query = SqlQueries.user_table_insert,
   # insert_columns = 'userid, first_name, last_name, gender, level',
    truncate = True
    )

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    sql_query = SqlQueries.song_table_insert,
   # insert_columns = 'songid, title, artistid, year, duration',
    truncate = True
    )

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    sql_query = SqlQueries.artist_table_insert,
   # insert_columns = 'artistid, name, location, lattitude, longitude',
    truncate = True
    )

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql_query = SqlQueries.time_table_insert,
   # insert_columns = 'start_time, hour, day, week, month, year, weekday',
    truncate = True
    )
'''
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
    )
'''
#end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
