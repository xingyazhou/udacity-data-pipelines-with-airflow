from datetime import datetime, timedelta
import datetime
#import logging
import os
from airflow import conf
from airflow import DAG
#from airflow.contrib.hooks.aws_hook import AwsHook
#from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# Default args 

default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime.now(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}


dag = DAG(
    'sparkify_dag',
    start_date = datetime.datetime.now()
    

)

f= open(os.path.join(conf.get('core','dags_folder'),'create_tables.sql'))
create_tables_sql = f.read()

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,  
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    json_path="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,    
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    append_only=False
)

load_songs_table = LoadDimensionOperator(
    task_id='Load_songs_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    append_only=False
)


load_users_table = LoadDimensionOperator(
    task_id='Load_users_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    append_only=False
)

load_artists_table = LoadDimensionOperator(
    task_id='Load_artists_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    append_only=False
)

staging_quality_checks = DataQualityOperator(
    task_id='staging_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[ "staging_events", "staging_songs"]
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[ "songplays", "songs", "artists", "users", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  \
    >> create_trips_table \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> staging_quality_checks \
    >> load_songplays_table \
    >> [load_users_table, load_songs_table, load_artists_table, load_time_table] \
    >> run_quality_checks \
    >> end_operator

    

 
