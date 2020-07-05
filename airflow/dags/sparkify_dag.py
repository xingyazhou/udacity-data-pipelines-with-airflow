from datetime import datetime, timedelta
import datetime
import os
from airflow import conf
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# Default args 
default_args = {
    'owner': 'xingya-zhou',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sparkify_dag',
    default_args = default_args,
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


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[ "songplays", "songs", "artists",  "time", "users"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  \
    >> create_trips_table \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [ load_songs_table, load_artists_table, load_time_table, load_users_table] \
    >> run_quality_checks \
    >> end_operator

    

 
