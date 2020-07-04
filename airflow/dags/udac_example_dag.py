import datetime
import logging
import os
from airflow import conf
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators import (StageToRedshiftOperator)
from helpers import SqlQueries

dag = DAG(
    'test3',
    start_date=datetime.datetime.now()
)

f= open(os.path.join(conf.get('core','dags_folder'),'create_tables.sql'))
create_tables_sql = f.read()

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    data_format="JSON"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    data_format="JSON"
)




create_trips_table  \
    >> [stage_events_to_redshift, stage_songs_to_redshift]
 
