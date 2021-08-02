from airflow import DAG
from airflow.operators import BashOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from datetime import datetime, timedelta
from PRIVATE import bucket_name, bucket_key, aws_conn_id

# S3KeySensor monitors an S3 bucket for new files
# bucket_name is the base bucket name
# bucket_key is the path to the file(s) you want to monitor
# poke_interval is the refresh interval in seconds

# code keeps repeating itself if it detects the file name present in the S3 bucket
# our file is always going to be there; we need the trigger when an upload happens

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email': ['something@here.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_repeating_sensor', default_args=default_args, schedule_interval= '@once')

t1 = BashOperator(
    task_id='trigger_test',
    bash_command='echo "Hello World"',
    dag=dag)

t2 = BashOperator(
    task_id='repeat_test',
    bash_command='airflow trigger_dag s3_repeating_sensor',
    dag=dag)

sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_key=bucket_key,
    bucket_name=bucket_name,
    wildcard_match=True,
    aws_conn_id=aws_conn_id, # should match with Admin -> Connections -> aws_conn in Airflow UI
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

sensor >> t1 >> t2