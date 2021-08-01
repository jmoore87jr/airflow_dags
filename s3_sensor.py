from airflow import DAG
from airflow.operators import BashOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta

# S3KeySensor monitors an S3 bucket for new files
# bucket_name is the base bucket name
# bucket_key is the path to the file(s) you want to monitor
# poke_interval is the refresh interval in seconds

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

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval= '@once')

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt ',
    dag=dag)

sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_key='airflow/file-to-watch-*',
    bucket_name='mnnk',
    wildcard_match=True,
    aws_conn_id='aws_conn',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

t1.set_upstream(sensor)