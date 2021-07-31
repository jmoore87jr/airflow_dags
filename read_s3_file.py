"""
I want to use the Airflow AWS connection to get files from S3
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from datetime import datetime, timedelta
import boto3

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

dag = DAG('get_s3_file', default_args=default_args, schedule_interval= '@once')

s3 = boto3.resource('s3')

def get_files_from_S3(bucket_name, object_name, file_name):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, file_name)
    body = obj.get()['Body'].read()

    print(body)
    

t1 = PythonOperator(
    task_id='read_s3_file',
    python_callable=get_files_from_S3,
    dag=dag,
    op_kwargs={
        'bucket_name': 'mnnk',
        'object_name': 'airflow/file-to-watch-1.txt',
        'file_name': 'airflow/file-to-watch-1.txt',
    })
