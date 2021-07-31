from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Test functions
def push_func(**kwargs):
    s = "Muffie the cat"
    kwargs['ti'].xcom_push(key='push string', value=s)

def pull_func(**kwargs):
    # only XComs with matching key and task_ids will be pulled
    ti = kwargs['ti']
    fetched_string = ti.xcom_pull(key='push string', task_ids=['push']) # match key with push func
    print(fetched_string)



# Airflow
default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='xcom_test',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    t1 = PythonOperator(
        task_id="push",
        provide_context=True, # necessary for getting 'ti' (task instance) parameters
        python_callable=push_func
    )

    t2 = PythonOperator(
        task_id="pull",
        provide_context=True, # necessary for getting 'ti' (task instance) parameters
        python_callable=pull_func
    )

# run tasks
t1 >> t2
