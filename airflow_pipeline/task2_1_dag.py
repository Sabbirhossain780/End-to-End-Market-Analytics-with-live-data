import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), 'scripts')

default_args = {
    'owner': 'sabbir',
    'retries': 0,
}

def sleep_task(**kwargs):
    tid = kwargs['task_instance'].task_id
    print(f"Task {tid}: sleeping for 3 seconds")
    time.sleep(3)
    print(f"Task {tid}: done sleeping")

def print_task(**kwargs):
    tid = kwargs['task_instance'].task_id
    print(f"Task {tid}: Hello from print_task!")

def count_task(**kwargs):
    tid = kwargs['task_instance'].task_id
    count = 0
    for i in range(10):
        count += 1
    print(f"Task {tid}: counted up to {count}")

with DAG(
    dag_id='task2_1_dag',
    default_args=default_args,
    description='12-task DAG for Assignment 2 Task 2.1',
    start_date=datetime(2026, 1, 1),
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['assignment2', 'task2.1'],
) as dag:

    t1 = BashOperator(task_id='t1', bash_command='echo "t1: starting the pipeline"')
    t2 = BashOperator(task_id='t2', bash_command='echo "t2: running"')
    t3 = BashOperator(task_id='t3', bash_command=f'bash {SCRIPTS_DIR}/hello.sh; echo "t3: script completed"')
    t4 = PythonOperator(task_id='t4', python_callable=print_task)
    t5 = BashOperator(task_id='t5', bash_command=f'python3 {SCRIPTS_DIR}/counter.py')
    t6 = BashOperator(task_id='t6', bash_command='echo "t6: leaf task done"')
    t7 = PythonOperator(task_id='t7', python_callable=sleep_task)
    t8 = PythonOperator(task_id='t8', python_callable=count_task)
    t9 = BashOperator(task_id='t9', bash_command='echo "t9: running"')
    t10 = BashOperator(task_id='t10', bash_command=f'bash {SCRIPTS_DIR}/hello.sh; echo "t10: script completed"')
    t11 = PythonOperator(task_id='t11', python_callable=print_task)
    t12 = PythonOperator(task_id='t12', python_callable=sleep_task)
    t13 = BashOperator(task_id='t13', bash_command=f'python3 {SCRIPTS_DIR}/counter.py')
    t14 = BashOperator(task_id='t14', bash_command='echo "t14: merging branches"')
    t15 = PythonOperator(task_id='t15', python_callable=count_task)
    t16 = PythonOperator(task_id='t16', python_callable=print_task)
    t17 = BashOperator(task_id='t17', bash_command=f'bash {SCRIPTS_DIR}/hello.sh; echo "t17: script completed"')
    t18 = BashOperator(task_id='t18', bash_command=f'python3 {SCRIPTS_DIR}/counter.py')
    t19 = PythonOperator(task_id='t19', python_callable=sleep_task)

    t1 >> [t2, t3, t4, t5]
    t2 >> t6
    t3 >> t7
    t5 >> [t8, t9]
    t7 >> [t12, t13, t18]
    t8 >> t10
    t9 >> [t11, t12]
    t10 >> t14
    t11 >> [t14, t15]
    t12 >> t14
    t13 >> t18
    t14 >> [t16, t17]
    t15 >> t18
    t16 >> t19
    t17 >> t18
    t18 >> t19
