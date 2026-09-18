from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello world!")
    return "Hello world!"

with DAG(
    dag_id="helloworld",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello,
    )
