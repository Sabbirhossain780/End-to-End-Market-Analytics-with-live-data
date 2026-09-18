import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), 'scripts', 'task2_2')
sys.path.insert(0, SCRIPTS_DIR)

COMPANIES = ['AAPL', 'GOOGL', 'META', 'MSFT', 'AMZN']
COLUMN_NAMES = {
    'AAPL': 'APPLE',
    'GOOGL': 'GOOGLE',
    'META': 'META',
    'MSFT': 'MICROSOFT',
    'AMZN': 'AMAZON',
}

default_args = {
    'owner': 'sabbir',
    'retries': 0,
}

def fetch_data_task(**kwargs):
    from fetch_data import fetch_all
    fetch_all()

def train_predict_task(ticker, **kwargs):
    from train_predict import walk_forward_errors
    errors = walk_forward_errors(ticker)
    # XCom requires JSON-serializable values -> convert date keys to strings
    errors_serializable = {str(d): float(e) for d, e in errors.items()}
    kwargs['ti'].xcom_push(key=f'errors_{ticker}', value=errors_serializable)

def build_errors_csv_task(**kwargs):
    import pandas as pd
    ti = kwargs['ti']

    all_errors = {}
    for ticker in COMPANIES:
        all_errors[ticker] = ti.xcom_pull(task_ids=f'train_predict_{ticker}', key=f'errors_{ticker}')

    all_dates = sorted(set(d for errs in all_errors.values() for d in errs.keys()))

    rows = []
    for d in all_dates:
        row = {'Date': d}
        for ticker in COMPANIES:
            row[COLUMN_NAMES[ticker]] = all_errors[ticker].get(d)
        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = os.path.join(SCRIPTS_DIR, 'data', 'errors.csv')
    df.to_csv(out_path, index=True, index_label='')
    print(f"Errors CSV written to {out_path}")
    print(df)

with DAG(
    dag_id='task2_2_dag',
    default_args=default_args,
    description='yfinance stock price fetching, prediction, and error tracking pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['assignment2', 'task2.2'],
) as dag:

    fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_task,
    )

    predict_tasks = []
    for ticker in COMPANIES:
        t = PythonOperator(
            task_id=f'train_predict_{ticker}',
            python_callable=train_predict_task,
            op_kwargs={'ticker': ticker},
        )
        predict_tasks.append(t)

    build_csv = PythonOperator(
        task_id='build_errors_csv',
        python_callable=build_errors_csv_task,
    )

    fetch >> predict_tasks >> build_csv
