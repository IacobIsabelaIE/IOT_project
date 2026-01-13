from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from Airflow 3!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,     # ✅ REQUIRED in Airflow 3
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )
