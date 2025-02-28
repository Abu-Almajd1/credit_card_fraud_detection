from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default_args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "simple_dag",
    default_args=default_args,
    description="A simple DAG to test Airflow setup",
    schedule_interval=None,  # Runs daily
    catchup=False,
)

# Define a Python function to print a message
def print_hello():
    print("Hello, Airflow!")

# Create a task using PythonOperator
hello_task = PythonOperator(
    task_id="hello_task",
    python_callable=print_hello,
    dag=dag,
)

# Set task dependencies
hello_task
