from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id="create_postgres_tables",
    start_date=datetime(2024, 3, 3),
    schedule_interval=None,  
    catchup=False
)

sql_file_path = "C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\scripts\data_modeling.sql"

# Read the SQL file
with open(sql_file_path, "r") as file:
    sql_script = file.read()


create_tables_task = PostgresOperator(
    task_id="create_postgres_tables",
    postgres_conn_id="fraud_detection_database",  
    sql=sql_script,
    dag=dag
)

create_tables_task