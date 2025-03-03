from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define DAG
dag = DAG(
    dag_id="kafka_spark_postgres_pipeline",
    start_date=datetime(2024, 3, 3),
    schedule_interval="*/1  * * * *",
    catchup=False
)


KAFKA_PRODUCER_SCRIPT = "C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\scripts\kafka_producer.py"  
PYSPARK_CONSUMER_SCRIPT = "C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\scripts\pyspark_consumer.py"  
POSTGRES_SQL_FILE = "C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\scripts\data_modeling.sql"  


start_kafka_producer = BashOperator(
    task_id="start_kafka_producer",
    bash_command=f"python {KAFKA_PRODUCER_SCRIPT}",
    dag=dag
)


consume_kafka_data = SparkSubmitOperator(
    task_id="consume_kafka_data",
    application=PYSPARK_CONSUMER_SCRIPT,
    conn_id="spark_default",  
    executor_cores=2,
    executor_memory="4g",
    driver_memory="2g",
    num_executors=2,
    dag=dag
)

with open(POSTGRES_SQL_FILE, "r") as file:
    sql_script = file.read()

load_data_to_postgres = PostgresOperator(
    task_id="load_data_to_postgres",
    postgres_conn_id="fraud_detection_database", 
    sql=sql_script,
    dag=dag
)


start_kafka_producer >> consume_kafka_data >> load_data_to_postgres
