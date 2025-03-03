import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, expr, to_timestamp, date_format, dayofweek, hour
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField, StringType, BooleanType
import pandas as pd
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename="prediction_errors.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load the saved models and encoders
try:
    svm_model = joblib.load(r"C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\notebooks\svm_model.joblib")
    encoder_merchant = joblib.load(r"C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\notebooks\encoder_merchant.joblib")
    encoder_category = joblib.load(r"C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\notebooks\encoder_category.joblib")
    encoder_gender = joblib.load(r"C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\notebooks\encoder_gender.joblib")
    encoder_job = joblib.load(r"C:\Users\LOQ\Desktop\data_engineering\online_transactions_fraud_detection\notebooks\encoder_job.joblib")
except Exception as e:
    logging.error(f"Error loading models or encoders: {e}")
    raise e

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FraudDetectionToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.jars", r"C:\Users\LOQ\BigData\jars\postgresql-42.7.4.jar") \
    .enableHiveSupport() \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Define schema without is_fraud column
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("trans_date_trans_time", StringType(), True),
    StructField("cc_num", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("city_pop", DoubleType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("trans_num", StringType(), True),
    StructField("unix_time", IntegerType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True),
    StructField("is_fraud", IntegerType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud-detection") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse and split the Kafka message
parsed_data = df.selectExpr("CAST(value AS STRING)") \
    .select(expr("split(value, ',') as values")) \
    .select([col("values")[i].alias(schema.fields[i].name) for i in range(len(schema.fields))])

# Apply prediction model UDFs (same as before)
@pandas_udf(IntegerType())
def encode_merchant_udf(merchant_series):
    return merchant_series.apply(lambda x: encoder_merchant.transform([x])[0] if x in encoder_merchant.classes_ else -1)

@pandas_udf(IntegerType())
def encode_category_udf(category_series):
    return category_series.apply(lambda x: encoder_category.transform([x])[0] if x in encoder_category.classes_ else -1)

@pandas_udf(IntegerType())
def encode_gender_udf(gender_series):
    return gender_series.apply(lambda x: encoder_gender.transform([x])[0] if x in encoder_gender.classes_ else -1)

@pandas_udf(IntegerType())
def encode_job_udf(job_series):
    return job_series.apply(lambda x: encoder_job.transform([x])[0] if x in encoder_job.classes_ else -1)

@pandas_udf(BooleanType())
def predict_fraud_udf(amt, merchant, category, gender, city_pop, job):
    features = pd.DataFrame({
        "amt": amt,
        "merchant": merchant,
        "category": category,
        "gender": gender,
        "city_pop": city_pop,
        "job": job
    })
    # Convert to boolean (0 or 1)
    return pd.Series(svm_model.predict(features.values).astype(bool))

# Apply transformations
processed_data = parsed_data \
    .withColumn("trans_date", to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("cc_num", col("cc_num").cast("bigint")) \
    .withColumn("amt", col("amt").cast("double")) \
    .withColumn("city_pop", col("city_pop").cast("integer")) \
    .withColumn("dob", to_timestamp(col("dob"), "yyyy-MM-dd")) \
    .withColumn("day_of_week", date_format(col("trans_date"), "EEEE")) \
    .withColumn("hour_of_day", hour(col("trans_date")))

# Apply prediction model
prediction_features = processed_data \
    .withColumn("merchant_encoded", encode_merchant_udf(col("merchant"))) \
    .withColumn("category_encoded", encode_category_udf(col("category"))) \
    .withColumn("gender_encoded", encode_gender_udf(col("gender"))) \
    .withColumn("job_encoded", encode_job_udf(col("job"))) \
    .withColumn("is_fraud", predict_fraud_udf(
        col("amt"),
        col("merchant_encoded"),
        col("category_encoded"),
        col("gender_encoded"),
        col("city_pop"),
        col("job_encoded")
    ))

# PostgreSQL credentials
database_url = "jdbc:postgresql://localhost:5433/airflow"
username = "postgres"
password = "postgres"

# Function to write data to PostgreSQL tables
def write_to_postgres(batch_df, batch_id):
    try:
        # Register the batch as a temporary view
        batch_df.createOrReplaceTempView("transactions_temp")
        
        # 1. Extract and insert customers dimension
        customers_df = spark.sql("""
            SELECT DISTINCT 
                cc_num,
                first AS first_name,
                last AS last_name,
                gender,
                dob,
                job,
                street,
                city,
                state,
                zip AS zip_code,
                city_pop
            FROM transactions_temp
        """)
        
        customers_df.write \
            .format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "customers") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .option("batchsize", 1000) \
            .option("truncate", "false") \
            .option("stringtype", "unspecified") \
            .save()
        
        # 2. Extract and insert merchants dimension
        merchants_df = spark.sql("""
            SELECT DISTINCT 
                merchant AS merchant_name,
                category,
                merch_lat,
                merch_long
            FROM transactions_temp
        """)
        
        merchants_df.write \
            .format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "merchants") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .option("batchsize", 1000) \
            .option("truncate", "false") \
            .save()
        
        # 3. Extract and insert locations dimension
        locations_df = spark.sql("""
            SELECT DISTINCT 
                city,
                state,
                zip AS zip_code,
                lat,
                long,
                city_pop
            FROM transactions_temp
        """)
        
        locations_df.write \
            .format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "locations") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .option("batchsize", 1000) \
            .option("truncate", "false") \
            .save()
        
        # 4. Extract and insert transactions
        transactions_df = spark.sql("""
            SELECT
                trans_num,
                trans_date,
                cc_num,
                merchant AS merchant_name,
                amt,
                unix_time,
                CAST(is_fraud AS BOOLEAN) AS is_fraud
            FROM transactions_temp
        """)
        
        transactions_df.createOrReplaceTempView("transactions_stage")
        
        # Join with merchants to get merchant_id and insert transactions
        spark.sql("""
            SELECT 
                t.trans_num,
                t.trans_date,
                t.cc_num,
                m.merchant_id,
                t.amt,
                t.unix_time,
                t.is_fraud
            FROM transactions_stage t
            JOIN (
                SELECT merchant_id, merchant_name 
                FROM merchants
            ) m ON t.merchant_name = m.merchant_name
        """).createOrReplaceTempView("transactions_with_ids")
        
        spark.sql("SELECT * FROM transactions_with_ids").write \
            .format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "transactions") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .option("batchsize", 1000) \
            .option("truncate", "false") \
            .save()
        
        # 5. Create and insert fact table records
        fact_df = spark.sql("""
            WITH customer_ids AS (
                SELECT cc_num, customer_id FROM customers
            ),
            merchant_ids AS (
                SELECT merchant_name, merchant_id FROM merchants
            ),
            location_ids AS (
                SELECT zip_code, location_id FROM locations
            ),
            transaction_ids AS (
                SELECT trans_num, transaction_id FROM transactions
            )
            SELECT
                t.transaction_id,
                c.customer_id,
                m.merchant_id,
                l.location_id,
                tt.trans_date,
                tt.amt,
                tt.category,
                tt.is_fraud,
                tt.day_of_week,
                tt.hour_of_day
            FROM transactions_temp tt
            JOIN customer_ids c ON tt.cc_num = c.cc_num
            JOIN merchant_ids m ON tt.merchant = m.merchant_name
            JOIN location_ids l ON tt.zip = l.zip_code
            JOIN transaction_ids t ON tt.trans_num = t.trans_num
        """)
        
        fact_df.write \
            .format("jdbc") \
            .option("url", database_url) \
            .option("dbtable", "fact_transactions") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .option("batchsize", 1000) \
            .option("truncate", "false") \
            .save()
        
        logging.info(f"Successfully processed batch {batch_id}")
        
    except Exception as e:
        logging.error(f"Error processing batch {batch_id}: {e}")
        raise e

# Define the output stream with foreach batch
query = prediction_features.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", "checkpoint-dir") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()