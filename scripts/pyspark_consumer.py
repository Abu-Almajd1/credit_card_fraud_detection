import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, expr
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField, StringType
import pandas as pd
import logging

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
    .appName("KafkaConsumerHive") \
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

# Define Pandas UDFs for encoding
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

@pandas_udf(DoubleType())
def predict_udf(amt, merchant, category, gender, city_pop, job):
    features = pd.DataFrame({
        "amt": amt,
        "merchant": merchant,
        "category": category,
        "gender": gender,
        "city_pop": city_pop,
        "job": job
    })
    return pd.Series(svm_model.predict(features.values))

# Apply transformations and encoding
encoded_data = parsed_data \
    .withColumn("merchant", encode_merchant_udf(col("merchant"))) \
    .withColumn("category", encode_category_udf(col("category"))) \
    .withColumn("gender", encode_gender_udf(col("gender"))) \
    .withColumn("job", encode_job_udf(col("job"))) \
    .withColumn("prediction", predict_udf(
        col("amt"),
        col("merchant"),
        col("category"),
        col("gender"),
        col("city_pop"),
        col("job")
    ))

# Drop is_fraud column (not included in schema)
final_data = encoded_data.drop("is_fraud")


df = spark.createDataFrame(final_data, schema)

# PostgreSQL credentials
database_url = "jdbc:postgresql://localhost:5433/airflow"
username = "airflow"
password = "airflow"
table_name = "fraud_detection_table_ml"

df.write \
    .format("jdbc") \
    .option("url", database_url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Table created and data inserted successfully!")
