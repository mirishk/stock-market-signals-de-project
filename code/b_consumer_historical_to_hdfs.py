# Load historical data from kafka to hdfs- just for initial load
# Consume topic name - "historical_stock_prices"
#*****************************************************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from config import kafka_brokers, hdfs_port, hdfs_host, hdfs_user, symbol, kafka_spark
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = kafka_spark
os.environ['HADOOP_USER_NAME'] = hdfs_user
hdfs_path = f"hdfs://{hdfs_host}:{hdfs_port}"
print(hdfs_path)

symbol= symbol

spark = SparkSession \
    .builder \
    .appName("Save_Historical_Data_HDFS") \
    .config("spark.hadoop.fs.defaultFS", hdfs_path) \
    .getOrCreate()

bootstrap_servers = kafka_brokers
topic_name= "historical_stock_prices"

# Define schema for historical stock prices
historical_schema = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("date", T.DateType(), True),
    T.StructField("close_price", T.StringType(), True),
    T.StructField("volume", T.StringType(), True)
])

# Read historical stock prices from Kafka
historical_stock_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic_name) \
    .load() \
    .select(F.col('value').cast(F.StringType())) 

# Parse JSON messages and extract necessary fields
historical_df = historical_stock_df\
    .withColumn('parsed_json', F.from_json(F.col('value'), historical_schema)) \
    .select(F.col('parsed_json.*'))\
    .withColumn("close_price", F.col("close_price").cast("double")) \
    .withColumn("volume", F.col("volume").cast("integer").alias("volume"))

# Partitioning by year and month
partitioned_df = historical_df\
    .withColumn("year", F.year("date").cast("integer")) \
    .withColumn("month", F.month("date").cast("integer"))

partitioned_df.show()
# Save to HDFS partitioned by year and month
partitioned_df \
    .write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .parquet(f"{hdfs_path}/final-project/historical_stock_data/symbol={symbol}")

# Stop the Spark session
spark.stop()
