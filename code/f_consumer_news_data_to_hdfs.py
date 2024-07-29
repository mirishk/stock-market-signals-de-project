# Send news from kafka topic "stock_news_data" to hdfs partition by symbol, year, month, day
#*******************************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from config import kafka_brokers, hdfs_user, hdfs_host, hdfs_port, symbol, kafka_spark
import os 

os.environ['PYSPARK_SUBMIT_ARGS'] = kafka_spark
os.environ['HADOOP_USER_NAME'] = hdfs_user
hdfs_path = f"hdfs://{hdfs_host}:{hdfs_port}"
print(hdfs_path)

symbol= symbol

spark = SparkSession \
    .builder \
    .appName("Save_News_Data") \
    .config("spark.hadoop.fs.defaultFS", hdfs_path) \
    .getOrCreate()

bootstrap_servers = kafka_brokers
topic_name = "stock_news_data"
symbol= 'AAPL'
start_from_earliest= False
#-------------------------------------

base_path = f"{hdfs_path}//final-project/news_data/symbol={symbol}"
try:
    # Read data from the specified path
    df = spark.read.parquet(base_path)
    # Check if DataFrame is empty
    if df.count() == 0:
        raise ValueError("DataFrame is empty")

    # Find the maximum value of the timestamp column
    max_timestamp_row = df.agg(F.max(F.col("timestamp")).alias("max_timestamp")).collect()[0]

    # Extract max_timestamp from the Row object
    max_timestamp = max_timestamp_row["max_timestamp"]

    starting_offset_timestamp_ms = int(max_timestamp.timestamp() * 1000)

    # Convert the timestamp to a JSON string with the appropriate topic and partition offsets
    starting_offsets_json = '{"' + topic_name + '": {"0": ' + str(starting_offset_timestamp_ms) + '}}'

    print("Max Timestamp:", max_timestamp, " ",starting_offset_timestamp_ms, " ", starting_offsets_json)

except Exception as e:
    print("An error occurred:", str(e))
    print("Starting from earliest...")
    start_from_earliest = True
    max_timestamp = None
#---------------------------------------

# Define schema for news data
news_schema = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("publishedAt", T.TimestampType(), True),
    T.StructField("description", T.StringType(), True),
    T.StructField("url", T.StringType(), True),
    T.StructField("source_name", T.StringType(), True),
    T.StructField("content", T.StringType(), True)
])  

# If no max timestamp from HDFS, start from earliest
if start_from_earliest:
    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic_name,
        "startingOffsets": "earliest"
    }

else:
    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic_name,
        "startingOffsetsByTimestamp": starting_offsets_json
    }

news_df = spark \
            .read \
            .format("kafka") \
            .options(**kafka_options) \
            .load()

# Extract value and timestamp columns
parsed_news_df = news_df \
        .select(F.col("value").cast(F.StringType()), F.col("timestamp"))

# Parse JSON messages and extract necessary fields
parsed_news_df = parsed_news_df \
        .withColumn("parsed_json", F.from_json(F.col("value"), news_schema)) \
        .select(F.col("parsed_json.*"), F.col("timestamp"))

if not start_from_earliest:
    # Filter out messages with the same timestamp as max_timestamp
    parsed_news_df = parsed_news_df.filter(parsed_news_df["timestamp"] > max_timestamp)

parsed_news_df.show()
# Partitioning by year and month
partitioned_df = parsed_news_df \
        .withColumn("year", F.year("timestamp").cast("integer")) \
        .withColumn("month", F.month("timestamp").cast("integer")) \
        .withColumn("day", F.dayofmonth("timestamp").cast("integer"))

# Save to HDFS if there are new rows
if partitioned_df.count() > 0:
    
    partitioned_df \
            .write \
            .partitionBy("year", "month", "day") \
            .mode("append") \
            .parquet(base_path)

# Stop the Spark session
spark.stop()

