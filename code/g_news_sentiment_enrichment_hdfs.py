# Enrichment of news data with sentiment analysis and send back to hdfs
#**********************************************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
from config import hdfs_user, hdfs_host, hdfs_port, symbol, kafka_spark
from textblob import TextBlob

import os 

os.environ['PYSPARK_SUBMIT_ARGS'] = kafka_spark
os.environ['HADOOP_USER_NAME'] = hdfs_user
hdfs_path = f"hdfs://{hdfs_host}:{hdfs_port}"
print(hdfs_path)

symbol= symbol

spark = SparkSession \
    .builder \
    .appName("Stock_news_sentiment_analysis_enrichment") \
    .config("spark.hadoop.fs.defaultFS", hdfs_path) \
    .getOrCreate()


news_data_path = f"{hdfs_path}//final-project/news_data/symbol={symbol}"
enriched_data_path = f"{hdfs_path}//final-project/enriched_news_data/symbol={symbol}"

# Define the schema for the Parquet files- just for documentation
schema = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("publishedAt", T.TimestampType(), True),
    T.StructField("description", T.StringType(), True),
    T.StructField("url", T.StringType(), True),
    T.StructField("source_name", T.StringType(), True),
    T.StructField("content", T.StringType(), True),
    T.StructField("timestamp", T.TimestampType(), True)
])

# Read data for the current day from HDFS
current_date = datetime.datetime.now()  # Get the current date
year = current_date.year
month = current_date.month
day = current_date.day

latest_timestamp = datetime.datetime(current_date.year, current_date.month, current_date.day)

# Read the latest timestamp from enriched_news_data
try:
    latest_timestamp_df = spark.read.parquet(f"{enriched_data_path}/year={year}/month={month}/day={day}") \
        .agg(F.max("timestamp").alias("latest_timestamp"))

    # Extract the latest timestamp value
    if latest_timestamp_df.count() > 0:
        latest_timestamp = latest_timestamp_df.collect()[0]["latest_timestamp"]
except:
    pass    

print(f'latest_timestamp: {latest_timestamp}')

# Read data for the current day from HDFS and filter it according latest_timstamp
current_day_news_data = spark.read.parquet\
    (f"{news_data_path}/year={year}/month={month}/day={day}")\
    .filter(F.col("timestamp") > latest_timestamp)

# Perform sentiment analysis using TextBlob
def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity
    return sentiment_score

sentiment_udf = F.udf(analyze_sentiment, returnType=T.FloatType())
enriched_data = current_day_news_data.withColumn("sentiment_score", sentiment_udf("content"))

# Select relevant columns
selected_columns = ["symbol", "title", "publishedAt", "description", "url", "source_name", "content", "timestamp","sentiment_score"]
enriched_data_for_hdfs = enriched_data.select(selected_columns)

# Partitioning by year and month
partitioned_df = enriched_data_for_hdfs \
        .withColumn("year", F.year("timestamp").cast("integer")) \
        .withColumn("month", F.month("timestamp").cast("integer")) \
        .withColumn("day", F.dayofmonth("timestamp").cast("integer"))

partitioned_df.show()

# Write enriched data to HDFS
partitioned_df \
    .write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(enriched_data_path)

# Stop SparkSession
spark.stop()
