# Aggregate the news data with sentiment (from hdfs) and determine the sentiment signal
# Combine with the SMA signals from kafka "crossover_strategy"
# Send the combined signals to kafka in sreaming , topic name- "sma_sentiment_combined_signals"
#**************************************************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
from config import kafka_brokers, hdfs_user, hdfs_host, hdfs_port, symbol, kafka_spark
import os 
import pytz

os.environ['PYSPARK_SUBMIT_ARGS'] = kafka_spark
os.environ['HADOOP_USER_NAME'] = hdfs_user
hdfs_path = f"hdfs://{hdfs_host}:{hdfs_port}"
print(hdfs_path)

symbol= symbol

spark = SparkSession.builder \
    .appName("Combine_SMA_and_News_Sentiment") \
    .config("spark.hadoop.fs.defaultFS", hdfs_path) \
    .getOrCreate()

# Read data for the current day from HDFS
enriched_data_path = f"{hdfs_path}//final-project/enriched_news_data/symbol={symbol}"
current_date = datetime.datetime.now()  # Get the current date
year = current_date.year
month = current_date.month
day = current_date.day

# kafka 
bootstrap_servers = kafka_brokers
consume_topic_name = "crossover_strategy"
produce_topic_name = "sma_sentiment_combined_signals"

news_sentiment_df = None
# Read news sentiment data from HDFS
try:
    news_sentiment_df = spark.read.parquet\
        (f"{enriched_data_path}/year={year}/month={month}/day={day}")
    news_sentiment_df.show()    
except:
    print('No news data for this day')


# Aggregate sentiment data
if news_sentiment_df:
    total_count = news_sentiment_df.count()
    
    # Calculate counts and percentage of positive and negative sentiments
    sentiment_aggregated_df = news_sentiment_df.groupBy("symbol").agg(
        F.count(F.when(F.col("sentiment_score") > 0, True)).alias("positive_count"),
        (F.count(F.when(F.col("sentiment_score") > 0, True)) / total_count * 100).alias("positive_percentage"),
        F.avg(F.when(F.col("sentiment_score") > 0, F.col("sentiment_score"))).alias("positive_avg_score"),
        F.count(F.when(F.col("sentiment_score") < 0, True)).alias("negative_count"),
        (F.count(F.when(F.col("sentiment_score") < 0, True)) / total_count * 100).alias("negative_percentage"),
        F.avg(F.when(F.col("sentiment_score") < 0, F.col("sentiment_score"))).alias("negative_avg_score"),
        F.count(F.when(F.col("sentiment_score") == 0, True)).alias("neutral_count"),
    )

    # Define buy, sell, hold signals based on sentiment analysis results
    sentiment_agg_df = sentiment_aggregated_df.withColumn(
        "sentiment_signal",
        F.when(((F.col("positive_percentage") > 50) & (F.col("positive_avg_score") > 0.3))
                |(F.col("positive_percentage") == 100), "buy")
        .when((F.col("positive_percentage") > 55) & (F.col("positive_avg_score") > 0.6), "strong_buy")
        .when(((F.col("negative_percentage") > 50) & (F.col("negative_avg_score") < -0.3))
              |(F.col("negative_percentage") == 100), "sell")
        .when((F.col("negative_percentage") > 55) & (F.col("negative_avg_score") < -0.6), "strong_sell")
        .otherwise("hold")
    )

    # Display results
    sentiment_agg_df.show()

    # Extract sentiment signal column to a variable
    sentiment_signal = sentiment_agg_df.select("sentiment_signal").first()[0]
    print ('sss:', sentiment_signal)

# Calculate the starting timestamp for consuming data
start_time_seconds = spark.sparkContext.startTime/1000
start_datetime = datetime.datetime.fromtimestamp(start_time_seconds, tz=pytz.utc)
start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", start_datetime_str)

# Read SMA data from Kafka topic as a streaming DataFrame
sma_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", consume_topic_name) \
    .option("startingOffsets", 'latest') \
    .option("failOnDataLoss", "false")\
    .load()\
    .select(
         F.col('value').cast(T.StringType()).alias('value'),
         F.col('timestamp').alias('kafka_timestamp')
     )

# Convert kafka_timestamp to a string and then to a timestamp to ensure correct format
sma_df = sma_df.withColumn("kafka_timestamp", F.to_timestamp(F.col("kafka_timestamp")))

# Filter the DataFrame based on the calculated starting timestamp
filtered_sma_df = sma_df.filter(F.col("kafka_timestamp") >= F.to_timestamp(F.lit(start_datetime_str)))

# Define schema for real-time stock prices 
sma_schema = T.StructType([
     T.StructField("symbol", T.StringType(), True),
     T.StructField("datetime", T.StringType(), True),
     T.StructField("open", T.StringType(), True),
     T.StructField("high", T.StringType(), True),
     T.StructField("low", T.StringType(), True),
     T.StructField("close", T.StringType(), True),
     T.StructField("volume", T.StringType(), True),
     T.StructField("short_term_avg", T.StringType(), True),
     T.StructField("long_term_avg", T.StringType(), True),
     T.StructField("short_above_long", T.StringType(), True),
     T.StructField("is_significant", T.StringType(), True),
     T.StructField("signal", T.StringType(), True)
])

parsed_sma_df = filtered_sma_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), sma_schema)) \
    .select(F.col('parsed_json.*')) \
    .withColumn("open", F.col("open").cast("double")) \
    .withColumn("high", F.col("high").cast("double")) \
    .withColumn("low", F.col("low").cast("double")) \
    .withColumn("close", F.col("close").cast("double")) \
    .withColumn("volume", F.col("volume").cast("double")) \
    .withColumn("datetime", F.to_timestamp("datetime")) \
    .withColumn("sma_signal", F.col("signal")) \
    .drop("signal")

# Apply combination logic to create combined signal column
combined_df = parsed_sma_df\
.withColumn("sentiment_signal", F.lit(sentiment_signal))

combined_df= combined_df\
.withColumn(
    "combined_signal",
    F.when(
        (parsed_sma_df["sma_signal"] == "strong_buy") & 
        ((F.col("sentiment_signal") == "strong_buy") | (F.col("sentiment_signal") == "buy"))
        , "strong_buy"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "strong_buy") & 
        ((F.col("sentiment_signal") == "hold") |(F.col("sentiment_signal") == "sell"))
        , "buy"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "strong_buy") & 
        (F.col("sentiment_signal") == "strong_sell")
        , "hold"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "buy") & 
        (F.col("sentiment_signal") == "strong_buy")
        , "strong_buy"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "buy") & 
        ((F.col("sentiment_signal") == "buy") |(F.col("sentiment_signal") == "hold"))
        , "buy"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "buy") & 
        ((F.col("sentiment_signal") == "sell") |(F.col("sentiment_signal") == "strong_sell"))
        , "hold"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "hold") & 
        (F.col("sentiment_signal") == "strong_buy")
        , "buy"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "hold") & 
        ((F.col("sentiment_signal") == "buy") |(F.col("sentiment_signal") == "hold") | (F.col("sentiment_signal") == "sell"))
        , "hold"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "hold") & 
        (F.col("sentiment_signal") == "strong_sell") 
        , "sell"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "sell") & 
        (F.col("sentiment_signal") == "strong_sell")
        , "strong_sell"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "sell") & 
        ((F.col("sentiment_signal") == "sell") |(F.col("sentiment_signal") == "hold"))
        , "sell"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "sell") & 
        ((F.col("sentiment_signal") == "buy") |(F.col("sentiment_signal") == "strong_buy"))
        , "hold"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "strong_sell") & 
        ((F.col("sentiment_signal") == "strong_sell") | (F.col("sentiment_signal") == "sell"))
        , "strong_sell"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "strong_sell") & 
        ((F.col("sentiment_signal") == "hold") |(F.col("sentiment_signal") == "buy"))
        , "sell"
    )
    .when(
        (parsed_sma_df["sma_signal"] == "strong_sell") & 
        (F.col("sentiment_signal") == "strong_buy")
        , "hold"
    ).otherwise(parsed_sma_df["sma_signal"])  # Default to sma signal if no specific condition is met
)

combined_df.printSchema()

query = combined_df \
    .selectExpr(
        "CAST(symbol AS STRING) AS key", 
        "CAST(to_json(struct(*)) AS STRING) AS value"
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", produce_topic_name) \
    .option("checkpointLocation", f"{hdfs_path}//final-project/checkpoints/combined_signals") \
    .outputMode("append") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()

# # Stop SparkSession
spark.stop()
