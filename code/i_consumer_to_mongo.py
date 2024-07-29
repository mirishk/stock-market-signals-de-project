# Send the combined signals from kafka topic "sma_sentiment_combined_signals" to mongo
#**********************************************************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
import pymongo
from config import kafka_brokers, mongodb_client, symbol, kafka_spark
import os 

os.environ['PYSPARK_SUBMIT_ARGS'] = kafka_spark

symbol= symbol

spark = SparkSession.builder \
    .appName("Send_signals_to_mongo") \
    .getOrCreate()

bootstrap_servers = kafka_brokers
consume_topic_name= "sma_sentiment_combined_signals"

#read the real time data from kafka
realtime_df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", bootstrap_servers) \
     .option("subscribe", consume_topic_name) \
     .load() \
     .select(
        F.col('value').cast(T.StringType()).alias('value'),
        F.col('timestamp').alias('kafka_timestamp')
     )


# Define schema for real-time stock signals
signals_schema = T.StructType([
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
     T.StructField("sma_signal", T.StringType(), True),
     T.StructField("sentiment_signal", T.StringType(), True),
     T.StructField("combined_signal", T.StringType(), True)
])


parsed_signals_df = realtime_df \
    .withColumn("kafka_timestamp", F.to_timestamp(F.col("kafka_timestamp"))) \
    .withColumn('parsed_json', F.from_json(F.col('value'), signals_schema)) \
    .select(F.col('parsed_json.*'), "kafka_timestamp") \
    .withColumn("open", F.col("open").cast("double")) \
    .withColumn("high", F.col("high").cast("double")) \
    .withColumn("low", F.col("low").cast("double")) \
    .withColumn("close", F.col("close").cast("double")) \
    .withColumn("volume", F.col("volume").cast("double")) \
    .withColumn("datetime", F.to_timestamp("datetime")) \
    .withColumn("short_term_avg", F.col("short_term_avg").cast("double")) \
    .withColumn("long_term_avg", F.col("long_term_avg").cast("double")) \
    .withColumn("short_above_long", F.col("short_above_long").cast("boolean")) \
    .withColumn("is_significant", F.col("is_significant").cast("boolean"))

parsed_signals_df.printSchema()

#### Defining A Function To Send Dataframe To MongoDB ####
def write_df_mongo(target_df):

    mogodb_client = pymongo.MongoClient(mongodb_client)
    mydb = mogodb_client["stocks_db"]
    mycol = mydb["sma_sentiment_combined_signals"]

    post = {
        "symbol": target_df.symbol,
        "kafka_datetime": target_df.kafka_timestamp.isoformat(),
        "datetime": target_df.datetime.isoformat(),
        "open": target_df.open,
        "high": target_df.high,
        "low": target_df.low,
        "close": target_df.close,
        "volume": target_df.volume,
        "short_term_avg": target_df.short_term_avg,
        "long_term_avg": target_df.long_term_avg,
        "short_above_long": target_df.short_above_long,
        "is_significant": target_df.is_significant,
        "sma_signal": target_df.sma_signal,
        "sentiment_signal": target_df.sentiment_signal,
        "combined_signal": target_df.combined_signal
    }

    mycol.insert_one(post)
    print('item inserted')
    print(post)

### Spark Action ###
parsed_signals_df \
    .writeStream \
    .foreach(write_df_mongo)\
    .outputMode("append") \
    .start() \
    .awaitTermination()


spark.stop()