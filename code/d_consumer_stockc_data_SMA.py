# Read historical data from hdfs - batch
# Read near real time price from kafka topic 'realtime_stock_data' -streaming
# Send to kafka SMA signals in streaming, topic name- 'crossover_strategy'
#*************************************************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from config import kafka_brokers, hdfs_host, hdfs_port, hdfs_user, symbol, kafka_spark
import datetime
import pytz
import os
from pyspark.sql.window import Window

os.environ['PYSPARK_SUBMIT_ARGS'] = kafka_spark
os.environ['HADOOP_USER_NAME'] = hdfs_user
hdfs_path = f"hdfs://{hdfs_host}:{hdfs_port}"

spark = SparkSession \
    .builder \
    .appName("calculate_SMA") \
    .config("spark.hadoop.fs.defaultFS", hdfs_path) \
    .getOrCreate()

bootstrap_servers = kafka_brokers
consume_topic_name= 'yesterday_stock_data'#'realtime_stock_data'#
produce_topic_name= 'crossover_strategy'

#----------------------
# Define the window sizes for short-term (5-day) and long-term (10-day) SMAs
short_window_size = 5
long_window_size = 10

try:
    # Read historical data from HDFS
    symbol= symbol
    historical_data_path = f"{hdfs_path}/final-project/historical_stock_data/symbol={symbol}"
    print(f'path:{historical_data_path}')
    historical_df = spark.read.parquet(historical_data_path)

    # Add a new column "symbol" , Convert the "date" column to datetime
    historical_df = historical_df\
        .withColumn("symbol", F.lit(symbol))\
        .withColumn("date", F.to_timestamp("date"))

    # Order historical data by date and select the last n rows
    historical_df = historical_df\
        .select("symbol", "date", "close_price") \
        .orderBy(F.col("date").desc())\
        .limit(long_window_size*3)

    historical_df.show()

    # Cache the historical data for faster access
    historical_df.cache()

except FileNotFoundError as e:
    print("Historical data file not found:", str(e))
except Exception as e:
    print("An error occurred:", str(e))

# Create window specifications
short_window_spec = Window.orderBy("date").rowsBetween(-short_window_size + 1, 0)
long_window_spec = Window.orderBy("date").rowsBetween(-long_window_size + 1, 0)
previous_day_spec = Window.orderBy("date")

# Calculate the short-term and long-term SMAs
historical_df = historical_df.withColumn("SMA_short", F.avg(F.col("close_price")).over(short_window_spec))
historical_df = historical_df.withColumn("SMA_long", F.avg(F.col("close_price")).over(long_window_spec))

# Check if short above long
historical_df = historical_df.withColumn(
    "short_above_long",
    F.when(F.col("SMA_short") > F.col("SMA_long"), 1).otherwise(0)
)

# Calculate the positions
historical_df = historical_df.withColumn(
    "position", 
        F.col("short_above_long") - F.lag(F.col("short_above_long"), 1)
    .over(previous_day_spec))

# Show the result
historical_df.show(long_window_size*3)

# Extract the last row
last_row = historical_df.orderBy(F.col("date").desc()).head(1)[0]

# Access the attributes of the last row
print("Last row data:")
print("Date:", last_row["date"])
short_term_avg= last_row["SMA_short"]
long_term_avg= last_row["SMA_long"]
short_above_long= last_row["short_above_long"]==1
position= None
if last_row["position"]==1:
    position= 'buy'
elif last_row["position"]==-1:
    position= 'sell'
else:
    position='hold'

threshold_percent = 0.03
significant_diff = abs(short_term_avg - long_term_avg) / long_term_avg > threshold_percent

# Display the average value for the last n rows
print(f'short: {short_term_avg} and long: {long_term_avg} , position: {position}, significant: {significant_diff} , short>long: {short_above_long}')
#-----------------------------------------------------------

# Calculate the starting timestamp for consuming data
start_time_seconds = spark.sparkContext.startTime/1000
start_datetime = datetime.datetime.fromtimestamp(start_time_seconds, tz=pytz.utc)
start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", start_datetime_str)

# Read the real time data from kafka
realtime_df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", bootstrap_servers) \
     .option("subscribe", consume_topic_name) \
     .option("startingOffsets", "latest") \
     .option("failOnDataLoss", "false")\
     .load() \
     .select(
        F.col('value').cast(T.StringType()).alias('value'),
        F.col('timestamp').alias('kafka_timestamp')
    )

# Convert kafka_timestamp to a string and then to a timestamp to ensure correct format
realtime_df = realtime_df.withColumn("kafka_timestamp", F.to_timestamp(F.col("kafka_timestamp")))

# Filter the DataFrame based on the calculated starting timestamp
filtered_df = realtime_df.filter(F.col("kafka_timestamp") >= F.to_timestamp(F.lit(start_datetime_str)))

# Define schema for real-time stock prices
realtime_schema = T.StructType([
     T.StructField("symbol", T.StringType(), True),
     T.StructField("datetime", T.StringType(), True), 
     T.StructField("open", T.StringType(), True),
     T.StructField("high", T.StringType(), True),
     T.StructField("low", T.StringType(), True),
     T.StructField("close", T.StringType(), True),
     T.StructField("volume", T.StringType(), True)
])

parsed_realtime_df = filtered_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), realtime_schema)) \
    .select(F.col('parsed_json.*')) \
    .withColumn("open", F.col("open").cast("double")) \
    .withColumn("high", F.col("high").cast("double")) \
    .withColumn("low", F.col("low").cast("double")) \
    .withColumn("close", F.col("close").cast("double")) \
    .withColumn("volume", F.col("volume").cast("double")) \
    .withColumn("datetime", F.to_timestamp("datetime"))


parsed_realtime_df.printSchema()

# Add short-term and long-term average values as columns
streaming_df_with_avg = parsed_realtime_df \
    .withColumn("short_term_avg", F.lit(short_term_avg)) \
    .withColumn("long_term_avg", F.lit(long_term_avg))\
    .withColumn("short_above_long", F.lit(short_above_long))\
    .withColumn("is_significant", F.lit(significant_diff))\
    .withColumn("position", F.lit(position))

processing_df = streaming_df_with_avg \
    .withColumn("signal", 
        F.when(F.col("short_above_long") & F.col("is_significant"), 
               F.when((F.col("close") > F.col("short_term_avg")), "strong_buy")
                .otherwise("hold"))
        .otherwise(
            F.when(F.col("short_above_long") & ~F.col("is_significant"), 
                   F.when((F.col("close") > F.col("short_term_avg")) & (F.col("position") == "buy"), "strong_buy")
                   .when((F.col("close") > F.col("short_term_avg")) & (F.col("position") != "buy"), "buy") 
                   .when((F.col("close") < F.col("long_term_avg")) & (F.col("Position") != "buy"), "sell") 
                   .otherwise("hold"))
            .otherwise(
                F.when(~F.col("short_above_long") & F.col("is_significant"),
                    F.when((F.col("close") < F.col("short_term_avg")), "strong_sell") 
                    .otherwise("hold"))
                .otherwise(
                    F.when(~F.col("short_above_long") & ~F.col("is_significant"), 
                        F.when((F.col("close") < F.col("short_term_avg")) & (F.col("Position") == "sell"), "strong_sell") 
                        .when((F.col("close") < F.col("short_term_avg")) & (F.col("Position") != "sell"), "sell") 
                        .when((F.col("close") > F.col("long_term_avg")) & (F.col("Position") != "sell"), "buy")
                        .otherwise("hold"))
                    )
                )
            )
    )

#Select the required columns for output
output_df = processing_df.select("symbol", "datetime", "open", "high", "low", "close", "volume", "short_term_avg", "long_term_avg", "short_above_long", "is_significant", "signal")

# Write signals to Kafka
signals_query = output_df \
    .selectExpr(
        "CAST(symbol AS STRING) AS key",  # Use symbol as the key in kafka
        "CAST(to_json(struct(*)) AS STRING) AS value"
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", produce_topic_name) \
    .option("checkpointLocation", f"{hdfs_path}/final-project/checkpoints/signals") \
    .outputMode("append") \
    .start()

signals_query.awaitTermination()

historical_df.unpersist()

spark.stop()