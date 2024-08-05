# Stock Market Signals

This project implements a comprehensive data engineering pipeline for stock price analysis and trading signal generation. The pipeline integrates historical stock prices, real-time price updates, sentiment analysis of news, and trading signal notifications.

[View the presentation](final_project_de_presentation.pdf)


## Architecture
![final_project_architecture](https://github.com/user-attachments/assets/90176db1-ea92-46b3-8a66-b79c1ea46006)


### 1. Historical Stock Prices
- **Data Source**: Twelve Data API
- **Processing**: Data is fetched and sent to a Kafka topic.
- **Storage**: Using PySpark, data is stored in HDFS. Daily update is performed to append the daily close price.

### 2. Real-Time Stock Prices
- **Data Source**: Real-time stock prices (1-minute interval)
- **Processing**: Data is sent to a Kafka topic.
- **Streaming**: A Spark Streaming job processes historical data from HDFS and real-time data from Kafka. It computes metrics like the Simple Moving Average (SMA) and generates stock signals based on these metrics.

### 3. News Sentiment Analysis
- **Data Source**: Recent news articles (last 24 hours) via news API
- **Processing**: News data is sent to a Kafka topic, processed by Spark, and stored in HDFS. Sentiment analysis is performed, and results are enriched and stored in HDFS.

### 4. Signal Generation
- **Integration**: A PySpark script combines price signals and sentiment analysis to determine final trading signals (e.g., Buy, Hold, Sell, Strong Buy, Strong Sell).
- **Output**: Final trading signals are sent to a Kafka topic and stored in MongoDB.
- **Notification**: A Python script sends trading signals to a Telegram bot based on user requests.

## Components

- **Data Ingestion**: Twelve Data API, news API
- **Message Brokers**: Kafka
- **Data Processing**: PySpark, Spark Streaming
- **Storage**: HDFS, MongoDB
- **Notification**: Telegram bot

