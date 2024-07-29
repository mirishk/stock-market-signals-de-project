# Send news from last day to kafka topic "stock_news_data"
# Read the last timestamp from hdfs file
# Need to run every hour
#*****************************************

import requests
import json
import time
import datetime
from kafka import KafkaProducer
from langdetect import detect
import re
from config import kafka_brokers, news_api_key, hdfs_host, hdfs_port, hdfs_user, symbol
import pyarrow as pa

class NewsStockDataProducer:
    def __init__(self, kafka_broker, kafka_topic, api_key, base_url, endpoint, params, hdfs_host, hdfs_port, hdfs_user, hdfs_path):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.api_key = api_key
        self.base_url = base_url
        self.endpoint = endpoint
        self.params = params
        self.hdfs_path = hdfs_path
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker,
                                       value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.processed_articles = set()
        self.fs = pa.hdfs.connect(host=hdfs_host, port=hdfs_port, user=hdfs_user)

    def fetch_news(self):
        try:
            response = requests.get(f"{self.base_url}{self.endpoint}", params=self.params)
            data = response.json()
            articles = data.get('articles', [])
            sorted_articles = sorted(articles, key=lambda x: x['publishedAt'])
            return sorted_articles
        except Exception as e:
            print(f"Error fetching news: {e}")
            return []

    def detect_language(self, text):
        try:
            return detect(text)
        except:
            return None

    def clean_text(self, text):
        if text is None or not isinstance(text, str):
            return ''
        text = re.sub(r'http\S+', '', text)
        text = text.replace('\n', ' ')
        text = text.replace('\\', '')
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def read_last_published_at(self, symbol):
        try:
            with self.fs.open(f'{self.hdfs_path}/timestamp.txt', 'rb') as f:
                last_published_at = f.read().decode('utf-8').strip()
                return last_published_at
        except FileNotFoundError:
            return None

    def write_last_published_at(self, symbol, last_published_at):
        with self.fs.open(f'{self.hdfs_path}/timestamp.txt', 'wb') as f:
            f.write(last_published_at.encode('utf-8'))

    def send_to_kafka(self, symbol, articles):
        for article in articles:
            article_identifier = article.get("title", "")
            
            language = self.detect_language(article_identifier)
            if language != 'en':
                print("Skipping non-English article:", article_identifier)
                continue

            if article_identifier.lower() in self.processed_articles:
                print("Skipping duplicate article:", article_identifier)
                continue

            self.processed_articles.add(article_identifier.lower())
            #----------------------------------------------------
            content = article.get('content', '')
            cleaned_content = self.clean_text(content)
            description = article.get('description', '')
            cleaned_description = self.clean_text(description)

            # Extract required fields
            article_data = {
                'symbol': symbol,
                'title': article.get('title', ''),
                'publishedAt': article.get('publishedAt', ''),
                'description': cleaned_description,
                'url': article.get('url', ''),
                'source_name': article.get('source', {}).get('name', ''),
                'content': cleaned_content
            }
            key_bytes = bytes(symbol, 'utf-8')
            self.producer.send(self.kafka_topic, key=key_bytes, value=article_data)
            print("Sent data to Kafka:", article_data.get('title'))
            time.sleep(0.1)

    def run(self, symbol):
        last_published_at = self.read_last_published_at(symbol)
        last_published_at = datetime.datetime.strptime(last_published_at, '%Y-%m-%dT%H:%M:%SZ')
        print(f'last timestamp:{last_published_at}')
        
        if last_published_at > (datetime.datetime.today() - datetime.timedelta(days=1)):
            last_published_at += datetime.timedelta(minutes=1)
            self.params['from'] = last_published_at.isoformat()
        
        articles = self.fetch_news()
        if articles:
            self.send_to_kafka(symbol, articles)
            last_published_at = articles[-1]['publishedAt']
            self.write_last_published_at(symbol, last_published_at)

if __name__ == "__main__":
    company_name_by_symbol ={
        "AAPL": "Apple",
        "MSFT": "Microsoft"
    }

    kafka_broker_address = kafka_brokers
    kafka_topic = "stock_news_data"
    api_key = news_api_key
    symbol = symbol
    company_name = company_name_by_symbol.get(symbol)

    one_day_ago = datetime.date.today() - datetime.timedelta(days=1)
    one_day_ago_formatted = one_day_ago.isoformat()

    base_url = "https://newsapi.org/v2/"
    endpoint = "everything"
    params = {
        "q": f"{symbol} OR ({company_name} Inc)",
        "searchIn": "title,description",
        "apiKey": api_key,
        "from": one_day_ago_formatted,
        "language": "en",
        "sortBy": "publishedAt",
        "fields": "title,publishedAt,description,url,source.name"  # Filter only required fields
    }

    hdfs_host = hdfs_host
    hdfs_port = hdfs_port
    hdfs_user = hdfs_user
    hdfs_path = f"/final-project/news_timestamp/symbol={symbol}"

    news_producer = NewsStockDataProducer(
        kafka_broker_address,
        kafka_topic,
        api_key,
        base_url,
        endpoint,
        params,
        hdfs_host,
        hdfs_port,
        hdfs_user,
        hdfs_path
    )
    
    news_producer.run(symbol)
   
