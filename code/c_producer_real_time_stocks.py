# Fetch current price in real time - 1 minute interval
# Send to kafka topic- 'realtime_stock_data'
#*************************************************************

import datetime
import time
import json
import requests
from kafka import KafkaProducer
from config import kafka_brokers, twelve_api_key, symbol
import pytz

class RealTimeStockDataProducer:
    def __init__(self, kafka_broker, topic, base_url, endpoint, api_key):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.base_url= base_url
        self.endpoint= endpoint
        self.api_key= api_key

    def fetch_current_price(self, symbol):
        params = {
            "symbol": symbol,
            "interval": "1min",
            "apikey": self.api_key,
            "outputsize":1
        }
        
        try:
            response = requests.get(f'{self.base_url}{self.endpoint}', params=params)
            data = response.json()
            current_price = data.get("values", [])
            return current_price
        
        except Exception as e:
            print(f"Error fetching current price for {symbol}: {e}")
            return None


    def send_to_kafka(self, symbol, current_price):
        if current_price:
            # Extracting symbol, datetime, open, high, low, close, and volume
            datetime_value = current_price[0].get("datetime")
            open_price = current_price[0].get("open")
            high_price = current_price[0].get("high")
            low_price = current_price[0].get("low")
            close_price = current_price[0].get("close")
            volume = current_price[0].get("volume")

            message = {
                "symbol": symbol,
                "datetime": datetime_value,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume
            }

            self.producer.send(self.topic, key=symbol.encode(), value=json.dumps(message).encode('utf-8'))
    
    def run_kafka(self, symbol):
        while True:
            israel_tz = pytz.timezone('Israel')
            current_time = datetime.datetime.now(israel_tz)
            print ('current',current_time)
            # Set Israel's trading hours
            trading_hours_start = current_time.replace(hour=16, minute=30, second=0, microsecond=0)
            trading_hours_end = current_time.replace(hour=23, minute=0, second=0, microsecond=0)

            if trading_hours_start <= current_time <= trading_hours_end:
                current_price = self.fetch_current_price(symbol)
                if current_price is not None:
                    self.send_to_kafka(symbol, current_price)
                    time.sleep(61)
                    print(f"Sent data for {symbol} to Kafka: {current_price}")
            else:
                print("Market closed. Sleeping for 2 minutes...")
                time.sleep(120)

if __name__ == "__main__":
    kafka_broker_address = kafka_brokers
    kafka_topic = 'realtime_stock_data'
    base_url= "https://api.twelvedata.com"
    api_endpoint = "/time_series"
    api_key = twelve_api_key
    symbol= symbol

    producer = RealTimeStockDataProducer(kafka_broker_address, kafka_topic, base_url, api_endpoint, api_key)
    producer.run_kafka(symbol)  
