# Load the historical prices to kafka- just for initial load
# Topic name - "historical_stock_prices"
# The daily update will be directly into hdfs cause it is just one value per day for each stock
#************************************************************************

from kafka import KafkaProducer
import requests
import json
import datetime
from config import kafka_brokers, twelve_api_key, symbol

##Stock close price for each day
class HistStockPriceProducer:
    def __init__(self, kafka_broker, topic, base_url, endpoint, api_key):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        self.base_url= base_url
        self.endpoint= endpoint
        self.api_key= api_key

    ##Fetching data from the url according the params
    def fetch_data(self, symbol, interval, start_date=None, end_date=None, outputsize=None):
        params={
            "symbol": symbol,
            "interval": interval,
            "apikey": self.api_key,
            "order": "asc"
        }

        #optional params
        if start_date:
            params["start_date"]= start_date
        if end_date:
            params["end_date"]= end_date
        if outputsize:
            params["outputsize"]= outputsize

        try:
            response = requests.get(f"{self.base_url}{self.endpoint}", params=params)
            data = response.json()
            values = data.get("values", [])
            return values
        
        except Exception as e:
            print(f'Error fetching data: {e}')
            return []
        
    ##Sending each data point to kafka function
    def process_data(self, symbol, data):
        if data:
            for data_point in data:
                date = data_point["datetime"]
                close_price = data_point["close"]
                volume = data_point["volume"]
                self.publish_to_kafka(symbol, date, close_price, volume)
            print(f"Initial load is complete for {symbol}")
                   
        else:
            print(f"No data found for {symbol}")

    ##Load the period only in the first time, just for computing moving average
    def load_historical_data(self, symbol, outputsize):
        interval = "1day"
        end_date = datetime.date.today() 
        data= self.fetch_data(symbol, interval, end_date= end_date, outputsize= outputsize)
        self.process_data(symbol, data)

    #load to kafka producer for multiple consumers
    def publish_to_kafka(self, symbol, date, close_price, volume):
        message = {
            "symbol": symbol,
            "date": str(date),
            "close_price": close_price,
            "volume": volume
        }
        self.producer.send(self.topic, key=symbol.encode(), value=json.dumps(message).encode('utf-8'))
        self.producer.flush()

if __name__ == "__main__":
    kafka_broker_address = kafka_brokers
    stock_topic = "historical_stock_prices"
    base_url=   "https://api.twelvedata.com"
    endpoint=  "/time_series"
    api_key= twelve_api_key
    symbol= symbol
    outputsize= 200

    stock_producer = HistStockPriceProducer(kafka_broker_address, stock_topic, base_url, endpoint, api_key)

    # Initial load (run only once)
    stock_producer.load_historical_data(symbol, outputsize)

