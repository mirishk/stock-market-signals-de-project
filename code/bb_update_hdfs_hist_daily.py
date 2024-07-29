# Update the daily close price directly in hdfs file, 
# Need to run in the end of trading day
#********************************************

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import requests
from config import twelve_api_key, hdfs_host, hdfs_port, hdfs_user, symbol

# Connect to HDFS
fs = pa.hdfs.connect(host=hdfs_host, port=hdfs_port, user=hdfs_user)

symbol = symbol
base_url = "https://api.twelvedata.com"
endpoint = "/time_series"
hdfs_path = f"/final-project/historical_stock_data/symbol={symbol}"

# Define function to fetch daily data
def get_closing_price(symbol):
    params = {
        "symbol": symbol,
        "interval": "1day",
        "apikey": twelve_api_key,
        "outputsize": 1
    }

    try:
        response = requests.get(f"{base_url}{endpoint}", params=params)
        data = response.json().get("values", [])
        print(data)

        if data:
            data_point = data[0]
            date = data_point["datetime"]
            close_price = float(data_point["close"])
            volume = int(data_point["volume"])
            return symbol, date, close_price, volume
        else:
            raise Exception(f"No data found for {symbol} on {date}")

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# Define function to update current month's data
def update_current_month_data(symbol):
    # Fetch daily data
    data = get_closing_price(symbol)
    if data:
        symbol, date_str, close_price, volume = data
        date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        year, month = date.year, date.month

        # Define the HDFS path for the Parquet file
        file_path = f"{hdfs_path}/year={year}/month={month}/data.parquet"

        # Read existing Parquet data
        try:
            with fs.open(file_path, 'rb') as f:
                existing_table = pq.read_table(f)
                existing_df = existing_table.to_pandas()
        except FileNotFoundError:
            # If the file does not exist, create an empty DataFrame
            existing_df = pd.DataFrame(columns=["symbol", "date", "close_price", "volume"])

        # Create DataFrame for new data
        new_data = pd.DataFrame([(symbol, date, close_price, volume)],
                                columns=["symbol", "date", "close_price", "volume"])

        # Append the new data
        updated_df = pd.concat([existing_df, new_data], ignore_index=True)

        # Write updated DataFrame back to Parquet file
        updated_table = pa.Table.from_pandas(updated_df)
        with fs.open(file_path, 'wb') as f:
            pq.write_table(updated_table, f)

        print(f"Appended data for {symbol} on {date_str}")
    else:
        print(f"No data to append for {symbol}")

# Run the update function
update_current_month_data(symbol)


