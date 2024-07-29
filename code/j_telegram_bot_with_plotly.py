import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
import pymongo
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd
import os
import git
from config import telegram_token, mongodb_client, repo_path, repo_url
from datetime import datetime
from plotly.subplots import make_subplots

# Initialize MongoDB client
mongo_client = pymongo.MongoClient(mongodb_client)
db = mongo_client["stocks_db"]
collection = db["sma_sentiment_combined_signals"]

# Initialize your bot with the token obtained from BotFather
TOKEN = telegram_token
bot = telebot.TeleBot(TOKEN)

# Define bot commands
bot_commands = [
    telebot.types.BotCommand("/start", "Start the bot"),
    telebot.types.BotCommand("/signals", "Get the latest stock signals"),
    telebot.types.BotCommand("/help", "Show help message")
]

# Set bot commands
bot.set_my_commands(bot_commands)

# Define a dictionary to store user data during the conversation
user_data = {}

# Repository details
REPO_PATH = repo_path
REPO_URL = repo_url

# Commonly used stock symbols
common_stocks = ["AAPL", "NVDA", "GOOGL", "TSLA", "AMZN"]

# Define options in start
def main_menu():
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("View Signals", callback_data='view_signals'),
        InlineKeyboardButton("Help", callback_data='help')
    )
    return keyboard

# Start -> main_menu
@bot.message_handler(commands=['start'])
def start(message):
    welcome_text = "Welcome to the Stock Signals Bot! Choose an option:"
    bot.send_message(message.chat.id, welcome_text, reply_markup=main_menu())

# Menu options handler
@bot.callback_query_handler(func=lambda call: True)
def handle_query(call):
    if call.data == 'view_signals':
        prompt_symbol_selection(call.message)
    elif call.data == 'help':
        send_help_message(call.message.chat.id)
    else:
        symbol = call.data
        send_signals(call.message, symbol)

# Function to prompt symbol selection
def prompt_symbol_selection(message):
    markup = ReplyKeyboardMarkup(row_width=2, one_time_keyboard=True, resize_keyboard=True)
    buttons = [KeyboardButton(stock) for stock in common_stocks]
    markup.add(*buttons)
    bot.send_message(message.chat.id, "Please select a symbol from the list or type your own symbol:", reply_markup=markup)
    bot.register_next_step_handler(message, get_symbol)

# signals command handler
@bot.message_handler(commands=['signals'])
def signals(message):
    prompt_symbol_selection(message)

# Get the stock symbol from the user
def get_symbol(message):
    symbol = message.text.strip().upper()
    send_signals(message, symbol)

# Function to prompt the user to check another stock or exit
def prompt_check_another_stock(message):
    markup = ReplyKeyboardMarkup(row_width=2, one_time_keyboard=True, resize_keyboard=True)
    markup.add(KeyboardButton("Check another stock"), KeyboardButton("Exit"))
    bot.send_message(message.chat.id, "Would you like to check another stock?", reply_markup=markup)
    bot.register_next_step_handler(message, handle_check_another_stock)


# Handler for the check another stock prompt
def handle_check_another_stock(message):
    if message.text == "Check another stock":
        prompt_symbol_selection(message)
    else:
        # Clear the keyboard when the user chooses to exit
        bot.send_message(message.chat.id, "Thank you for using the Stock Signals Bot!", reply_markup=ReplyKeyboardRemove())

# Help command handler
@bot.message_handler(commands=['help'])
def help(message):
    send_help_message(message.chat.id)

# Function to send help message
def send_help_message(chat_id):
    help_text = (
    "Here are the commands you can use:\n"
    "/start: Start the bot\n"
    "/signals: Get the latest stock signals\n"
    "/help: Show this help message"
    )
    bot.send_message(chat_id, help_text, parse_mode='MarkdownV2')

# Function to send signals to the user
def send_signals(message, symbol):
    signals_data = get_signals(symbol)
    if signals_data:
        for signal in signals_data:
            signal_text = (
                f"Symbol: {signal['symbol']}\n"
                f"Time: {signal['datetime']}\n"
                f"Price: {signal['close']}\n"
                f"SMA_Signal: {signal['sma_signal']}\n"
                f"Sentiment_Signal: {signal['sentiment_signal']}\n"
                f"combined_Signal: {signal['combined_signal']}"
            )
            bot.send_message(message.chat.id, signal_text)
        html_file_path = generate_combined_chart(message.chat.id, symbol)
        file_url = push_to_github(html_file_path)
        bot.send_message(message.chat.id, f"Here is the link to the chart: {file_url}")  
    else:
        bot.send_message(message.chat.id, f"No signals available for {symbol}.")

    prompt_check_another_stock(message)

# Define a function to retrieve signals from MongoDB for a specific symbol
def get_signals(symbol):
    signals_data = collection.find({"symbol": symbol})\
        .sort("datetime", pymongo.DESCENDING).limit(3)
    return list(signals_data)

# Function to generate a combined chart
def generate_combined_chart(chat_id, symbol):
    today = datetime.now().date()

    # Define the start of today's trading hours
    start_of_trading = datetime(today.year, today.month, 24, 9, 30).isoformat()

    query = {
        "symbol": symbol,
        "datetime": {
            "$gte": start_of_trading
        }
    }

    # Try-except block for error handling (replace with your specific error handling)
    try:
        # Query MongoDB for data
        data = list(collection.find(query))
        df = pd.DataFrame(data)
    except pymongo.errors.ConnectionError as e:
        print(f"Error connecting to MongoDB: {e}")
        return None  # Or handle the error differently

    # Create a subplot with shared x-axis
    fig = make_subplots(rows=2, cols=1, shared_xaxes=False, vertical_spacing=0.3,
                    subplot_titles=('Candlestick Chart', 'Line Chart'))

    # Clear the subplot before adding the candlestick chart (optional)
    fig.layout.update(annotations=[])  # Optional: Clear annotations from the subplot

    # Add candlestick chart
    fig.add_trace(go.Candlestick(x=df['datetime'], open=df['open'], high=df['high'],
                                 low=df['low'], close=df['close'], name='Candlestick'),
                 row=1, col=1)
    
    # Add line chart with price
    fig.add_trace(go.Scatter(x=df['datetime'], y=df['close'], mode='lines', name='Price',
                         line=dict(color='blue')),  # Set line color to blue
              row=2, col=1)
    
    # Calculate the average price
    avg_price = df['close'].mean()

    # Add trace for average line with customization (optional)
    fig.add_trace(go.Scatter(x=df['datetime'], y=[avg_price] * len(df), mode='lines',
                             name='Average', line=dict(color='blue', width=2, dash='dash')),
                 row=2, col=1)

    # Add label for average line
    fig.add_annotation(
        x=df['datetime'].iloc[0],
        y=avg_price,
        text=f'Average Price: ${avg_price:.2f}',
        showarrow=True,
        arrowhead=1,
        row=2, col=1
    )

    # Save the figure as an HTML file
    html_file_path = os.path.join(REPO_PATH, f'{chat_id}_{symbol}_combined_chart_{datetime.now().strftime("%Y%m%d%H%M%S")}.html')
    pio.write_html(fig, file=html_file_path, auto_open=False)

    return html_file_path


# Function to push the HTML file to GitHub
def push_to_github(file_path):
    repo = git.Repo(REPO_PATH)
    repo.index.add([file_path])
    repo.index.commit(f'Add {os.path.basename(file_path)}')
    origin = repo.remote(name='origin')
    origin.push()

    # Construct the GitHub URL for the file
    file_url = f'{REPO_URL}/blob/main/{os.path.basename(file_path)}'
    return file_url

# Start the bot
bot.polling()
