import websocket
import json
import csv
from datetime import datetime

# Define the symbol for which you want to fetch the index prices
symbol = "ethusdt"

# Define the WebSocket URL for Binance Futures
socket_url = "wss://fstream.binance.com/stream?streams=ethusdt@markPrice@1s"

# Define the CSV file path
csv_file = f"Websocket_{symbol}_futures_index_prices_17-8-2023.csv"

# Define the CSV header
csv_header = ["timestamp","IndexPrice","Symbol"]

# Initialize the CSV file and write the header
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(csv_header)

# Define the WebSocket message handler
def on_message(ws, message):
    print(message)
    event_data = json.loads(message)
    data = event_data['data']
    timestamp = datetime.fromtimestamp(data["E"] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    index_price = data["i"]
    
    # Append data to CSV file
    with open(csv_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([timestamp,index_price,symbol])

# Create a WebSocket connection
ws = websocket.WebSocketApp(socket_url, on_message=on_message)

# Run the WebSocket connection
ws.run_forever()
