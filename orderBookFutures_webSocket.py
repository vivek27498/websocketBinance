import websocket
import json
import csv
import time
from datetime import datetime

# Create a CSV file to store order book data
csv_file = open('webSocketFutures_orderBook_17-8-2023.csv', 'w', newline='')
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['Current Time','Transaction Time', 'Message Time', 'Highest Bid', 'Lowest Ask', 'Mid Price'])

last_fetch_time = time.time()

def on_message(ws, message):

    global last_fetch_time  # Declare last_fetch_time as global

    event_data = json.loads(message)
    data = event_data['data']

    if 'e' in data and data['e'] == 'depthUpdate':

        current_time = time.time()

        if current_time - last_fetch_time >= 1:  # Fetch data every second

            print(data)
            last_fetch_time = current_time
            
            transaction_time = datetime.utcfromtimestamp(data['T'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
            message_time = datetime.utcfromtimestamp(data['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
            highest_bid = float(data['b'][0][0])
            lowest_ask = float(data['a'][0][0])
            current_local_time = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S.%f')
            
            for bid in data['b']:
                price = float(bid[0])
                if highest_bid is None or price > highest_bid:
                    highest_bid = price

            for ask in data['a']:
                price = float(ask[0])
                if lowest_ask is None or price < lowest_ask:
                    lowest_ask = price

            # Calculate and store mid price
            if highest_bid is not None and lowest_ask is not None:
                mid_price = (highest_bid + lowest_ask) / 2
                current_time = datetime.utcfromtimestamp(data['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                csv_writer.writerow([current_local_time,transaction_time,message_time,highest_bid, lowest_ask, mid_price])
                                                                                             

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Closed")

def on_open(ws):
    print("WebSocket opened")                                                                                                                                                                                                                                           

if __name__ == "__main__":
    # <symbol>@depth<levels> OR <symbol>@depth<levels>@500ms OR <symbol>@depth<levels>@100ms.
    # depth can be 5 10 20
    websocket_url = "wss://fstream.binance.com/stream?streams=ethusdt@depth20@100ms"
    ws = websocket.WebSocketApp(websocket_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open)
    ws.on_open = on_open
    ws.run_forever()
