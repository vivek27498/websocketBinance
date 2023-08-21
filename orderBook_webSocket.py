import websocket
import json
import csv
from datetime import datetime

# Create a CSV file to store mid prices
csv_file = open('mid_prices.csv', 'w', newline='')
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['Datetime', 'Highest Bid', 'Lowest Ask', 'Mid Price'])

def on_message(ws, message):
    print(message)
    data = json.loads(message)
    if 'e' in data and data['e'] == 'depthUpdate':
        highest_bid, lowest_ask = get_bid_ask_prices(data)
        if highest_bid is not None and lowest_ask is not None:
            timestamp = data['T']  # Capture the timestamp 'T'
            datetime_str = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
            mid_price = (highest_bid + lowest_ask) / 2
            csv_writer.writerow([datetime_str, highest_bid, lowest_ask, mid_price])

def get_bid_ask_prices(data):
    bids = data['b']
    asks = data['a']
    if bids and asks:
        highest_bid = float(bids[0][0])
        lowest_ask = float(asks[0][0])
        return highest_bid, lowest_ask
    return None, None

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Closed")
    csv_file.close()

def on_open(ws):
    payload = {
        "method": "SUBSCRIBE",
        "params": [
            f"ethusdt@depth"
        ],
        "id": 1
    }
    ws.send(json.dumps(payload))

if __name__ == "__main__":
    url = "wss://stream.binance.com:9443/ws/ethusdt@depth"
    # wss://fstream.binance.com/stream?streams=ethusdt@depth
    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()