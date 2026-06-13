#https://pypi.org/project/websocket_client/
# {
#   "data": [
#     {
#       "p": 7296.89,
#       "s": "BINANCE:BTCUSDT",
#       "t": 1575526691134,
#       "v": 0.011467
#     }
#   ],
#   "type": "trade"
# }

import websocket
import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={os.environ['FINNHUB_API_KEY']}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()