
import websocket
import json
from datetime import datetime

API_KEY = "d8gpmchr01qhjpmp42mgd8gpmchr01qhjpmp42n0"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

def on_open(ws):
    print("✅ 已連線，訂閱中...")
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"  📡 訂閱 {symbol}")
    print("-" * 50)

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for trade in data.get("data", []):
            symbol = trade.get("s")
            price  = trade.get("p")
            volume = trade.get("v")
            ts     = datetime.fromtimestamp(trade.get("t") / 1000).strftime("%H:%M:%S")
            print(f"[{ts}]  {symbol:<6}  價格: {price:>10.2f}  量: {volume}")

def on_error(ws, error):
    print(f"❌ 錯誤: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔌 連線關閉")

ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={API_KEY}",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)
ws.run_forever()