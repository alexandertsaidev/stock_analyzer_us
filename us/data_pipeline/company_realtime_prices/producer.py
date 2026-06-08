import os
import sys

import json

import signal

from dotenv import load_dotenv, find_dotenv

import websocket
from kafka import KafkaProducer

# ---- 載入 .env 環境變數 ----
load_dotenv(find_dotenv())

# ---- 設定 ----
API_KEY = os.environ["FINNHUB_API_KEY"]                          # Finnhub API 金鑰
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]              # 訂閱的股票代碼
MAX_RETRIES = 10                                                  # 最大重連次數
retry_count = 0                                                   # 當前重連次數

# ---- 建立 Kafka Producer（整個程式只建一次）----
producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],     # Kafka broker 位置
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),    # value 自動轉 JSON
    key_serializer=lambda k: k.encode("utf-8"),                  # key 轉 bytes
)

# ---- Graceful Shutdown：收到 Ctrl+C 或 SIGTERM 時安全關閉 ----
def shutdown(sig, frame):
    print("⛔ 關閉中...")
    ws.close()           # 關閉 WebSocket 連線
    producer.flush()     # 將緩衝區剩餘資料全部送出
    producer.close()     # 關閉 Producer
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)   # 對應 Ctrl+C
signal.signal(signal.SIGTERM, shutdown)  # 對應 Docker stop / kill

# ---- WebSocket Callbacks ----

def on_open(ws):
    """連線建立時：重置重連計數，並訂閱所有股票"""
    global retry_count
    retry_count = 0  # 連線成功，重置重連計數
    print("✅ 已連線，訂閱中...")
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"  📡 訂閱 {symbol}")
    print("-" * 50)

def on_message(ws, message):
    """收到訊息時：解析 trade 資料並送進 Kafka"""
    try:
        data = json.loads(message)
        if data.get("type") == "trade":
            for trade in data.get("data", []):
                future = producer.send(
                    "trades",           # Kafka topic
                    value=trade,        # 整包 trade dict，自動 JSON 序列化
                    key=trade.get("s"), # 用 symbol 當 partition key，確保同股票進同 partition
                )
                # 非同步發送，透過 callback 捕捉失敗
                future.add_errback(lambda e: print(f"⚠️ Kafka 發送失敗: {e}"))
    except (json.JSONDecodeError, KeyError) as e:
        # 避免壞掉的訊息讓整個程式 crash
        print(f"⚠️ 訊息解析失敗: {e}")

def on_error(ws, error):
    """發生錯誤時：區分錯誤類型並主動關閉連線"""
    if isinstance(error, ConnectionRefusedError):
        print("❌ 無法連線到伺服器")
    elif isinstance(error, TimeoutError):
        print("❌ 連線逾時")
    else:
        print(f"❌ 未知錯誤: {error}")
    ws.close()  # 主動關閉，避免殭屍連線，讓 reconnect 機制接手

def on_close(ws, close_status_code, close_msg):
    """連線關閉時：取消訂閱、檢查重連次數、清空 Producer 緩衝區"""
    global retry_count
    retry_count += 1
    print("🔌 連線關閉")

    # 嘗試告知 server 取消訂閱（連線可能已斷，所以用 try/except）
    for symbol in SYMBOLS:
        try:
            ws.send(json.dumps({"type": "unsubscribe", "symbol": symbol}))
        except:
            pass  # 連線已斷就忽略，不影響後續流程

    # 超過重連上限則強制停止
    if retry_count >= MAX_RETRIES:
        print("❌ 重連次數過多，停止")
        producer.flush()
        producer.close()
        sys.exit(1)

    # 每次關閉都確保緩衝區資料送出
    producer.flush()
    producer.close()

# ---- 啟動 WebSocket ----
ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={API_KEY}",  # Finnhub WebSocket 端點
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

ws.run_forever(
    reconnect=5,       # 斷線後每 5 秒自動重連
    ping_interval=30,  # 每 30 秒送一次 ping，維持連線心跳
    ping_timeout=10,   # 10 秒內沒有 pong 視為斷線
)