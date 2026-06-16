import os
import threading

import sys
import json

import requests
import websocket

import signal

import random

import logging

import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import duckdb
from botocore.exceptions import ClientError

from dotenv import load_dotenv, find_dotenv

from kafka import KafkaProducer

from ...config.minio_conn import MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

# ---- 載入 .env 環境變數 ----
load_dotenv(find_dotenv())

# ---- 設定 ----
API_KEY = os.environ["FINNHUB_API_KEY"]
# SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN", "V"]

MAX_RETRIES = 100
retry_count = 0

ws_ref = [None]  # 用 list 當 mutable container

# ---- 建立 Kafka Producer（整個程式只建一次）----
producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def get_co_fetch_list(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    object_name: str
    ) -> list[str]:

    try:
        tickers = conn.execute(f"""
            SELECT DISTINCT "ticker"
            FROM read_parquet('s3://{bucket}/{object_name}')
            WHERE "created_at" >= CURRENT_DATE - INTERVAL '2 years'
        """).df()["ticker"].tolist()
        
        logger.info(f"從 {bucket}/{object_name} 取得 {len(tickers)} 檔")
        return tickers

    except duckdb.HTTPException as e:
        logger.error(f"DuckDB 讀取 S3 失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise FileNotFoundError(f"找不到檔案: s3://{bucket}/{object_name}") from e

    except ClientError as e:
        # 保留 boto3 ClientError，以防其他地方仍用 s3_client
        logger.error(f"MinIO 讀取失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"讀取 {bucket}/{object_name} 發生未知錯誤: {e}", exc_info=True)
        raise


# ---- Graceful Shutdown：收到 Ctrl+C 或 SIGTERM 時安全關閉 ----
def shutdown(sig, frame):
    print("\n⛔ 關閉中...")
    try:
        if ws_ref[0]:
            ws_ref[0].close()  # ← 用 ws_ref 更安全

    except Exception:
        pass
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ---- WebSocket Callbacks ----

def fetch_holiday_hours(date_str: str) -> str | None:
    """
    查詢指定日期是否為假日。
    回傳：
      None        → 非假日，正常交易
      ""          → 假日休市
      "09:30-13:00" → 假日但縮短交易
    """
    url = f"https://finnhub.io/api/v1/stock/market-holiday?exchange=US&token={API_KEY}"
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json()
        for event in data.get("data", []):
            if event.get("atDate") == date_str:
                return event.get("tradingHour", "")
        return None  # 不在假日清單 → 正常交易日
    except Exception as e:
        print(f"⚠️ 假日 API 失敗，預設當正常交易日: {e}")
        return None  # API 炸了就當正常日處理，不中斷


def parse_trading_hours(trading_hour: str):
    """解析 '09:30-13:00' → (time(9,30), time(13,0))"""
    start_str, end_str = trading_hour.split("-")
    sh, sm = map(int, start_str.split(":"))
    eh, em = map(int, end_str.split(":"))
    return dtime(sh, sm), dtime(eh, em)


def get_market_window(tz) -> tuple[dtime, dtime] | None:
    """
    回傳今日交易時段 (open_time, close_time)。
    None 表示今日休市。
    """
    now_ny = datetime.now(ZoneInfo(tz))

    # 週末直接休市
    if now_ny.weekday() >= 5:
        return None

    date_str = now_ny.strftime("%Y-%m-%d")
    holiday_hours = fetch_holiday_hours(date_str)

    if holiday_hours is None:
        # 正常交易日
        return dtime(9, 30), dtime(16, 0)
    
    elif holiday_hours == "":
        # 假日休市
        return None
    
    else:
        # 假日縮短交易
        return parse_trading_hours(holiday_hours)


def market_close_watcher(ws_ref: list, tz, close_time):
    """用 list 包裝 ws，確保拿到最新的 ws 物件"""
    while True:
        
        now_ny = datetime.now(ZoneInfo(tz)).time()

        if now_ny >= close_time:
            print(f"🕐 已收盤（{close_time}），停止程式")

            try:
                producer.flush()
                producer.close()
                if ws_ref[0]:
                    ws_ref[0].close()
            
            except Exception as e:
                print(f"⚠️ 關閉時發生錯誤: {e}")
            finally:
                sys.exit(0)

        time.sleep(30)


def on_open(ws, tickers, sleep_range=(0.05, 0.2)):
    global retry_count
    retry_count = 0          # ← 重連成功，次數歸零
    ws_ref[0] = ws           # ← 更新 ws_ref，watcher 拿到最新物件

    print("✅ 已連線，訂閱中...")

    for symbol in tickers:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"  📡 訂閱 {symbol}")
        
        time.sleep(random.uniform(*sleep_range))

    print("-" * 50)


def on_message(ws, message):

    try:
        data = json.loads(message)
        if data.get("type") == "trade":
            for trade in data.get("data", []):
                future = producer.send(
                    "trades",
                    value=trade,
                    key=trade.get("s"),
                )
                future.add_callback(
                    lambda m, s=trade.get("s"): print(f"✅ Produced | {s} | partition={m.partition} | offset={m.offset}")
                )
                future.add_errback(lambda e: print(f"⚠️ Kafka 發送失敗: {e}"))

    except (json.JSONDecodeError, KeyError) as e:
        print(f"⚠️ 訊息解析失敗: {e}")


def on_error(ws, error):
    """發生錯誤時：區分錯誤類型並主動關閉連線"""

    if isinstance(error, ConnectionRefusedError):
        print("\n❌ 無法連線到伺服器")

    elif isinstance(error, TimeoutError):
        print("\n❌ 連線逾時")

    elif isinstance(error, KeyboardInterrupt):
        print("\n⛔ 使用者中斷")
        producer.flush()   # ← 先清空 Kafka buffer
        producer.close()   # ← 再關閉 producer
        ws.close()         # ← 最後關閉 WebSocket
        sys.exit(0)
    else:
        print(f"\n❌ 未知錯誤: {error}")


def on_close(ws, close_status_code, close_msg):
    """連線關閉時：檢查重連次數、清空 Producer 緩衝區"""
    global retry_count
    retry_count += 1
    print(f"🔌 連線關閉（第 {retry_count} 次）")

    if retry_count >= MAX_RETRIES:
        print("❌ 重連次數過多，停止")
        producer.flush()
        producer.close()
        sys.exit(1)

    producer.flush()


def main():

    conn = get_duckdb_conn()

    tickers = get_co_fetch_list(
        conn = conn,
        bucket= MINIO_BUCKET,
        object_name= f"stock/screening/us_all_co_screen.parquet"
    )

    # ---- 啟動 WebSocket ----
    tz = "America/New_York"

    window = get_market_window(tz)

    if window is None:
        print("🕐 今日休市，結束程式")
        sys.exit(0)

    open_time, close_time = window
    now_ny = datetime.now(ZoneInfo(tz)).time()

    if now_ny >= close_time:
        print("🕐 已收盤，結束程式")
        sys.exit(0)

    if now_ny < open_time:
        wait_sec = (
            datetime.combine(datetime.now(ZoneInfo(tz)).date(), open_time) -
            datetime.combine(datetime.now(ZoneInfo(tz)).date(), now_ny)
        ).seconds

        for i in range(wait_sec, 0, -1):
            h = i // 3600
            m = (i % 3600) // 60
            s = i % 60
            print(f"⏳ 開盤倒數 {h} 小時 {m} 分 {s} 秒...", end='\r')
            time.sleep(1)

    print(f"\n✅ 現在 {tz} 時間 {datetime.now(ZoneInfo(tz)).strftime('%H:%M:%S')} \n正在建立連線...")

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_open=lambda ws: on_open(ws=ws, tickers=tickers),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    watcher = threading.Thread(
        target=market_close_watcher,
        args=(ws_ref, tz, close_time),
        daemon=True
    )
    watcher.start()

    ws.run_forever(reconnect=15, ping_interval=60, ping_timeout=20)


if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()
