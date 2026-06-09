import os
import io
import threading

import json

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer

from ...config.minio_conn import s3_client, MINIO_BUCKET

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def make_consumer(group_id):
    """
    建立並回傳一個 KafkaConsumer 實例
    每個 Consumer 傳入不同 group_id，確保各自維護獨立的 offset
    
    Args:
        group_id: Consumer Group 名稱
                  ex: "minio-consumer" / "alert-consumer" / "monitor-consumer"
    """
    
    consumer = KafkaConsumer(
        "trades", # 訂閱 Producer 送進來的的資料 topic
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], # Kafka Broker 位置

        # 將 Producer 送進來的 bytes,value 反序列化
        # bytes → decode("utf-8") → JSON 字串 → dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),

        # 將 Producer 送進來的 bytes,key 反序列化
        # Producer 的 key 是 symbol bytes
        # bytes → decode → 字串
        key_deserializer=lambda k: k.decode("utf-8"),

        # 同一個 group_id 的 Consumer 共享 offset（同一組）
        # 不同 group_id 各自獨立消費，互不影響
        group_id=group_id,
    )

    return consumer
    # 回傳的 consumer 是一個可迭代物件
    # 呼叫端直接 for msg in consumer 就能持續讀取訊息
    # msg.key   → "AAPL"               （字串，已 deserialize）
    # msg.value → {"s":"AAPL","p":182.5,...} （dict，已 deserialize）
    # msg.topic     → "trades"
    # msg.partition → 0
    # msg.offset    → 目前讀到第幾筆

def run_minio_consumer(consumer_id):

    consumer = make_consumer(consumer_id)

    # 暫存緩衝區，累積後批次寫入，避免每筆都打 MinIO
    buf = []

    # 記錄上次 flush 的時間，用於時間條件判斷
    last_flush = time.time()

    for msg in consumer:

        # msg.value 已被 deserializer 還原為 dict
        # 例：{"s":"AAPL","p":182.50,"v":120,"t":1705123456789,"c":[]}
        buf.append(msg.value)

        # flush 條件：500 筆 or 10 秒，擇先觸發
        if len(buf) >= 500 or time.time() - last_flush > 10:

            # ── 依 symbol 分組，各自寫入獨立路徑 ────────────────
            # 避免 AAPL / TSLA / MSFT 混在同一個檔案
            # 之後用 DuckDB 查詢時可以 partition pruning，只讀需要的 symbol
            from collections import defaultdict
            groups = defaultdict(list)
            for record in buf:
                groups[record["s"]].append(record)

            # ── 取當下時間，作為檔案命名依據 ─────────────────────
            # ts = datetime.now().strftime("%Y-%m-%d/%H-%M-%S")
            ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d/%H-%M-%S")
            for symbol, records in groups.items():

                # ── 序列化成 JSON Lines（每行一筆 JSON）─────────
                # JSON Lines 優點：
                #   - 可逐行讀取，不需全載入記憶體
                #   - 單行損壞不影響其他行
                #   - 適合 streaming append 場景
                jsonl_bytes = io.BytesIO(
                    "\n".join(json.dumps(r) for r in records).encode("utf-8")
                )

                # ── 物件路徑：依 symbol / date / time 分區 ───────
                # 例：trades/raw/AAPL/2024-01-15/15-32-00.jsonl
                key = f"stock/real-time/prices/bronze/{symbol}/{ts}.jsonl"

                # ── 寫入 MinIO ────────────────────────────────────
                # length 必填：MinIO streaming 寫入需提前知道大小
                s3_client.put_object(
                    MINIO_BUCKET,                           # bucket 名稱
                    key,                                   # 物件路徑
                    jsonl_bytes,                           # 資料內容
                    length=jsonl_bytes.getbuffer().nbytes, # 資料大小（bytes）
                    content_type="application/octet-stream",
                )
                print(f"📦 MinIO flush {len(records):>4} 筆  {symbol:<6} → {key}")

            # ── flush 完成，重置緩衝區和計時器 ───────────────────
            buf = []
            last_flush = time.time()

def run_monitor_consumer():
    consumer = make_consumer("monitor-consumer")

    for msg in consumer:
        trade = msg.value
        # ts    = datetime.fromtimestamp(trade["t"] / 1000).strftime("%H:%M:%S")
        ts = datetime.fromtimestamp(trade["t"] / 1000, tz=ZoneInfo("America/New_York")).strftime("%H:%M:%S")
        print(f"[{ts}]  {trade['s']:<6}  {trade['p']:>10.2f}  vol:{trade['v']}")

if __name__ == "__main__":
    targets = [run_minio_consumer, run_monitor_consumer]
    threads = [threading.Thread(target=t, daemon=True) for t in targets]
    [t.start() for t in threads]
    [t.join()  for t in threads]
