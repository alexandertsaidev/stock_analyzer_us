import os
import io
import threading

import json

import time
from datetime import datetime

from kafka import KafkaConsumer

from ...config.minio_conn import s3_client, MINIO_BUCKET

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def make_consumer(group_id):
    return KafkaConsumer(
        "trades",
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8"),
        group_id=group_id,
    )

# ──────────────────────────────────────────────────────────────
# Consumer 1：MinIO 落地
# 職責：從 Kafka 讀取 trade，批次寫入 MinIO（JSON Lines 格式）
# 路徑：tick-data/trades/raw/{symbol}/{date}/{time}.jsonl
# ──────────────────────────────────────────────────────────────
def run_minio_consumer():

    # 建立 Kafka Consumer，使用獨立 group_id
    # 與其他 Consumer 互不影響 offset
    consumer = make_consumer("minio-consumer")

    # 暫存緩衝區，累積後批次寫入，避免每筆都打 MinIO
    buf = []

    # 記錄上次 flush 的時間，用於時間條件判斷
    last_flush = time.time()

    for msg in consumer:

        # msg.value 已被 deserializer 還原為 dict
        # 例：{"s":"AAPL","p":182.50,"v":120,"t":1705123456789,"c":[]}
        buf.append(msg.value)

        # ── flush 條件：500 筆 or 10 秒，擇先觸發 ──────────────
        # 盤中熱絡：很快滿 500 筆 → 筆數條件先到
        # 盤後冷清：不易滿 500 筆 → 時間條件先到，確保定期落地
        if len(buf) >= 500 or time.time() - last_flush > 10:

            # ── 依 symbol 分組，各自寫入獨立路徑 ────────────────
            # 避免 AAPL / TSLA / MSFT 混在同一個檔案
            # 之後用 DuckDB 查詢時可以 partition pruning，只讀需要的 symbol
            from collections import defaultdict
            groups = defaultdict(list)
            for record in buf:
                groups[record["s"]].append(record)

            # ── 取當下時間，作為檔案命名依據 ─────────────────────
            ts = datetime.now().strftime("%Y-%m-%d/%H-%M-%S")

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
                key = f"trades/raw/{symbol}/{ts}.jsonl"

                # ── 寫入 MinIO ────────────────────────────────────
                # length 必填：MinIO streaming 寫入需提前知道大小
                s3_client.put_object(
                    "tick-data",                           # bucket 名稱
                    key,                                   # 物件路徑
                    jsonl_bytes,                           # 資料內容
                    length=jsonl_bytes.getbuffer().nbytes, # 資料大小（bytes）
                    content_type="application/octet-stream",
                )
                print(f"📦 MinIO flush {len(records):>4} 筆  {symbol:<6} → {key}")

            # ── flush 完成，重置緩衝區和計時器 ───────────────────
            buf = []
            last_flush = time.time()


# ──────────────────────────────────────
# Consumer 2：告警（price cross 90）
# ──────────────────────────────────────
def run_alert_consumer():
    consumer  = make_consumer("alert-consumer")
    prev      = {}   # symbol → 上一筆價格
    last_sent = {}   # symbol → 上次告警時間

    for msg in consumer:
        trade  = msg.value
        symbol = trade["s"]
        price  = float(trade["p"])
        p      = prev.get(symbol)

        if p is not None and p >= 90 and price < 90:
            now = time.time()
            if now - last_sent.get(symbol, 0) > 60:
                text = f"⚠️ {symbol} 跌破 90：{p:.2f} → {price:.2f}"
                print(text)
                # requests.post(SLACK_WEBHOOK, json={"text": text})
                last_sent[symbol] = now

        prev[symbol] = price

# ──────────────────────────────────────
# Consumer 3：即時監控 print
# ──────────────────────────────────────
def run_monitor_consumer():
    consumer = make_consumer("monitor-consumer")

    for msg in consumer:
        trade = msg.value
        ts    = datetime.fromtimestamp(trade["t"] / 1000).strftime("%H:%M:%S")
        print(f"[{ts}]  {trade['s']:<6}  {trade['p']:>10.2f}  vol:{trade['v']}")

# ──────────────────────────────────────
# 啟動全部
# ──────────────────────────────────────
if __name__ == "__main__":
    targets = [run_minio_consumer, run_alert_consumer, run_monitor_consumer]
    threads = [threading.Thread(target=t, daemon=True) for t in targets]
    [t.start() for t in threads]
    [t.join()  for t in threads]