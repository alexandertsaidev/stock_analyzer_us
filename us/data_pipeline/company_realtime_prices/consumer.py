import os
import io
import threading

import json
from collections import defaultdict

import logging

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer

from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.minio_duckdb_conn import get_duckdb_conn

from ...notify.slack_notify import slack_price_notify

from dotenv import load_dotenv, find_dotenv

import duckdb
from botocore.exceptions import ClientError

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)

EXCLUDE_CODES = {"24", "25"}   # 盤外成交


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


def load_alert_config() -> dict:
    conn = get_duckdb_conn()
    tickers = get_co_fetch_list(
        conn = conn,
        bucket= MINIO_BUCKET,
        object_name= f"stock/screening/us_all_co_screen.parquet"
    )

    # DuckDB 支援直接傳 list 參數，不需手動拼 SQL 字串
    result = conn.execute("""
        SELECT "ticker", "upperband", "lowerband", "upper_1_7", "lower_1_7"
        FROM read_parquet('s3://us-stock/stock/history/prices/gold/final_all/us_all_prices.parquet')
        WHERE "period" = 'D'
        AND "ticker" = ANY($1)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY "ticker" ORDER BY "Date" DESC) = 1
    """, [tickers]).fetchall()

    config = {}
    for ticker, upper, lower, upper_17, lower_17 in result:
        config[ticker] = [
            {"threshold": upper,    "cooldown": 60, "level": "critical",  "label": "upperband"},
            {"threshold": lower,    "cooldown": 60, "level": "critical",  "label": "lowerband"},
            {"threshold": upper_17, "cooldown": 30, "level": "warning", "label": "upper_1_7"},
            {"threshold": lower_17, "cooldown": 30, "level": "warning", "label": "lower_1_7"},
        ]

    # 找出沒有對應 Parquet 資料的 ticker，方便 debug
    missing = set(tickers) - set(config.keys())
    if missing:
        print(f"[load_alert_config] ⚠️ 無資料的 ticker：{missing}")

    return config


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

        # 將 Producer 送進來的 bytes, value 反序列化
        # bytes → decode("utf-8") → JSON 字串 → dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),

        # 將 Producer 送進來的 bytes, key(symbol bytes) 反序列化
        # bytes → decode("utf-8") → 字串
        key_deserializer=lambda k: k.decode("utf-8"),

        # 同一個 group_id 的 Consumer 共享 offset（同一組）,不同各自獨立消費
        group_id=group_id,
        enable_auto_commit=False,  # ← 關掉 auto commit
    )

    return consumer
    # 回傳的 consumer 是一個可迭代物件
    # 呼叫端直接 for msg in consumer 就能持續讀取訊息
    # msg.key   → "AAPL"               （字串，已 deserialize）
    # msg.value → {"s":"AAPL","p":182.5,...} （dict，已 deserialize）
    # msg.topic     → "trades"
    # msg.partition → 0
    # msg.offset    → 目前讀到第幾筆


# def run_minio_consumer():
#     consumer = make_consumer("minio-consumer")
#     buf = []
#     last_flush = time.time()

#     tz = ZoneInfo("America/New_York")
    
#     for msg in consumer:
#         try:
#             buf.append(msg.value)

#             if len(buf) >= 500 or time.time() - last_flush > 10:

#                 # dedup：用 (t, s, p, v) 當唯一鍵
#                 seen = set()
#                 deduped = []
#                 for r in buf:
#                     key = (r["t"], r["s"], r["p"], r["v"])
#                     if key not in seen:
#                         seen.add(key)
#                         deduped.append(r)

#                 groups = defaultdict(list)
#                 for record in deduped:
#                     groups[record["s"]].append(record)

#                 for symbol, records in groups.items():
#                     # 用資料實際交易時間當檔名
#                     t_start = datetime.fromtimestamp(records[0]["t"] / 1000, tz = tz)
#                     t_end   = datetime.fromtimestamp(records[-1]["t"] / 1000, tz = tz)
#                     date    = t_start.strftime("%Y-%m-%d")
#                     ts      = f"{t_start.strftime('%H-%M-%S')}_{t_end.strftime('%H-%M-%S')}"

#                     jsonl_bytes = io.BytesIO(
#                         "\n".join(json.dumps(r) for r in records).encode("utf-8")
#                     )
#                     key = f"stock/real-time/prices/bronze/{symbol}/{date}/{ts}.jsonl"
#                     s3_client.put_object(
#                         Bucket=MINIO_BUCKET,
#                         Key=key,
#                         Body=jsonl_bytes.getvalue(),
#                         ContentLength=jsonl_bytes.getbuffer().nbytes,
#                         ContentType="application/octet-stream",
#                     )
#                     print(f"📦 MinIO flush {len(records):>4} 筆  {symbol:<6} → {key}")

#                 buf = []
#                 last_flush = time.time()
#                 consumer.commit()

#         except Exception as e:
#             print(f"❌ minio-consumer 錯誤: {e}")


def run_minio_consumer():
    consumer = make_consumer("minio-consumer")
    buf = []
    last_flush = time.time()
    tz = ZoneInfo("America/New_York")
    
    for msg in consumer:
        try:
            trade = msg.value
            if EXCLUDE_CODES.intersection(trade.get("c", [])):   # ← 盤外略過
                continue

            buf.append(msg.value)

            if len(buf) >= 500 or time.time() - last_flush > 600:

                groups = defaultdict(list)
                for record in buf:          # ← 直接用 buf，不 dedup
                    groups[record["s"]].append(record)

                for symbol, records in groups.items():
                    t_start = datetime.fromtimestamp(records[0]["t"] / 1000, tz=tz)
                    t_end   = datetime.fromtimestamp(records[-1]["t"] / 1000, tz=tz)
                    date    = t_start.strftime("%Y-%m-%d")
                    ts      = f"{t_start.strftime('%H-%M-%S')}_{t_end.strftime('%H-%M-%S')}"

                    jsonl_bytes = "\n".join(json.dumps(r) for r in records).encode("utf-8")
                    key = f"stock/real-time/prices/bronze/{symbol}/{date}/{ts}.jsonl"
                    s3_client.put_object(
                        Bucket=MINIO_BUCKET,
                        Key=key,
                        Body=jsonl_bytes,
                        ContentLength=len(jsonl_bytes),
                        ContentType="application/octet-stream",
                    )
                    print(f"📦 MinIO flush {len(records):>4} 筆  {symbol:<6} → {key}")

                buf = []
                last_flush = time.time()
                consumer.commit()

        except Exception as e:
            print(f"❌ minio-consumer 錯誤: {e}")

def run_monitor_consumer():
    consumer = make_consumer("monitor-consumer")

    for msg in consumer:
        trade = msg.value
        if EXCLUDE_CODES.intersection(trade.get("c", [])):
            continue

        ts = datetime.fromtimestamp(
            trade["t"] / 1000,
            tz=ZoneInfo("America/New_York")
        ).strftime("%H:%M:%S")

        consumer.commit()
        print(f"[{ts}]  {trade['s']:<6}  {trade['p']:>10.2f}  vol:{trade['v']:>6}")

def run_alert_consumer():
    # 啟動時從 Parquet 讀取每個 ticker 的警戒線設定
    ALERT_CONFIG = load_alert_config()

    consumer     = make_consumer("alert-consumer")
    
    prev         = {}   # symbol → 上一筆成交價（用來判斷是否穿越警戒線）
    last_sent    = {}   # (symbol, label) → 上次告警的 timestamp（cooldown 用）

    for msg in consumer:
        trade  = msg.value
        if EXCLUDE_CODES.intersection(trade.get("c", [])):
            continue

        symbol = trade["s"]
        price  = float(trade["p"])
        p      = prev.get(symbol)   # 取上一筆，第一筆進來時為 None

        if p is not None:   # 需要前後兩筆才能判斷穿越，第一筆略過
            for cfg in ALERT_CONFIG.get(symbol, []):   # 逐一檢查該 ticker 的每條警戒線
                threshold = cfg["threshold"]   # 從 Parquet 讀來的實際價位
                cooldown  = cfg["cooldown"]    # 同一條線的告警冷卻秒數
                level     = cfg["level"]       # "warning" / "critical"
                label     = cfg["label"]       # "upperband" / "lowerband" / "1.7_upper" / "1.7_lower"
                key       = (symbol, label)    # 每條線各自獨立的 cooldown key

                crossed   = False
                direction = ""

                if "upper" in label and p <= threshold and price > threshold:
                    # 上軌：前一筆在線下或等於，這一筆升破 → 向上穿越
                    direction = "▲ 升破"
                    crossed   = True
                elif "lower" in label and p >= threshold and price < threshold:
                    # 下軌：前一筆在線上或等於，這一筆跌破 → 向下穿越
                    direction = "▼ 跌破"
                    crossed   = True

                if crossed:
                    now = time.time()
                    if now - last_sent.get(key, 0) > cooldown:   # 確認距上次告警已超過 cooldown
                        emoji = "🚨" if level == "critical" else "⚠️"

                        text = (
                            f"{emoji} [ {level.upper():<8}] "
                            f"{symbol:<6} {direction} "
                            f"{label:<12}({threshold:>8.2f})："
                            f"{p:>8.2f} → {price:>8.2f}"
                        )
                        print(text)
                        slack_price_notify(text)

                        last_sent[key] = now   # 更新該條線的上次告警時間

        prev[symbol] = price   # 更新必須在 loop 最後，讓下一筆能取到這一筆的值
        consumer.commit()

if __name__ == "__main__":
    targets = [run_minio_consumer, run_alert_consumer, run_monitor_consumer]
    threads = [threading.Thread(target=t, daemon=True) for t in targets]
    [t.start() for t in threads]
    [t.join()  for t in threads]

