import io
import sys
import asyncio

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import random

import logging

import pyarrow.parquet as pq

import pandas as pd
import yfinance as yf

import duckdb

from botocore.exceptions import ClientError

from yfinance.exceptions import (
    YFInvalidPeriodError,
    YFRateLimitError,
    YFTickerMissingError
)

from ...notify.slack_notify import slack_text_notify
from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

#  存 Parquet（success）
def _save_parquet(
        ticker: str,
        bucket: str,
        object_name: str,
        df: pd.DataFrame
    ) -> bool:

    try:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        try:
            s3_client.head_bucket(Bucket = bucket)
        except ClientError:
            s3_client.create_bucket(Bucket = bucket)

        s3_client.put_object(
            Bucket = bucket,
            Key = object_name,
            Body = buf,
        )
        logger.info(f"{ticker}: 已存入 {bucket}/{object_name}")
        return True

    except Exception as e:
        logger.error(f"{ticker}: MinIO 上傳失敗 - {e}", exc_info=True)
        return False


def get_latest_parquet_buffer_this_month(
        bucket: str,
        prefix: str = "",
    ) -> dict | None:
    """
    從 MinIO 列出指定 bucket/prefix 下，本月份的 .parquet 檔案，
    回傳 last_modified 最新的一筆（含 BytesIO buffer）。

    Returns:
        dict | None，包含：
            - object_name   (str)     : MinIO 完整物件路徑
            - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
            - size          (int)     : 檔案大小（bytes）
            - buffer        (BytesIO) : Parquet 內容，指標在位置 0
        若本月無符合檔案則回傳 None。
    """
    tz_taipei   = ZoneInfo("Asia/Taipei")
    now         = datetime.now(tz_taipei)
    this_year   = now.year
    this_month  = now.month

    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    objects  = response.get("Contents", [])

    # 篩選本月份候選物件，依 last_modified 排序取最新
    candidates = [
        obj for obj in objects
        if obj["Key"].endswith(".parquet")
        and "final_all" not in obj["Key"]
        and obj["LastModified"].astimezone(tz_taipei).year  == this_year
        and obj["LastModified"].astimezone(tz_taipei).month == this_month
    ]

    if not candidates:
        logger.info("本月無符合的 .parquet 檔案")
        return None

    latest = max(candidates, key=lambda o: o["LastModified"])

    try:
        res    = s3_client.get_object(Bucket=bucket, Key=latest["Key"])
        buffer = io.BytesIO(res["Body"].read())
        buffer.seek(0)
        result = {
            "object_name":   latest["Key"],
            "last_modified": latest["LastModified"].astimezone(tz_taipei),
            "size":          latest["Size"],
            "buffer":        buffer,
        }
        logger.info(f"✓ 最新檔案：{latest['Key']}")
        return result

    except Exception as e:
        logger.warning(f"✗ 讀取失敗 {latest['Key']}: {e}")
        return None

# 同步，抓資料
def fetch_price(ticker: str,):

    start = time.perf_counter()

    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period='max')

        # 1.將 index 變成普通欄位，並自動生成新的整數 index。
        df = df.reset_index()
        # 2. 將日期轉換為 datetime 類型
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        elapse = time.perf_counter() - start
        return {
            "ticker": ticker,
            "df": df,
            "status": "success" if not df.empty else "failed",
            "error_type": None,
            "error": None,
            "elapse": elapse,
        }

    except (YFTickerMissingError, YFInvalidPeriodError) as e:
        elapse = time.perf_counter() - start
        logger.warning(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "df": None,
            "status": "failed",
            "error_type": None,
            "error": None,
            "elapse": elapse,
        }

    except YFRateLimitError as e:
        elapse = time.perf_counter() - start
        logger.warning(f"{ticker}: {e}")
        return {
            "ticker": ticker,
            "df": None,
            "status": "retry",
            "error_type": None,
            "error": None,
            "elapse": elapse,
        }

    except Exception as e:
        elapse = time.perf_counter() - start
        logger.error(f"{ticker} failed: {e}", exc_info=True)
        return {
            "ticker": ticker,
            "df": None,
            "status": "failed",
            "error_type": "exception",
            "error": str(e),
            "elapse": elapse,
        }

# 非同步，包裝 fetch_price(同步)
async def fetch_one(ticker: str):
    # 取得當前 event loop，避免阻塞 event loop
    loop = asyncio.get_running_loop()
    # run_in_executor 把同步函式丟進執行緒池
    result = await loop.run_in_executor(
        None,                        # None 表示使用預設的 ThreadPoolExecutor
        lambda: fetch_price(ticker)  # 將同步函式包成 lambda 傳入執行緒
    )
    return result

# 非同步，統籌所有 fetch_one
async def fetch_all(tickers: list):
    # 限制同時最多 n 個 ticker 並發執行，避免對 yfinance 發送過多請求
    sem = asyncio.Semaphore(20)

    async def fetch_with_sem(ticker):
        await asyncio.sleep(random.uniform(0, 1))
        # 進入 semaphore，超過 n 個時自動等待，執行完畢後釋放名額
        async with sem:
            # 等這個 fetch_one 抓完，期間 event loop 可以去跑其他協程
            crawl_result = await fetch_one(ticker)

        if crawl_result["status"] == "success":
            print(f"{crawl_result['ticker']}: 爬蟲抓取成功")  # ✅ 暫時替代
            # 取得當前 event loop，避免阻塞 event loop
            loop = asyncio.get_running_loop()
            # _save_parquet 是同步函式，包進執行緒池避免阻塞 event loop
            await loop.run_in_executor(
                None,
                lambda: _save_parquet(
                    crawl_result["ticker"],
                    MINIO_BUCKET,
                    f"stock/history/prices/bronze/{crawl_result['ticker']}.parquet",
                    crawl_result["df"]
                )
            )

        return crawl_result
    
    # 針對需要 第1次~第n次 的爬蟲
    async def fetch_with_retry(ticker, max_retry=3):
        result = None

        for attempt in range(1, max_retry + 1):

            # 每次 attempt 開始前先等待（第1次不等）
            if attempt > 1:
                wait = (attempt - 1) * 10   # 第2次等5秒，第3次等10秒
                print(f"{ticker}: retry {attempt}/{max_retry}，等待 {wait} 秒...")
                await asyncio.sleep(wait)

            result = await fetch_with_sem(ticker)

            if result["status"] != "retry":
                return result   # success 或 failed 直接回傳

        # n次都是 retry
        print(f"{ticker}: 超過最大重試數: 3次 !!")
        return result
    
    total_start = time.perf_counter()
    results = await asyncio.gather(
        # 將所有 ticker 展開成獨立的 coroutine，同時並發執行
        *[fetch_with_retry(t) for t in tickers],
        # 單一 ticker 失敗時不中斷其他任務，例外會以物件形式放進 results
        return_exceptions=True
    )
    total_elapse = time.perf_counter() - total_start
    print(f"本輪總共花費時間:{total_elapse}")

    success, retry, failed = [], [], []

    for result in results:
        if isinstance(result, Exception):
            failed.append(result)

        elif not isinstance(result, dict):
            failed.append(result)

        else:
            status = result.get("status")

            if status == "success":
                success.append(result)
            elif status == "retry":
                retry.append(result)
            else:
                failed.append(result)

    return success, retry, failed

# 文字摘要
def text_summary(success, retry, failed) -> str:
    lines = ["📊 ==1.股價抓取結果摘要=="]
    
    lines.append(f"\n✅ 成功 success ({len(success)})")
    for r in success:
        lines.append(f"  • {r['ticker']}（{r['elapse']:.2f}s）")

    lines.append(f"\n🔁 待重試 retry ({len(retry)})")
    for r in retry:
        lines.append(f"  • {r['ticker']}")

    lines.append(f"\n❌ 失敗 failed ({len(failed)})")
    for r in failed:
        if isinstance(r, dict):
            err = r.get("error") or "unknown"
            lines.append(f"  • {r['ticker']}：{err}")
        else:
            lines.append(f"  • {r}")

    return "\n".join(lines)

def main():
    # 1. 取得本月檔案 buffer 列表
    parquet_file = get_latest_parquet_buffer_this_month(
        bucket= MINIO_BUCKET,
        prefix= "stock/screening/"
    )
    
    if parquet_file:
        conn = duckdb.connect()

        # BytesIO → PyArrow Table → DuckDB
        arrow_table = pq.read_table(parquet_file["buffer"])
        conn.register("us_co_screen", arrow_table)

        df = conn.execute(f"""
            SELECT * FROM "us_co_screen" 
        """).df()

        tickers = df["股票代碼"].tolist()

        # tickers = ["BRK-B", "V", "MA", "PG", "KO", "PEP", "PM", "MO", "TSLAp"]
        success, retry, failed = asyncio.run(fetch_all(tickers))

        print(f"success={len(success)}\nfailed={len(failed)}\nretry={len(retry)}")

        summary = text_summary(success, retry, failed)
        slack_text_notify(summary)

    return


if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()        