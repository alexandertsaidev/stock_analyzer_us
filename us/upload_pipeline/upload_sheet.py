import pandas as pd
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import io
import sys
import logging

import pyarrow.parquet as pq

from pathlib import Path

import duckdb

import gspread
from google.oauth2.service_account import Credentials

from ..config.minio_conn import s3_client, MINIO_BUCKET
from ..utils.helpers import countdown

logger = logging.getLogger(__name__)

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

def rewrite_gsheet(df, sheet_name, worksheet_name):

    # 1.權限
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # 2.憑證
    creds = Credentials.from_service_account_file(
        Path("/app/stock_analyzer_us/us/upload_pipeline/credentials.json"),
        scopes=scope
    )

    # 3.連線
    client = gspread.authorize(creds)
    worksheet = client.open(sheet_name).worksheet(worksheet_name)

    # 4.處理 df 
    df = df.copy()

    if "Date_" in df.columns:

        # 只處理欄位名稱含 "Date_"
        date_cols = [col for col in df.columns if "Date_" in col]

        # 處理日期欄位
        df[date_cols] = (
            df[date_cols]
            .apply(lambda x: pd.to_datetime(x, errors="coerce").dt.strftime("%Y-%m-%d"))
        )

    # 5.清空
    worksheet.clear()
    
    # 6.一次寫入
    worksheet.update(
        [df.columns.tolist()] + df.astype(str).values.tolist()
    )

    return

def main():

    try:
        # 1. 取得本月檔案 buffer 列表
        parquet_file = get_latest_parquet_buffer_this_month(
            bucket= MINIO_BUCKET,
            prefix= "stock/history/prices/gold/conclusion/"
        )
        
        if parquet_file:
            conn = duckdb.connect()

            # BytesIO → PyArrow Table → DuckDB
            arrow_table = pq.read_table(parquet_file["buffer"])
            conn.register("us_co_screen", arrow_table)

            df = conn.execute(f"""
                SELECT * FROM "us_co_screen"
                ORDER BY "Date_D" DESC, "ticker" ASC
            """).df()

            time.sleep(1)
            rewrite_gsheet(df, "entry_conclusion", "US")
        
    except Exception as e:
        print("發生錯誤 !", e)

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()