import io
import sys
from pathlib import Path

import pandas as pd

import logging

from datetime import date, datetime
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

import duckdb
import pyarrow.parquet as pq

from ...notify.slack_notify import slack_text_notify
from ...config.minio_conn import s3_client, MINIO_BUCKET

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

def _save_parquet(
    df: pd.DataFrame,
    bucket: str,
    object_name: str,
    ) -> bool:

    try:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(
            Bucket = bucket,
            Key = object_name,
            Body = buf,
        )
        logger.info(f"已存入 {bucket}/{object_name}，共 {len(df)} 筆")
        return True

    except Exception as e:
        logger.error(f"MinIO 上傳失敗 - {e}", exc_info=True)
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

def text_summary(save_results: list[dict]) -> str:
    success = [r for r in save_results if r["status"] == "success"]
    failed  = [r for r in save_results if r["status"] == "failed"]

    lines = ["📐==3.Gold 指標計算結果摘要=="]

    lines.append(f"\n✅ 成功 ({len(success)})")
    for r in success:
        lines.append(f"  • {r['parquet_name']}")

    lines.append(f"\n❌ 失敗 ({len(failed)})")
    for r in failed:
        lines.append(f"  • {r['parquet_name']}")

    return "\n".join(lines)

def main():

    # 1. 取得今日檔案 buffer 列表
    parquet_files = get_latest_parquet_buffer_this_month(
        bucket= MINIO_BUCKET,
        prefix= "stock/fundamentals/"
    )

    # 
    if parquet_files:
        conn = duckdb.connect()

        # BytesIO → PyArrow Table → DuckDB
        arrow_table = pq.read_table(parquet_files["buffer"])
        conn.register("us_co_screen", arrow_table)

        df = conn.execute(Path("/app/stock_analyzer_us/us/data_pipeline/company_screening/us_fundamentals_screen.sql").read_text()).df()
        df["紀錄日期"] = pd.to_datetime(df["紀錄日期"]).dt.date

        conn.close()

        _save_parquet(
            df = df,
            bucket = MINIO_BUCKET,
            object_name = f"stock/screening/{date.today()}_us_co_screen.parquet"
        )
        
        logger.info(f"篩選結果：{len(df)} 筆")
        print(df)


    # results 文字摘要（供 slack send）
    # summary = text_summary(save_results)
    # slack_text_notify(summary)

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()