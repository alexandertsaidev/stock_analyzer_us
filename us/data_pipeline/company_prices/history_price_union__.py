import io
import sys

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from pathlib import Path

import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ...notify.slack_notify import slack_text_notify
from ...config.minio_conn import s3_client, MINIO_BUCKET
from ...config.snowflake_conn import snow_conn

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

LOCAL_PARQUET_PATH      = Path("us_all_prices.parquet")
LOCAL_PARQUET_MINIO_KEY = "stock/history/prices/gold/final_all/us_all_prices.parquet"
GOLD_PREFIX           = "stock/history/prices/gold/"


# ── Step 1：從 MinIO 讀取今日 Parquet ────────────────────────────────────────
def get_today_parquet_buffers(
        bucket: str,
        prefix: str = "",
    ) -> list[dict]:
    """
    從 MinIO 列出指定 bucket/prefix 下，今日修改的 .parquet 檔案，
    讀入記憶體後回傳 BytesIO buffer 列表，供後續 pandas 查詢使用。

    Returns:
        list[dict]，每筆包含：
            - object_name   (str)     : MinIO 完整物件路徑
            - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
            - size          (int)     : 檔案大小（bytes）
            - buffer        (BytesIO) : Parquet 內容，指標在位置 0
    """
    tz_taipei = ZoneInfo("Asia/Taipei")
    today     = datetime.now(tz_taipei).date()

    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    objects  = response.get("Contents", [])

    results = []
    for obj in objects:
        if obj["LastModified"].astimezone(tz_taipei).date() != today:
            continue
        if not obj["Key"].endswith(".parquet"):
            continue

        try:
            res    = s3_client.get_object(Bucket=bucket, Key=obj["Key"])
            buffer = io.BytesIO(res["Body"].read())
            buffer.seek(0)
            results.append({
                "object_name":   obj["Key"],
                "last_modified": obj["LastModified"].astimezone(tz_taipei),
                "size":          obj["Size"],
                "buffer":        buffer,
            })
            logger.info(f"✓ {obj['Key']}")

        except Exception as e:
            logger.warning(f"✗ {obj['Key']}: {e}")

    logger.info(f"共 {len(results)} 個檔案")
    return results


# ── Step 2：合併今日 buffers → DataFrame ─────────────────────────────────────
def merge_buffers_to_df(buffers: list[dict]) -> pd.DataFrame:
    """將多個 BytesIO parquet buffer 合併為單一 DataFrame。"""
    frames = []
    for item in buffers:
        item["buffer"].seek(0)
        frames.append(pd.read_parquet(item["buffer"]))

    df = pd.concat(frames, ignore_index=True)
    df["Date"] = pd.to_datetime(df["Date"])
    return df


# ── Step 3：讀取 / 初始化目標 Parquet ────────────────────────────────────────
def load_existing_parquet(path: Path) -> pd.DataFrame | None:
    """
    讀取本地目標 Parquet；不存在時回傳 None。
    """
    if not path.exists():
        logger.info(f"目標 Parquet 不存在，將新建：{path}")
        return None

    logger.info(f"目標 Parquet 已存在，載入：{path}")
    df = pd.read_parquet(path)
    df["Date"] = pd.to_datetime(df["Date"])
    return df


# ── Step 4：刪除指定 tickers 最新 N 筆 ───────────────────────────────────────
def drop_latest_n_by_ticker(
        df: pd.DataFrame,
        tickers: list[str],
        n: int = 2,
    ) -> pd.DataFrame:
    """
    對 df 中屬於 tickers 的資料，依 Date 排序後移除最新 n 筆。
    不在 tickers 內的資料保持不動。
    """
    def _drop(group: pd.DataFrame) -> pd.DataFrame:
        if group.name not in tickers:
            return group
        sorted_g = group.sort_values("Date")
        return sorted_g.iloc[:-n] if len(sorted_g) > n else sorted_g.iloc[0:0]

    return (
        df.groupby("ticker", group_keys=False)
          .apply(_drop)
          .reset_index(drop=True)
    )


# ── Step 5：寫出本地 Parquet ──────────────────────────────────────────────────
def save_parquet(df: pd.DataFrame, path: Path) -> None:
    """排序後寫出 Parquet 至本地路徑。"""
    df = df.sort_values(["ticker", "Date"]).reset_index(drop=True)
    df.to_parquet(path, index=False)
    logger.info(f"✓ 寫出完成：{path}（共 {len(df):,} 筆）")


# ── Step 6：上傳至 MinIO ──────────────────────────────────────────────────────
def upload_to_minio(local_path: Path, bucket: str, key: str) -> None:
    """將本地 Parquet 回寫至 MinIO。"""
    try:
        with open(local_path, "rb") as f:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f)
        logger.info(f"✓ 已上傳至 MinIO：{key}")
    except ClientError as e:
        logger.error(f"✗ 上傳 MinIO 失敗：{e}")
        raise


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # 1. 讀取今日新資料
    buffers = get_today_parquet_buffers(bucket=MINIO_BUCKET, prefix=GOLD_PREFIX)
    if not buffers:
        logger.info("今日無新 Parquet，結束。")
        return

    # 2. 合併成 DataFrame
    df_new       = merge_buffers_to_df(buffers)
    tickers_today = df_new["ticker"].unique().tolist()
    logger.info(f"今日 tickers（共 {len(tickers_today)} 支）：{tickers_today}")

    # 3. 讀取既有目標 Parquet
    df_existing = load_existing_parquet(LOCAL_PARQUET_PATH)

    # 4. 合併：存在 → 去舊 2 筆再 append；不存在 → 直接用新資料
    if df_existing is not None:
        df_trimmed = drop_latest_n_by_ticker(df_existing, tickers_today, n=2)
        df_final   = pd.concat([df_trimmed, df_new], ignore_index=True)
    else:
        df_final = df_new

    # 5. 寫出本地
    save_parquet(df_final, LOCAL_PARQUET_PATH)

    # 6. 回寫 MinIO
    upload_to_minio(LOCAL_PARQUET_PATH, MINIO_BUCKET, LOCAL_PARQUET_MINIO_KEY)


if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()