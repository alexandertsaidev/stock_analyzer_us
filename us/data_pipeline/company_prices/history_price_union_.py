import io
import sys

import pandas as pd

import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

import duckdb
import pyarrow.parquet as pq

from ...notify.slack_notify import slack_text_notify
from ...config.minio_conn import s3_client, MINIO_BUCKET

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

#  存 Parquet（success）
def _save_parquet(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        parquet_name: str,
        df: pd.DataFrame
    ):

    object_name = f"stock/history/prices/gold/final_all/{parquet_name}.parquet"

    try:

        ### Step 1：DuckDB 清洗

        # DataFrame → 註冊進 DuckDB 作為虛擬資料表
        conn.register("temp_df", df)

        # 執行清洗查詢：
        #   - "Date"::DATE 確保欄位型別為 date32，避免讀回 pandas 時變成 Timestamp
        #   - EXCLUDE ("Date") 保留其餘欄位原始順序
        #   - WHERE 過濾四個價格欄位全為 NULL 的無效資料列
        # 使用 to_arrow_table() 直接取得 PyArrow Table，保留 date32 型別
        cleaned = conn.execute("""
            SELECT
                "Date"::DATE AS "Date",
                *
            EXCLUDE ("Date")
            FROM temp_df
        """).to_arrow_table()

        ### Step 2：PyArrow Table → Parquet bytes

        buf = io.BytesIO()
        pq.write_table(cleaned, buf)
        buf.seek(0)

        ### Step 3：上傳至 MinIO

        try:
            s3_client.head_bucket(Bucket= bucket)
        except ClientError:
            s3_client.create_bucket(Bucket= bucket)

        s3_client.put_object(
            Bucket= bucket,
            Key=object_name,
            Body=buf,
        )
        logger.info(f"{parquet_name}: 已存入 {bucket}/{object_name}")
        return True

    except Exception as e:
        logger.error(f"{parquet_name}: 處理失敗 - {e}", exc_info=True)
        return False

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

def upsert_parquet(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        parquet_name: str,
        tickers_with_periods: list[pd.DataFrame],
    ) -> bool:
    """
    若目標 parquet 不存在 → 直接呼叫 _save_parquet。
    若已存在 → 從 tickers_with_periods 取得 tickers，
               對舊資料以 DuckDB 刪除每個 ticker 最新 2 筆（按 "Date" 倒序），
               再與新資料合併後存回。

    Args:
        conn          : DuckDB 連線
        bucket        : MinIO bucket 名稱
        parquet_name  : 不含副檔名的 parquet 名稱（對應 _save_parquet 規則）
        tickers_with_periods : 今日新資料的 DataFrame 列表

    Returns:
        bool：成功為 True，失敗為 False
    """
    object_name = f"stock/history/prices/gold/final_all/{parquet_name}.parquet"

    # ── 合併今日新資料（統一處理）────────────────────────────────────────────
    if not tickers_with_periods:
        logger.warning("tickers_with_periods 為空，略過 upsert")
        return False

    new_df = pd.concat(tickers_with_periods, ignore_index=True)

    # ── 檢查目標 parquet 是否存在 ─────────────────────────────────────────────
    parquet_exists = True
    try:
        s3_client.head_object(Bucket=bucket, Key=object_name)
    except ClientError as e:
        # 404 → 不存在；其餘錯誤仍視為例外
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            parquet_exists = False
        else:
            logger.error(f"head_object 失敗：{e}", exc_info=True)
            return False

    # ── 不存在 → 直接存入 ────────────────────────────────────────────────────
    if not parquet_exists:
        logger.info(f"{parquet_name}：目標不存在，直接寫入")
        return _save_parquet(conn, bucket, parquet_name, new_df)

    # ── 存在 → 讀取舊資料 ────────────────────────────────────────────────────
    try:
        res = s3_client.get_object(Bucket=bucket, Key=object_name)
        old_buf = io.BytesIO(res["Body"].read())
        old_buf.seek(0)
        old_df = pd.read_parquet(old_buf)
    except Exception as e:
        logger.error(f"{parquet_name}：讀取舊 parquet 失敗 - {e}", exc_info=True)
        return False

    # ── 從 new_df 取得 tickers 列表 ──────────────────────────────────────────
    if "ticker" not in new_df.columns:
        logger.error(f"{parquet_name}：new_df 缺少 'ticker' 欄位")
        return False

    tickers = new_df["ticker"].unique().tolist()
    logger.info(f"{parquet_name}：本次更新 tickers = {tickers}")

    # ── DuckDB：刪除舊資料中對應 tickers 的最新 2 筆（按 "Date" 倒序）────────
    try:
        conn.register("old_df", old_df)

        # QUALIFY + ROW_NUMBER 保留「排除最新2筆後的其餘資料」
        # 即每個 ticker 按 Date 降冪排序，row_num > 2 才留下
        tickers_literal = ", ".join(f"'{t}'" for t in tickers)

        cleaned_old = conn.execute(f"""
            SELECT * FROM old_df
            WHERE "ticker" NOT IN ({tickers_literal})

            UNION ALL

            SELECT * EXCLUDE (_rn) FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY "ticker"
                        ORDER BY "Date" DESC
                    ) AS _rn
                FROM old_df
                WHERE "ticker" IN ({tickers_literal})
            )
            WHERE _rn > 2
        """).df()

        cleaned_old["Date"] = pd.to_datetime(cleaned_old["Date"]).dt.date

    except Exception as e:
        logger.error(f"{parquet_name}：DuckDB 刪除最新2筆失敗 - {e}", exc_info=True)
        return False

    # ── 合併新舊資料並存回 ───────────────────────────────────────────────────
    merged_df = pd.concat([cleaned_old, new_df], ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=["Date", "period", "ticker"], keep="first")
    merged_df = merged_df.sort_values(["ticker", "Date"]).reset_index(drop=True)

    logger.info(
        f"{parquet_name}：舊資料 {len(old_df)} 筆 → 清理後 {len(cleaned_old)} 筆"
        f" + 新資料 {len(new_df)} 筆 = 合併後 {len(merged_df)} 筆"
    )

    return _save_parquet(conn, bucket, parquet_name, merged_df)

def main():

    # 1. 取得今日檔案 buffer 列表
    parquet_files = get_today_parquet_buffers(
        bucket= MINIO_BUCKET,
        prefix= "stock/history/prices/gold/"
    )

    tickers_with_periods = []

    for parequet_file in parquet_files:

        parequet_file["buffer"].seek(0)
        df = pd.read_parquet(parequet_file["buffer"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        print(f"\n處理：{parequet_file['object_name']}")
        
        tickers_with_periods.append(df)

    # ── 新增：upsert 合併寫入 ─────────────────────────────────────────────
    if tickers_with_periods:
        conn = duckdb.connect()
        success = upsert_parquet(
            conn=conn,
            bucket=MINIO_BUCKET,
            parquet_name="us_all_prices",
            tickers_with_periods=tickers_with_periods,
        )
        conn.close()
        logger.info(f"upsert_parquet 結果：{'成功' if success else '失敗'}")

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()