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

final_parquet = "us_all_prices.parquet"   # ← 加副檔名
temp_parquet  = "temp.parquet"            # ← 加副檔名
gold_prefix   = "stock/history/prices/gold/final_all"

def _upload_df_as_parquet(
        conn: duckdb.DuckDBPyConnection,
        bucket: str,
        object_name: str,
        df: pd.DataFrame,
) -> bool:
    """DuckDB 清洗（Date::DATE）→ PyArrow Table → 上傳至 MinIO。"""
    try:
        conn.register("upload_df", df)
        arrow_table = conn.execute("""
            SELECT "Date"::DATE AS "Date", * EXCLUDE ("Date")
            FROM upload_df
        """).to_arrow_table()

        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        buf.seek(0)

        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(
            Bucket=bucket, 
            Key=object_name,
            Body=buf
        )
        logger.info(f"✓ 上傳完成：{bucket}/{object_name}")
        return True

    except Exception as e:
        logger.error(f"✗ 上傳失敗：{object_name} - {e}", exc_info=True)
        return False

def _upload_arrow_as_parquet(
    bucket: str,
    object_name: str,
    arrow_table,                        # 直接接受 PyArrow Table，跳過重複清洗
) -> bool:
    
    """PyArrow Table → Parquet bytes → 上傳至 MinIO。"""
    try:
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf)
        buf.seek(0)

        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(Bucket=bucket, Key=object_name, Body=buf)
        logger.info(f"✓ 上傳完成：{bucket}/{object_name}")
        return True

    except Exception as e:
        logger.error(f"✗ 上傳失敗：{object_name} - {e}", exc_info=True)
        return False
    
def _read_parquet_from_minio(
        bucket: str, 
        object_name: str
) -> pd.DataFrame | None:
    """
    從 MinIO 讀取 parquet，回傳 DataFrame。
    不存在（404）或讀取失敗時回傳 None。
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=object_name)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return None
        logger.error(f"head_object 失敗：{object_name} - {e}", exc_info=True)
        return None

    try:
        res = s3_client.get_object(Bucket=bucket, Key=object_name)
        buf = io.BytesIO(res["Body"].read())
        buf.seek(0)
        return pd.read_parquet(buf)
    
    except Exception as e:
        logger.error(f"讀取 parquet 失敗：{object_name} - {e}", exc_info=True)
        return None

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
        if "final_all" in obj["Key"]:          # ← 排除 final_all 目錄
            continue
        if "conclusion" in obj["Key"]:          # ← 排除 conclusion 目錄
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
        tickers_with_periods: list[pd.DataFrame],
) -> bool:
    """
    流程：
      1. 合併 tickers_with_periods → new_df
      2. 將 new_df 存為 temp.parquet（直接覆寫）
      3. 若 us_all_prices.parquet 不存在 → 以 new_df 直接建立後結束
      4. 若已存在：
           a. DuckDB 讀取 temp.parquet 取得 tickers_literal
           b. DuckDB 刪除 old_df 中對應 tickers 最新 2 筆（Date DESC）
              + merge temp_parquet，去重後一次輸出 PyArrow Table
           c. 覆寫 us_all_prices.parquet

    Returns:
        bool：成功 True，失敗 False
    """
    if not tickers_with_periods:
        logger.warning("tickers_with_periods 為空，略過 upsert")
        return False

    # ── Step 1：合併今日新資料 ────────────────────────────────────────────────
    new_df = pd.concat(tickers_with_periods, ignore_index=True)

    if not all(col in new_df.columns for col in ["Date", "ticker", "period"]):
        logger.error("new_df 缺少 Date, ticker,period (之一)欄位，中止")
        return False

    # ── Step 2：寫入 temp.parquet（覆寫）────────────────────────────────────

    logger.info(f"寫入 {gold_prefix}/{temp_parquet} ...")
    if not _upload_df_as_parquet(
            conn, 
            bucket, 
            f"{gold_prefix}/{temp_parquet}",
            new_df
            ):
        
        return False

    # ── Step 3：us_all_prices.parquet 不存在 → 直接建立 ──────────────────────
    old_df = _read_parquet_from_minio(
        bucket, 
        f"{gold_prefix}/{final_parquet}"
    )

    if old_df is None:
        logger.info(f"{gold_prefix}/{final_parquet} 不存在，直接以 new_df 建立")
        return _upload_df_as_parquet(
            conn, 
            bucket,
            f"{gold_prefix}/{final_parquet}",
            new_df
            )

    # ── Step 4a：DuckDB 讀取 temp.parquet，取得 tickers_literal ─────────────
    try:
        temp_df = _read_parquet_from_minio(
            bucket, 
            f"{gold_prefix}/{temp_parquet}"
        )
        if temp_df is None:
            logger.error(f"讀取 {gold_prefix}/{temp_parquet} 失敗...")
            return False

        conn.register("temp_parquet", temp_df)

        tickers_literal = conn.execute("""
            SELECT string_agg('''' || ticker || '''', ', ')
            FROM (SELECT DISTINCT ticker FROM temp_parquet)
        """).fetchone()[0]

        logger.info(f"讀取 {gold_prefix}/{temp_parquet} 中的 tickers：{tickers_literal}")

    except Exception as e:
        logger.error(f"讀取 {gold_prefix}/{temp_parquet} tickers 失敗 - {e}", exc_info=True)
        return False

    # ── Step 4b + 4c：DuckDB 一次完成刪舊 + merge + 去重 ────────────────────
    try:
        conn.register("old_df", old_df)

        merged_arrow = conn.execute(f"""

            /*
            * 最外層 SELECT：對合併後的全部資料做去重
            *   - DISTINCT ON ("Date", "ticker", "period")
            *     每個 (日期 × ticker × 週期) 組合只保留一筆
            *   - "Date"::DATE 確保輸出型別為 date32
            *   - * EXCLUDE ("Date", _priority) 移除輔助欄 _priority，
            *     不輸出到最終結果
            */
            SELECT DISTINCT ON ("Date", "ticker", "period")
                "Date"::DATE AS "Date",
                * EXCLUDE ("Date", _priority)
            FROM (

                /*
                * 區塊 1：不在更新名單的 ticker → 全數保留
                *   _priority = 1（最高優先）
                *   用途：這部分資料不受本次更新影響，
                *         即使衝突也應優先保留
                */
                SELECT *, 1 AS _priority FROM old_df
                WHERE "ticker" NOT IN ({tickers_literal})

                UNION ALL

                /*
                * 區塊 2：在更新名單的 ticker → 排除最新 2 筆後保留
                *   _priority = 2（次高優先）
                *   用途：保留不受本次新資料影響的歷史舊資料，
                *         若與區塊 1 衝突（理論上不會）仍輸給區塊 1
                *   EXCLUDE (_rn) 移除輔助排序欄，不帶入後續處理
                */
                SELECT * EXCLUDE (_rn), 2 AS _priority FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY "ticker"
                            ORDER BY "Date" DESC
                        ) AS _rn
                    FROM old_df
                    WHERE "ticker" IN ({tickers_literal})
                )
                WHERE _rn > 2

                UNION ALL

                /*
                * 區塊 3：temp.parquet 新資料
                *   _priority = 3（最低優先）
                *   用途：本次抓取的新資料，
                *         遇到舊資料衝突時跳過（DO NOTHING 語義），
                *         只補入舊資料不存在的 (Date, ticker, period) 組合
                */
                SELECT *, 3 AS _priority FROM temp_parquet
            )

            /*
            * QUALIFY：依 _priority 決定衝突時的勝出者
            *   ORDER BY _priority ASC → 數字最小（優先權最高）的那筆排名第一
            *   = 1 → 每組只保留排名第一的資料列
            *
            *   對應 PostgreSQL 語義：
            *     區塊1/2（舊資料）= 先插入者
            *     區塊3（新資料）  = 後插入者
            *     → 等同 ON CONFLICT DO NOTHING（後來者遇衝突跳過）
            */
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "Date", "ticker", "period"
                ORDER BY _priority ASC
            ) = 1

            -- 最終依 ticker → Date 升冪排序
            ORDER BY "ticker", "Date"

        """).to_arrow_table()   # 直接輸出 PyArrow Table，全程不經過 pandas

    except Exception as e:
        logger.error(f"DuckDB merge 失敗 - {e}", exc_info=True)
        return False

    logger.info(f"合併後共 {merged_arrow.num_rows} 筆，寫回 {gold_prefix}/{final_parquet}")

    return _upload_arrow_as_parquet(
        bucket,
        f"{gold_prefix}/{final_parquet}", 
        merged_arrow
        )

def main():

    # 1. 取得今日檔案 buffer 列表
    parquet_files = get_today_parquet_buffers(
        bucket= MINIO_BUCKET,
        prefix= "stock/history/prices/gold/"
    )

    tickers_with_periods = []

    for parquet_file in parquet_files:

        parquet_file["buffer"].seek(0)
        df = pd.read_parquet(parquet_file["buffer"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        print(f"\n處理：{parquet_file['object_name']}")

        tickers_with_periods.append(df)

    # ── upsert 合併寫入 ───────────────────────────────────────────────────────
    if tickers_with_periods:
        conn = duckdb.connect()
        success = upsert_parquet(
            conn=conn,
            bucket=MINIO_BUCKET,
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