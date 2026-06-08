import io
import sys
from pathlib import Path

import pandas as pd

import logging

from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

import duckdb
import pyarrow.parquet as pq

from ...notify.slack_notify import slack_text_notify
from ...config.minio_conn import s3_client, MINIO_BUCKET

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

def _get_buffer(
    bucket: str,
    object_name: str,
    ) -> io.BytesIO | None:

    try:
        res = s3_client.get_object(
            Bucket=bucket,
            Key=object_name
        )
        buffer = io.BytesIO(res["Body"].read())
        buffer.seek(0)
        return buffer
    
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise

def _save_parquet_to_minio(
    df: pd.DataFrame,
    bucket: str,
    object_name: str,
    ) -> bool:

    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(
            Bucket = bucket,
            Key = object_name,
            Body = buffer,
        )
        logger.info(f"已存入 {bucket}/{object_name}，共 {len(df)} 筆")
        return True

    except Exception as e:
        logger.error(f"MinIO 上傳失敗 - {e}", exc_info=True)
        return False

def _upsert_parquet_to_minio(
    df_new: pd.DataFrame,
    bucket: str,
    temp_object_name: str,
    final_object_name: str,
    ) -> bool:

    """
    1. 新資料存成 temp.parquet
    2. 用 DuckDB SQL upsert 進 final 的 .parquet
       - 重複的 (紀錄日期, ticker) → 新資料覆蓋
       - 新增的 → append
    3. 結果寫回 final 的 .parquet
    """
    try:
        # Step 1：存 temp
        if not _save_parquet_to_minio(df_new, bucket, temp_object_name):
            logger.error("temp 存檔失敗，中止 upsert")
            return False

        # Step 2：從 MinIO 讀出兩個 buffer
        buffer_temp  = _get_buffer(MINIO_BUCKET, temp_object_name)
        buffer_final = _get_buffer(MINIO_BUCKET, final_object_name)

        # Step 3：DuckDB SQL upsert
        conn = duckdb.connect()

        conn.register("temp_data", pq.read_table(buffer_temp))

        if buffer_final:
            conn.register("final_data", pq.read_table(buffer_final))

            df_merged = conn.execute("""
                -- 保留 master 中不衝突的舊資料
                SELECT * FROM final_data
                WHERE ("紀錄日期", "股票代碼") NOT IN (
                    SELECT "紀錄日期", "股票代碼" FROM temp_data
                )
                UNION ALL
                
                -- 全部新資料（覆蓋 + 新增）
                SELECT * FROM temp_data
            """).df()

            logger.info(f"Upsert 完成，合併後共 {len(df_merged)} 筆")
        else:
            # final 不存在，直接以 temp 作為初始 final
            df_merged = conn.execute("SELECT * FROM temp_data").df()
            logger.info(f"本次未找到之前的 us_all_co_screen.parquet ...\n正在初始化 {len(df_merged)} 筆")

        conn.close()

        # Step 4：寫回 final
        return _save_parquet_to_minio(df_merged, bucket, final_object_name)

    except Exception as e:
        logger.error(f"Upsert 失敗 - {e}", exc_info=True)
        return False

# def get_latest_parquet_buffer_this_month(
#         bucket: str,
#         prefix: str,
#     ) -> dict | None:
#     """
#     從 MinIO 列出指定 bucket/prefix 下，本月份的 .parquet 檔案，
#     回傳 last_modified 最新的一筆（含 BytesIO buffer）。

#     Returns:
#         dict | None，包含：
#             - object_name   (str)     : MinIO 完整物件路徑
#             - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
#             - size          (int)     : 檔案大小（bytes）
#             - buffer        (BytesIO) : Parquet 內容，指標在位置 0
#         若本月無符合檔案則回傳 None。
#     """
#     tz_taipei   = ZoneInfo("Asia/Taipei")
#     now         = datetime.now(tz_taipei)
#     this_year   = now.year
#     this_month  = now.month

#     response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
#     objects  = response.get("Contents", [])

#     # 篩選本月份候選物件，依 last_modified 排序取最新
#     candidates = [
#         obj for obj in objects
#         if obj["Key"].endswith(".parquet")
#         and "final_all" not in obj["Key"]
#         and obj["LastModified"].astimezone(tz_taipei).year  == this_year
#         and obj["LastModified"].astimezone(tz_taipei).month == this_month
#     ]

#     if not candidates:
#         logger.info("本月無符合的 .parquet 檔案")
#         return None

#     latest = max(candidates, key=lambda o: o["LastModified"])

#     try:
#         res    = s3_client.get_object(Bucket=bucket, Key=latest["Key"])
#         buffer = io.BytesIO(res["Body"].read())
#         buffer.seek(0)
#         result = {
#             "object_name":   latest["Key"],
#             "last_modified": latest["LastModified"].astimezone(tz_taipei),
#             "size":          latest["Size"],
#             "buffer":        buffer,
#         }
#         logger.info(f"✓ 最新檔案：{latest['Key']}")
#         return result

#     except Exception as e:
#         logger.warning(f"✗ 讀取失敗 {latest['Key']}: {e}")
#         return None

def get_one_parquet_buffer(
    bucket: str,
    object_name: str,
    ) -> dict | None:

    """
    從 MinIO 讀取指定 bucket/key 的 .parquet 檔案，回傳含 BytesIO buffer 的 dict。

    Returns:
        dict | None，包含：
            - object_name   (str)     : MinIO 完整物件路徑
            - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
            - size          (int)     : 檔案大小（bytes）
            - buffer        (BytesIO) : Parquet 內容，指標在位置 0
        若讀取失敗則回傳 None。
    """
    tz_taipei = ZoneInfo("Asia/Taipei")

    try:
        res = s3_client.get_object(
            Bucket=bucket,
            Key=object_name
        )
        buffer = io.BytesIO(res["Body"].read())
        buffer.seek(0)
        result = {
            "object_name":   object_name,
            "last_modified": res["LastModified"].astimezone(tz_taipei),
            "size":          res["ContentLength"],
            "buffer":        buffer,
        }
        logger.info(f"✓ 讀取成功：{object_name}")
        return result

    except Exception as e:
        logger.warning(f"✗ 讀取失敗 {object_name}: {e}")
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

    # 1. 取得指定檔案 buffer 列表
    parquet_file = get_one_parquet_buffer(
        bucket= MINIO_BUCKET,
        object_name= f"stock/fundamentals/us_all_co_fundamentals.parquet"
    )

    #
    if parquet_file:
        conn = duckdb.connect()

        # BytesIO → PyArrow Table → DuckDB
        arrow_table = pq.read_table(parquet_file["buffer"])
        conn.register("us_co_screen", arrow_table)

        df = conn.execute(Path("/app/stock_analyzer_us/us/data_pipeline/company_screening/us_fundamentals_screen.sql").read_text()).df()
        df["紀錄日期"] = pd.to_datetime(df["紀錄日期"]).dt.date

        conn.close()

        _upsert_parquet_to_minio(
            df_new             = df,
            bucket             = MINIO_BUCKET,
            temp_object_name   = "stock/screening/temp_us_co_screen.parquet",
            final_object_name  = "stock/screening/us_all_co_screen.parquet",
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