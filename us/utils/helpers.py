import time
from datetime import datetime , timedelta

from pathlib import Path

import sys
import os

import duckdb

import pyarrow as pa
import pyarrow.parquet as pq

import logging
from botocore.exceptions import ClientError

import fnmatch



def countdown(seconds):
    for i in range(seconds, 0, -1):
        print("Close in "+ str(i) +" seconds..." , end='\r')
        time.sleep(1)

def createFolder(goal_dir):
    if not os.path.exists(goal_dir):
        os.makedirs(goal_dir)

def judgePackEnv():
    if getattr(sys, 'frozen', False):  # 檢查是否為打包後的執行環境
        # abs_path = os.path.dirname(sys.executable)  # 取得 .exe 所在 "目錄"
        abs_path = sys.executable  # 取得 .exe檔案 所在 完整路徑
    else:
        abs_path = os.path.abspath(__file__) # 取得目前執行 .py檔案 的絕對路徑
    return abs_path

def getScriptLevelPath(levels):
    """
    取得目前執行的 Python 檔案所在資料夾，並回溯指定層數。
    :param levels: 要回溯的層數，預設為 1（取得執行檔案的上層資料夾）
    :return: 指定層數的資料夾絕對路徑
    """
    abs_path = judgePackEnv()  # 取得目前 環境執行檔案的絕對路徑
    folder_path = abs_path  # 初始為檔案路徑

    for _ in range(levels):
        folder_path = os.path.dirname(folder_path)  # 每次回溯一層

    return folder_path  # 回傳對應層級的資料夾路徑

def find_recent_files(folder, pattern, top_n=None, days=None):
    """
    遞迴搜尋資料夾中符合條件的檔案，功能：
        - 檔名通配符篩選 (pattern)
        - 過濾最近 days 天內修改的檔案 (days)
        - 只回傳最新 top_n 個檔案 (top_n)
    
    回傳 dict，key 是檔名，value 是 dict 包含完整路徑與修改時間

    參數:
        folder (str): 搜尋的根目錄
        pattern (str): 檔名通配符，預設 "*.csv"
        top_n (int or None): 只取最新 top_n 個檔案，None 表示不限制
        days (int or None): 過濾最近多少天內修改的檔案，None 表示不限制

    回傳:
        dict: {filename: {'full_path': 完整路徑, 'mtime': 修改時間}}
    """
    matches = []
    now = datetime.now()
    start_time = now - timedelta(days=days) if days is not None else None

    for root, dirs, files in os.walk(folder):
        for filename in files:
            # 檔名通配符篩選
            if not fnmatch.fnmatch(filename, pattern):
                continue

            full_path = os.path.join(root, filename)

            # 取得修改時間
            mtime = datetime.fromtimestamp(os.path.getmtime(full_path))

            # 天數過濾 (可選)
            if start_time and mtime < start_time:
                continue

            # 符合條件就加入 matches
            matches.append({
                'filename': filename,
                'full_path': full_path,
                'mtime': mtime
            })

    # 按修改時間排序，最新在前
    matches.sort(key=lambda x: x['mtime'], reverse=True)

    # 取前 top_n 個 (可選)
    if top_n is not None:
        matches = matches[:top_n]

    # 組成 dict 回傳
    result = {m['filename']: {'full_path': m['full_path'], 'mtime': m['mtime']} for m in matches}

    return result

def get_time_str(mode):
    """
    mode:
        "ymd" -> YYYYMMDD
        "ym"  -> YYYYMM
        "full"-> YYYYMMDD_HHMMSS
    """
    now = datetime.now()

    if mode == "ymd":
        return now.strftime("%Y%m%d")
    elif mode == "ym":
        return now.strftime("%Y%m")
    elif mode == "full":
        return now.strftime("%Y%m%d_%H%M%S")
    else:
        raise ValueError(f"Unknown mode: {mode}")

def get_pa_table(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    object_name: str
    ) -> pa.Table:
    
    logger = logging.getLogger(__name__)

    try:
        pa_table = conn.execute(f"""
            SELECT *
            FROM read_parquet('s3://{bucket}/{object_name}')
        """).to_arrow_table()
        
        logger.info(f"從 {bucket}/{object_name} 取得 {pa_table.num_rows} 檔")
        return pa_table

    except duckdb.HTTPException as e:
        if "404" in str(e):
            logger.warning(f"s3://{bucket}/{object_name} 不存在，視為首次執行")
            raise FileNotFoundError(f"找不到檔案: s3://{bucket}/{object_name}") from e  # 維持原本行為
        else:
            logger.error(f"DuckDB 讀取 S3 失敗 ({bucket}/{object_name}): {e}", exc_info=True)
            raise  # 403 / 其他錯誤 → 讓 Airflow fail

    except ClientError as e:
        # 保留 boto3 ClientError，以防其他地方仍用 s3_client
        logger.error(f"MinIO 讀取失敗 ({bucket}/{object_name}): {e}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"讀取 {bucket}/{object_name} 發生未知錯誤: {e}", exc_info=True)
        raise


# def get_one_parquet_buffer(
#     bucket: str,
#     object_name: str,
#     ) -> dict | None:

#     """
#     從 MinIO 讀取指定 bucket/key 的 .parquet 檔案，回傳含 BytesIO buffer 的 dict。

#     Returns:
#         dict | None，包含：
#             - object_name   (str)     : MinIO 完整物件路徑
#             - last_modified (datetime): 最後修改時間（台灣時區 UTC+8）
#             - size          (int)     : 檔案大小（bytes）
#             - buffer        (BytesIO) : Parquet 內容，指標在位置 0
#         若讀取失敗則回傳 None。
#     """
#     tz_taipei = ZoneInfo("Asia/Taipei")

#     try:
#         response = s3_client.get_object(
#             Bucket=bucket,
#             Key=object_name
#         )
#         buffer = io.BytesIO(response["Body"].read())
#         buffer.seek(0)
#         result = {
#             "object_name":   object_name,
#             "last_modified": response["LastModified"].astimezone(tz_taipei),
#             "size":          response["ContentLength"],
#             "buffer":        buffer,
#         }
#         logger.info(f"✓ 讀取成功：{object_name}")
#         return result

#     except Exception as e:
#         logger.warning(f"✗ 讀取失敗 {object_name}: {e}")
#         return None
