import io
import sys

import random
import logging

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from fake_useragent import UserAgent
from botocore.exceptions import ClientError

from ...config.minio_conn import s3_client, MINIO_BUCKET

from ...utils.helpers import countdown

logger = logging.getLogger(__name__)

# 爬蟲
def scrape_US_tickers(
    url="https://www.sec.gov/files/company_tickers.json",
    retries=5,
    sleep_range=(1, 10)
    ):

    attempt = 0
    data_json = None

    while attempt < retries:
        try:
            ua = UserAgent()
            suffix = random.randint(1111, 9999)
            headers = {
                "User-Agent": f"{ua.random} (contact: my_name{suffix}@gmail.com)"
            }

            logger.info(f"Try {attempt + 1} | User-Agent: {headers['User-Agent']}")
            res = requests.get(url, headers=headers, timeout=15)
            data_json = res.json()
            break

        except Exception as e:
            attempt += 1
            logger.warning(f"Failed: {e} | Retry {attempt}/{retries}")
            time.sleep(random.uniform(*sleep_range))

    return data_json


def _load_existing(
    bucket: str,
    object_name: str
    ) -> pd.DataFrame | None:
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_name)
        buf = io.BytesIO(response["Body"].read())
        df = pd.read_parquet(buf, engine="pyarrow")
        logger.info(f"載入現有資料：{len(df)} 筆")
        return df
    
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            logger.info("尚無現有 Parquet，將建立新檔")
            return None
        raise


def _upsert_df(
    existing: pd.DataFrame | None,
    new: pd.DataFrame,
    batch_timestamp: datetime
    ) -> pd.DataFrame:

    if existing is None:
        # 第一次，全部為 active
        new["is_active"] = True
        return new

    # 以 (cik, ticker) 為 key merge
    merged = existing.set_index(["cik", "ticker"])
    incoming = new.set_index(["cik", "ticker"])

    # 更新現有 / 插入新增
    merged.update(incoming)                          # 覆蓋 title、last_updated
    truly_new = incoming[~incoming.index.isin(merged.index)]
    merged = pd.concat([merged, truly_new])

    # 本批次沒出現的 → inactive
    merged["is_active"] = merged["last_updated"] >= batch_timestamp

    return merged.reset_index()


def _save_parquet(
    df: pd.DataFrame, 
    bucket: str,
    object_name: str
    ):

    try:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
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
        logger.info(f"已存入 {bucket}/{object_name}（共 {len(df)} 筆）")
        return True

    except Exception as e:
        logger.error(f"MinIO 上傳失敗: {e}", exc_info=True)
        return False


def upsert_co_list(
    json_data,
    bucket: str,
    object_name: str
    ):

    if not json_data:
        logger.warning("json_data 為空，略過")
        return

    tz = "America/New_York"
    batch_timestamp = datetime.now(ZoneInfo(tz)).replace(tzinfo=None)

    new_df = pd.DataFrame([
        {
            "cik":          row["cik_str"],
            "ticker":       row["ticker"],
            "title":        row["title"],
            "last_updated": batch_timestamp,
            "is_active":    True,
        }
        for row in json_data.values()
    ])

    existing_df = _load_existing(
        bucket,
        object_name
    )

    final_df    = _upsert_df(
        existing_df, 
        new_df, 
        batch_timestamp
    )

    _save_parquet(
        final_df, 
        bucket,
        object_name
    )


def main():
    
    object_name = f"stock/company_list/us_tickers_list.parquet"
    data_json = scrape_US_tickers()
    # data_json ={
    #     "0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"},
    #     "1":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
    #     "2":{"cik_str":1652044,"ticker":"GOOGL","title":"Alphabet Inc."},
    #     "3":{"cik_str":789019,"ticker":"MSFT","title":"MICROSOFT CORP"},
    #     "4":{"cik_str":1018724,"ticker":"AMZN","title":"AMAZON COM INC"},
    #     "5":{"cik_str":1730168,"ticker":"AVGO","title":"Broadcom Inc."},
    #     "6":{"cik_str":1326801,"ticker":"META","title":"Meta Platforms, Inc."},
    #     "7":{"cik_str":1318605,"ticker":"TSLA","title":"Tesla, Inc."},
    #     "8":{"cik_str":1067983,"ticker":"BRK-B","title":"BERKSHIRE HATHAWAY INC"},
    #     "9":{"cik_str":104169,"ticker":"WMT","title":"Walmart Inc."}
    # }
    upsert_co_list(
        json_data = data_json, 
        bucket = MINIO_BUCKET,
        object_name = object_name
    )

if __name__ == "__main__":
    main()
    countdown(10)

sys.exit()