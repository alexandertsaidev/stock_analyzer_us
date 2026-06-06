import time
from datetime import datetime

import random
import requests
from fake_useragent import UserAgent

import sys

from ...utils.helpers import countdown

from ...config.db_conn import engine
from ...config.db_conn import get_conn

from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

def scrape_US_tickers(
        url = "https://www.sec.gov/files/company_tickers.json",
        retries = 5,
        sleep_range = (1, 10)
    ):

    # 嘗試重試機制
    attempt = 0
    while attempt < retries:
        try:
            ua = UserAgent()
            # 隨機產生 email 後綴，增加 User-Agent 變化
            suffix = random.randint(1111, 9999)
            headers = {
                "User-Agent": f"{ua.random} (contact: my_name{suffix}@gmail.com)"
            }

            print(f"Try {attempt+1}")
            print(f"User-Agent : {headers['User-Agent']}")

            # 下載 JSON，timeout:等待時間, 防止無限等待
            res = requests.get(url, headers=headers, timeout=15)

            # 解析 JSON
            data_json = res.json()
            print(data_json)

            break

        except Exception as e:
            attempt += 1

            print(f"Failed: {e}")
            print(f"Retry {attempt}/{retries}")

            time.sleep(random.uniform(*sleep_range))

    if data_json:
        return data_json
    else:
        return None
    
def upsert_co_list(json_data,
                    engine,
                    schema_name: str = "US",
                    table_name="company_list"):


    batch_timestamp = datetime.now()
    
    if not json_data :
        return
    
    # ---------------------------
    # 1.建表（第一次用）

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                cik BIGINT NOT NULL,
                ticker VARCHAR(20),
                title TEXT,
                last_updated TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                UNIQUE(cik, ticker)  -- 複合唯一鍵
            );
    """

    # ---------------------------
    # 2.更新每次 JSON 傳送的資料

    upsert_sql = f"""
        INSERT INTO "{schema_name}"."{table_name}"(
            "cik","ticker","title","last_updated","is_active"
        )
        VALUES (
            :cik,
            :ticker,
            :title,
            :last_updated,
            TRUE
        )
        ON CONFLICT ("cik", "ticker")
        DO UPDATE SET
            "title" = EXCLUDED.title,  --新傳入、衝突的那筆資料
            "last_updated" = EXCLUDED.last_updated, --新傳入、衝突的那筆資料
            "is_active" = TRUE;
        
    """
    # ---------------------------
    # 3. inactive 更新

    inactive_sql = f"""
        UPDATE "{schema_name}"."{table_name}"
        SET "is_active" = FALSE
            WHERE "last_updated" < :batch_timestamp;
    """

    # ---------------------------
    # 4. list[(dict)] 資料, (批次)更新
    
    data_list = [
        {
            "cik": row["cik_str"],
            "ticker": row["ticker"],
            "title": row["title"],
            "last_updated": batch_timestamp
        }
        for row in json_data.values()
    ]

    # ---------------------------
    # 5 .執行連線 輸入資料庫

    with engine.begin() as conn:

        conn.execute(text(create_sql))
        conn.execute(text(upsert_sql), data_list)
        conn.execute(
            text(inactive_sql),  # 將 SQL 字串包裝成 SQLAlchemy 可執行物件
            {"batch_timestamp": batch_timestamp}  # 綁定參數：把 SQL 中的 :batch_timestamp 替換成 Python 的 batch_timestamp
        )
    
    return

def main():

    data_json = scrape_US_tickers()
    # data_json ={"0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA CORP"},
    #             "1":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},
    #             "2":{"cik_str":1652044,"ticker":"GOOGL","title":"Alphabet Inc."},
    #             "3":{"cik_str":789019,"ticker":"MSFT","title":"MICROSOFT CORP"},
    #             # "4":{"cik_str":1018724,"ticker":"AMZN","title":"AMAZON COM INC"},
    #             # "5":{"cik_str":1730168,"ticker":"AVGO","title":"Broadcom Inc."},
    #             "6":{"cik_str":1326801,"ticker":"META","title":"Meta Platforms, Inc."},
    #             "7":{"cik_str":1318605,"ticker":"TSLA","title":"Tesla, Inc."},
    #             "8":{"cik_str":1067983,"ticker":"BRK-B","title":"BERKSHIRE HATHAWAY INC"},
    #             "9":{"cik_str":104169,"ticker":"WMT","title":"Walmart Inc."}
    #         }

    upsert_co_list(json_data = data_json ,
                    engine = engine ,
                    schema_name = "US",
                    table_name = "company_list")

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()
