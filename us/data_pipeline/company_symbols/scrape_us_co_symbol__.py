import time
import random
import requests
import sys

import psycopg2
from psycopg2 import sql

from ...utils.helpers import get_time_str
from ...utils.helpers import countdown
from ...utils.helpers import getScriptLevelPath

from ...config.db_conn import get_conn

ymd = get_time_str("ymd")
ym = get_time_str("ym")

def scrape_US_tickers(
        url = "https://www.sec.gov/files/company_tickers.json",
        retries=3,
        sleep_range=(1, 3)
    ):
    """
    爬取 SEC 官方 company_tickers.json，轉存 CSV 並回傳 DataFrame

    Parameters
    ----------
    url : str
        爬取 SEC 官方網址 (預設 "https://www.sec.gov/files/company_tickers.json")
    retries : int
        若請求失敗，最大重試次數
    sleep_range : tuple
        每次重試前隨機等待秒數範圍 (秒)

    Returns
    -------
    pd.DataFrame
        轉換後的公司 ticker DataFrame
    """

    # 可隨機選擇的瀏覽器 User-Agent 列表
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    # 嘗試重試機制
    for i in range(retries):
        try:
            # 隨機產生 email 後綴，增加 User-Agent 變化
            suffix = random.randint(1000, 9999)
            headers = {
                "User-Agent": f"{random.choice(user_agents)} (contact: your_email+{suffix}@example.com)"
            }

            print(f"Try {i+1} | UA: {headers['User-Agent']}")

            # 下載 JSON，timeout 防止無限等待
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()  # 若下載失敗會拋出例外

            # 解析 JSON
            data_json = res.json()
            print(data_json)

        except Exception as e:
            print(f"Failed: {e}")
            # 若未到最後一次重試，隨機等待再重試
            if i < retries :
                sleep_time = random.uniform(*sleep_range)
                print(f"Retry after {sleep_time:.2f}s...\n")
                time.sleep(sleep_time)
            else:
                # 最後一次失敗就拋出例外
                raise

    return data_json

def upsert_co_list(json_data, 
                      conn, 
                      schema_name="US", 
                      table_name="comapany_list"):
    """
    將 JSON 資料插入 PostgreSQL，支援 UPSERT 與自動標記 inactive

    Parameters
    ----------
    json_data : dict
        JSON 格式資料，每個 value 是 dict 包含 cik_str, ticker, title
    conn : psycopg2 connection
        已建立的 PostgreSQL 連線
    schema_name : str
        schema 名稱 (預設 "US")
    table_name : str
        table 名稱 (預設 "US_co_list")
    """
    try:
        cursor = conn.cursor()

        # 1.建立 schema
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))

        # 2.建立 table
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id SERIAL PRIMARY KEY,
                cik BIGINT NOT NULL,
                ticker VARCHAR(20),
                title TEXT,
                last_updated TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                UNIQUE(cik, ticker)  -- 允許 相同 cik 對應不同 ticker
            );
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        cursor.execute(create_table_query)
        conn.commit()

        # 3.UPSERT JSON 資料
        upsert_query = sql.SQL("""
            INSERT INTO {}.{} (cik, ticker, title, last_updated, is_active)
            VALUES (%s, %s, %s, NOW(), TRUE)
            ON CONFLICT (cik,ticker)  -- 同一 cik 可以對應多個 ticker
            DO UPDATE SET
                ticker = EXCLUDED.ticker,
                title = EXCLUDED.title,
                last_updated = EXCLUDED.last_updated,
                is_active = TRUE;
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name))

        for row in json_data.values():
            cursor.execute(upsert_query, (
                row["cik_str"],
                row["ticker"],
                row["title"]
            ))

        # 4.標記"當下"沒抓到的公司為 inactive
        # 過濾"最後更新日期時間"比"執行當下日期時間早" 的資料
        mark_inactive_query = sql.SQL("""
            UPDATE {}.{}
            SET is_active = FALSE
            WHERE last_updated < NOW() ;
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        cursor.execute(mark_inactive_query)

        # 5.提交並關閉
        conn.commit()
        cursor.close()

        print(f"JSON inserted into {schema_name}.{table_name}")

    except (psycopg2.Error) as e:
        print(f"連線 or 設定錯誤: {e}")
        return None
    except Exception as e:
        print("其他錯誤:", e)
        return None
    
    return

def main():

    conn = get_conn(config_file=f"{getScriptLevelPath(2)}\\config\\config.ini", 
                    section="postgres")
    data_json = scrape_US_tickers()
    print(data_json)
    upsert_co_list(data_json, conn, schema_name="US", table_name="company_list")


if __name__ == "__main__":
    main()
    countdown(5)

# 強制關閉程序
sys.exit()
