import pandas as pd

import sys

import time

from ..utils.helpers import countdown

from ..config.db_conn import engine

from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

def get_screened_list(engine):

    # PostgreSQL 篩選要求 company
    query = """
        SELECT DISTINCT ON ("股票代碼")  --DISTINCT ON 的欄位必須在 ORDER BY 的第一個位置
            "紀錄日期", "股票代碼", "is_selected"
        FROM "US"."company_screened"
        WHERE is_selected <> 'no_selected' -- 排除掉不符合選股的股票
            AND "紀錄日期" >= CURRENT_DATE - INTERVAL '2 years'  -- 只抓距今 2 年內的資料
        ORDER BY "股票代碼", "紀錄日期" DESC; -- 每個股票代碼按日期降序排列，DISTINCT ON 只取"最新"一筆
    """
    # and (ticker='AAPL' or ticker='QQQ' or ticker='XOM')
    
    df_co_list = pd.read_sql(query, engine)
    # 取 "股票代碼" 欄位列表
    tickers = df_co_list["股票代碼"].tolist()

    print(f"總共有:{len(tickers)} 檔")
    print(df_co_list)
    print(tickers)

    return tickers

def create_stock_orders(engine,
                    tickers,
                    schema_name: str = "US",
                    source_table="prices_engine_W",
                    target_table="orders"):
    """

    """
    # ---------------------------
    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{target_table}" (
            "id" SERIAL PRIMARY KEY,
            "ticker" VARCHAR(20) NOT NULL,
            "Signal_Date" DATE NOT NULL,
            "Status" VARCHAR(10) ,
            "Side_1" TEXT NOT NULL,
            "Note" TEXT,
            UNIQUE("ticker", "Signal_Date", "Side_1") -- 同一股票,同一天,同策略不可重複
    );
    """

    with engine.begin() as conn:
        for idx1, ticker in enumerate(tickers, start=1):
            print(f"========================================")
            print(f"尋找的第 {idx1} 檔,股票:{ticker}")

            conn.execute(text(create_sql))

            insert_sql = f"""
                -- ================= Long =================
                INSERT INTO "{schema_name}"."{target_table}" 
                (
                    "Signal_Date",
                    "ticker",
                    "Side_1"
                )
                SELECT
                    "Date" AS "Signal_Date",
                    "ticker",
                    'Long' AS "Side_1"
                FROM "{schema_name}"."{source_table}"
                WHERE "Side_1" = 'Long'
                AND "ticker" = '{ticker}'
                AND "Date" >= CURRENT_DATE - INTERVAL '30 days'
                    ORDER BY "Date" DESC
                    LIMIT 1

                    ON CONFLICT ("ticker", "Signal_Date", "Side_1")
                    DO NOTHING;
                ;

                -- ================= Short =================
                INSERT INTO "{schema_name}"."{target_table}" 
                (
                    "Signal_Date",
                    "ticker",
                    "Side_1"
                )
                SELECT
                    "Date" AS "Signal_Date",
                    "ticker",
                    'Short' AS "Side_1"
                FROM "{schema_name}"."{source_table}"
                WHERE "Side_1" = 'Short'
                AND "ticker" = '{ticker}'
                AND "Date" >= CURRENT_DATE - INTERVAL '30 days'
                    ORDER BY "Date" DESC
                    LIMIT 1

                    ON CONFLICT ("ticker", "Signal_Date", "Side_1")
                    DO NOTHING;
            """
            conn.execute(text(insert_sql))
    return 

def main():
    tickers = get_screened_list(engine)

    try:
        create_stock_orders(engine,
                    tickers=tickers,
                    schema_name= "US",
                    source_table="prices_engine_W",
                    target_table="orders_W")

        time.sleep(1)

    except Exception as e:
        print("發生錯誤 !", e)

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()