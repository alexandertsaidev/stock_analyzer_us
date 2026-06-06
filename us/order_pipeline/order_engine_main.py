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
                    source_table="prices_engine_D",
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
            "Entry_Date" TIMESTAMP ,
            "Exit_Date" TIMESTAMP ,
            "Status" VARCHAR(10) ,
            "Holding_days" INTEGER ,
            "Side_1" TEXT NOT NULL,
            "Entry_Price" NUMERIC NOT NULL,
            "Exit_Price" NUMERIC,
            "Initial_Stop" NUMERIC NOT NULL,  -- 初始停損價：開倉時設定的固定停損價格，用於限制最大虧損
            "BreakEven"    NUMERIC ,  -- 保本價：當價格達一定獲利後，將停損移至進場成本價以鎖定零虧損
            "Trailing_Stop" NUMERIC ,  -- 移動停損價：隨價格上漲動態調整的停損價格，用於鎖定浮動獲利
            "Highest_Price" NUMERIC ,
            "Lowest_Price" NUMERIC ,
            "Entry_upperband" NUMERIC ,
            "Entry_lowerband" NUMERIC ,
            "Exit_high" NUMERIC ,
            "Exit_low" NUMERIC ,
            "Entry_score" NUMERIC(10,2), -- 入場分= (Entry_upperband - Entry_Price) / (Entry_upperband - Entry_lowerband)
            "Exit_score" NUMERIC(10,2) , -- 離場分= (Exit_Price - Exit_low) / (Exit_high - Exit_low)
            "Final_score" NUMERIC(10,2),  -- 交易總分= 損益(比) / (Entry_upperband - Entry_lowerband)
            "Volume" INTEGER,
            "Return" NUMERIC, -- 損益(比)= Exit_Price - Entry_Price
            "Return_pct" NUMERIC,
            "Commission" NUMERIC ,
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
                    "Entry_Price",
                    "Initial_Stop",
                    "Highest_Price",
                    "Lowest_Price",
                    "Entry_upperband",
                    "Entry_lowerband",
                    "Side_1"
                )
                SELECT
                    "Date" AS "Signal_Date",
                    "ticker",
                    "High" * 1.005 AS "Entry_Price",    -- Long 入場價
                    "High" * 0.9 AS "Initial_Stop",     -- Long 初始止損
                    "High" AS "Highest_Price",  -- Long 初始最高價
                    NULL AS "Lowest_Price",             -- 空單欄位不使用
                    "upperband" AS "Entry_upperband",
                    "lowerband" AS "Entry_lowerband",
                    'Long' AS "Side_1"
                FROM "{schema_name}"."{source_table}"
                WHERE "Side_1" = 'Long'
                AND "ticker" = '{ticker}'
                AND "Date" >= CURRENT_DATE - INTERVAL '14 days'
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
                    "Entry_Price",
                    "Initial_Stop",
                    "Highest_Price",
                    "Lowest_Price",
                    "Entry_upperband",
                    "Entry_lowerband",
                    "Side_1"
                )
                SELECT
                    "Date" AS "Signal_Date",
                    "ticker",
                    "Low" * 0.995 AS "Entry_Price",     -- Short 入場價
                    "Low" * 1.1 AS "Initial_Stop",      -- Short 初始止損
                    NULL AS "Highest_Price",             -- 多單欄位不使用
                    "Low" AS "Lowest_Price",     -- Short 初始最低價
                    "upperband" AS "Entry_upperband",
                    "lowerband" AS "Entry_lowerband",
                    'Short' AS "Side_1"
                FROM "{schema_name}"."{source_table}"
                WHERE "Side_1" = 'Short'
                AND "ticker" = '{ticker}'
                AND "Date" >= CURRENT_DATE - INTERVAL '14 days'
                    ORDER BY "Date" DESC
                    LIMIT 1

                    ON CONFLICT ("ticker", "Signal_Date", "Side_1")
                    DO NOTHING;
            """
            conn.execute(text(insert_sql))
    return 

def update_exit_orders(engine,
                       schema_name: str = "US",
                       source_table: str = "prices_engine_D",
                       target_table: str = "orders"):
    """
    功能：
        自動更新尚未平倉訂單的 Exit_Date、Exit_Price、Status。

    邏輯：
         1. 只處理已進場 (Entry_Date 有值)
         2. 只處理尚未平倉 (Exit_Date 為 NULL)
         3. Long 條件  → High >= upperband
         4. Short 條件 → Low <= lowerband
         5. 價格來自 prices_engine_D 當日資料
    """

    update_sql = f"""
        /* 
        更新訂單表 (orders)
        使用價格表 (prices_engine_D) 做 JOIN 比對
        符合平倉條件的訂單會被更新
        */

        UPDATE "{schema_name}"."{target_table}" o

        /* 設定要更新的欄位 */
        SET "Exit_Date"  = CURRENT_DATE - INTERVAL '1 days',   -- 平倉日期設為今天
            "Status"     = 'closed' ,       -- 訂單狀態改為 closed
            "Holding_days" = (Date(o."Exit_Date") - Date(o."Signal_Date")) ,  -- 整數天數integer
            "Exit_Price" = p."Close",      -- 平倉價格設為當日收盤價
            "Exit_high"  = p."High" ,
            "Exit_low"   = p."Low" ,
            "Entry_score" =
                CASE
                    -- 當上下通道寬度為 0 時，無法計算分數 → 設為 NULL
                    WHEN o."Entry_upperband" = o."Entry_lowerband" THEN NULL
                    ELSE 
                        ROUND(
                            (
                                -- 分子：上軌 - 實際入場價
                                -- 越接近下軌 (Entry_lowerband) 分數越高
                                (o."Entry_upperband" - o."Entry_Price")
                                /
                                -- 分母：通道寬度，NULLIF 避免除以 0
                                NULLIF((o."Entry_upperband" - o."Entry_lowerband"), 0)
                            ) * 100  -- 將比值轉成百分制 (0~100)
                        )  -- 四捨五入到整數
                END,
            "Exit_score" =
                CASE
                    WHEN o."Exit_high" = o."Exit_low" THEN NULL
                    ELSE 
                        ROUND(
                            (
                                (o."Exit_Price" - o."Exit_low")
                                /
                                NULLIF((o."Exit_high" - o."Exit_low"), 0)
                            ) * 100
                        )  -- 四捨五入到整數
                END,
            "Final_score" =
                CASE
                    -- 當 Entry 的上下通道寬度為 0 時，無法計算交易效率 → 設為 NULL
                    WHEN o."Entry_upperband" = o."Entry_lowerband" THEN NULL
                    ELSE 
                        ROUND(
                            (
                                -- 分子：實際獲利 (Exit_Price - Entry_Price)
                                -- 越大代表交易效率越高
                                (o."Exit_Price" - o."Entry_Price")
                                /
                                -- 分母：Entry 通道寬度，NULLIF 避免除以 0
                                NULLIF((o."Entry_upperband" - o."Entry_lowerband"), 0)
                            ) * 100  -- 將比值轉成百分制 (可正可負)
                        )  -- 四捨五入到整數
                END ,
            "Return" =   -- 計算損益
                CASE
                    WHEN o."Side_1" = 'Long'
                        THEN (o."Exit_Price" - o."Entry_Price") / o."Entry_Price"
                    WHEN o."Side_1" = 'Short'
                        THEN (o."Entry_Price" - o."Exit_Price") / o."Entry_Price"
                END ,
            "Return_pct" = ROUND(   -- 計算報酬率
                CASE 
                    WHEN o."Side_1" = 'Long'
                        THEN (o."Exit_Price" - o."Entry_Price") / o."Entry_Price"
                    WHEN o."Side_1" = 'Short'
                        THEN (o."Entry_Price" - o."Exit_Price") / o."Entry_Price"
                END
            , 4)
            
        /* 從價格表抓取資料 */
        FROM "{schema_name}"."{source_table}" p

        WHERE
            -- 已進場
            o."Entry_Date" IS NOT NULL

            -- 尚未平倉
            AND o."Exit_Date" IS NULL

            -- 用 ticker 對應訂單與價格
            AND o."ticker" = p."ticker"

            -- 只使用今天的價格
            AND p."Date" = CURRENT_DATE - INTERVAL '1 days'

            -- 平倉條件
            AND (
                -- 多單平倉條件
                    (
                        o."Side_1" = 'Long'
                        AND (
                                p."High" >= p."upperband"     -- 到達獲利目標
                            OR p."Close" <= o."Initial_Stop"     -- 跌破止損
                            OR p."Low" <= GREATEST(o."BreakEven", o."Trailing_Stop") -- 選「BreakEven 與 Trailing Stop」中較高的價格作為保護線
                            )
                    )
                -- 空單平倉條件
                OR
                    (
                        o."Side_1" = 'Short'
                        AND (
                                p."Low"  <= p."lowerband"     -- 到達獲利目標
                            OR p."Close" >= o."Initial_Stop"     -- 突破止損
                            OR p."High" >= LEAST(o."BreakEven", o."Trailing_Stop") -- 選「BreakEven 與 Trailing Stop」中較低的價格作為保護線
                        )
                    )
            )
        ;
    """

    with engine.begin() as conn:
        conn.execute(text(update_sql))

def update_risk_orders(
        engine,
        schema_name: str = "US",
        source_table: str = "prices_engine_D",
        target_table: str = "orders",
        trailing_percent=0.05,   # 5% trailing
        breakeven_percent=0.02   # 2% 保本觸發
    ):
    """
    更新持倉風控：
    1. 更新 Highest_Price / Lowest_Price
    2. 更新 Trailing_Stop
    3. 更新 BreakEven
    """

    update_sql = f"""
        UPDATE "{schema_name}"."{target_table}" o
        SET

            -- 更新最高價 多單 (Long)
            "Highest_Price" = 
            CASE
                WHEN o."Side_1" = 'Long'
                THEN GREATEST(o."Highest_Price", p."High")
                ELSE o."Highest_Price"
            END,

            -- 更新最低價 空單 (Short)
            "Lowest_Price" = 
            CASE
                WHEN o."Side_1" = 'Short'
                THEN LEAST(o."Lowest_Price", p."Low")
                ELSE o."Lowest_Price"
            END,

            -- 更新 Trailing Stop 多單(Long)、空單(Short)
            "Trailing_Stop" = 
            CASE
                WHEN o."Side_1" = 'Long'
                    THEN GREATEST(
                            COALESCE(o."Trailing_Stop", 0),
                            GREATEST(o."Highest_Price", p."High") * (1 - {trailing_percent})
                        )
                WHEN o."Side_1" = 'Short'
                    THEN LEAST(
                            COALESCE(o."Trailing_Stop", 999999999),
                            LEAST(o."Lowest_Price", p."Low") * (1 + {trailing_percent})
                        )
                ELSE o."Trailing_Stop"
            END,

            -- 更新 BreakEven 多單(Long)、空單(Short)
            "BreakEven" = 
            CASE
                WHEN o."Side_1" = 'Long'
                    AND (p."Close" - o."Entry_Price") / o."Entry_Price" >= {breakeven_percent}
                    THEN o."Entry_Price"
                WHEN o."Side_1" = 'Short' 
                    AND (o."Entry_Price" - p."Close") / o."Entry_Price" >= {breakeven_percent}
                    THEN o."Entry_Price"
                ELSE o."BreakEven"
            END

        FROM "{schema_name}"."{source_table}" p
        WHERE
            o."Entry_Date" IS NOT NULL
            AND o."Exit_Date" IS NULL
            AND o."Status" <> 'closed'
            AND o."ticker" = p."ticker"
            AND p."Date" = CURRENT_DATE - INTERVAL '1 days'
        ;
    """
    with engine.begin() as conn:
        conn.execute(text(update_sql))

    return

def main():
    tickers = get_screened_list(engine)

    try:
        create_stock_orders(engine,
                    tickers=tickers,
                    schema_name= "US",
                    source_table="prices_engine_D",
                    target_table="orders")

        time.sleep(1)

        update_risk_orders(
                engine,
                schema_name= "US",
                source_table= "prices_engine_D",
                target_table= "orders",
                trailing_percent=0.05,   # 5% trailing
                breakeven_percent=0.02   # 2% 保本觸發
            )
        time.sleep(1)

        update_exit_orders(engine,
                       schema_name= "US",
                       source_table= "prices_engine_D",
                       target_table= "orders")

    except Exception as e:
        print("發生錯誤 !", e)

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()