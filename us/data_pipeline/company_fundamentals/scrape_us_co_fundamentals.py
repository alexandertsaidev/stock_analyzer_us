import pandas as pd
import numpy as np

import yfinance as yf

import sys

import time
from datetime import date

import random

from ...utils.helpers import get_time_str
from ...utils.helpers import countdown

from ...config.db_conn import engine
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

ymd = get_time_str("ymd")
ym = get_time_str("ym")

def get_co_list(engine):

    # PostgreSQL 篩選要求 company
    query = """
        SELECT ticker, is_active
        FROM "US"."company_list"
        WHERE is_active = TRUE
        ;
    """
    # and (ticker='AAPL' or ticker='QQQ' or ticker='XOM')
    
    df_co_list = pd.read_sql(query, engine)
    # 取 "ticker" 欄位列表
    tickers = df_co_list["ticker"].tolist()

    print(f"總共有:{len(tickers)} 檔")
    print(df_co_list)
    print(tickers)

    return tickers

def get_StockFund(retries, max_delay):
    
    all_data = []  # 用 list 累積每個 ticker 的資料字典
    
    list = get_co_list(engine)
    print("原始list 長度: ",len(list))
    print("原始list :\n",list)

    chunk_size = 50
    small_lists = [list[i:i + chunk_size] for i in range(0, len(list), chunk_size)]

    # 確認結果
    print(f"總共拆分成 {len(small_lists)} 個小列表 !!!")
    for idx1, sublist in enumerate(small_lists, start=1):
        print(f"第 {idx1} 個小列表長度: {len(sublist)}") 

        for idx2, ticker in enumerate(sublist, start=1):
            attempt = 0
            while attempt < retries:
                try:
                    time.sleep(random.uniform(0.5, 1.5))

                    # 獲取股票數據
                    stock = yf.Ticker(ticker)

                    market = stock.get_info()

                    # 取得當前"股票"價
                    current_price = market.get("currentPrice", None)
                    # 取得歷史"股票"最高價
                    hist_high = stock.history(period="max")["Close"].max()

                    if current_price is not None:
                        # hist_high = market.get("allTimeHigh", None)
                        pass

                    else:
                        # 取得當前"股票"價
                        current_price = stock.history(period="max")["Close"].iloc[-1]

                    # 取得52週"股票"最高價
                    hist_high_52w = market.get("fiftyTwoWeekHigh", None)
                    # 計算 當前/最高價 "股票" 價格比例
                    price_ratio = np.trunc((current_price / hist_high) * 100) / 100
                    # 計算 當前/52週最高價 "股票" 價格比例
                    price_ratio_52w = np.trunc((current_price / hist_high_52w) * 100) / 100
                    
                    data = {
                        "紀錄日期": date.today(),
                        "股票代碼": ticker,
                        "市值": market.get("marketCap", None),
                        "本益比(trailingP/E)": market.get("trailingPE", None),
                        "預期本益比(forwardP/E)": market.get("forwardPE", None),
                        "市銷率P/S": market.get("priceToSalesTrailing12Months", None),
                        "流動比率": market.get("currentRatio", None),
                        "產權比率/負債權益比": market.get("debtToEquity", None),
                        "ROE": market.get("returnOnEquity", None),
                        "ROA(TTM)": market.get("returnOnAssets", None),
                        # "ROA(5Avg)": roa_5years,
                        "EPS(TTM)": market.get("epsTrailingTwelveMonths", None),
                        "EPS增長率": market.get("earningsGrowth", None),
                        # "近5年EPS增長率": EPS_CAGR_5Y ,
                        "EPS預期": market.get("forwardEps", None),
                        "淨利潤": market.get("netIncomeToCommon", None),
                        # "近5年淨利增長率/NetIncomeGrowth5Y": growth_5y,
                        "經營現金流": market.get("operatingCashflow", None),
                        "年度收入增長": market.get("revenueGrowth", None),
                        "年銷售收入": market.get("totalRevenue", None),
                        # "年銷售收入增長率/YoYRevenueGrowth": revenue_growth,
                        # "近5年銷售收入增長率": growth_5years,
                        "目前價格": current_price,
                        "52週價格最低/priceLow52W": market.get("fiftyTwoWeekLow", None),
                        "52週價格最高/priceHigh52W": market.get("fiftyTwoWeekHigh", None),
                        "52週價格變化/priceChange52W": market.get("52WeekChange", None),
                        "歷史新高率": price_ratio,
                        "52週新高率": price_ratio_52w,
                        "上一季日均成交/avgDailyVolumeLastQuarter": market.get("averageDailyVolume3Month", None),
                        "機構持股比例": market.get("heldPercentInstitutions", None),
                        "分析師平均評級/analystRatingMean": market.get("recommendationMean", None),
                        "分析師建議/analystRatingKey": market.get("recommendationKey", None)
                    }

                    print(f"{idx1}-{idx2}.")
                    print(data)
                    all_data.append(data)
                    
                    break  # 成功，跳出重試迴圈

                except Exception as e:
                    attempt += 1
                    print(f"{idx1}-{idx2}. Retry {attempt}/{retries} 失敗: {e}")
                    time.sleep(random.uniform(1, max_delay))

            else:
                print(f"{idx1}-{idx2}. 最終失敗 {ticker}，跳過") 
                
    final_df = pd.DataFrame(all_data)

    # 1.文字缺失值 → None
    # missing_values = ["NaN","nan","None","none","NULL","null",""," "]
    missing_values = ["None","none"]
    final_df.replace(missing_values, None, inplace=True)

    print(f"總共拆分成 {len(small_lists)} 個小列表")

    return final_df

def upsert_stock_fundamentals(df: pd.DataFrame,
                            engine,
                            schema_name: str = "US",
                            table_name: str = "company_fundamentals"):
    """
    將股票價格 + 技術指標資料進行 Bulk Upsert 至 PostgreSQL。
    自動偵測 df 新欄位並新增到表中。

    必要欄位：
        "紀錄日期", 
        "股票代碼"
    """
    # 檢查必要欄位
    required_cols = {"紀錄日期", "股票代碼"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"缺少必要欄位: {required_cols - set(df.columns)}")

    # ---------------------------
    # 1.建表（第一次用）

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
            "紀錄日期" DATE NOT NULL,
            "股票代碼" VARCHAR(20) NOT NULL,
            PRIMARY KEY ("紀錄日期", "股票代碼")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # ---------------------------
    # 2.(初次)反射資料表結構

    metadata = MetaData(schema=schema_name)
    table = Table(table_name, metadata, autoload_with=engine)

    db_cols = [c.name for c in table.columns]

    # ---------------------------
    # 3.偵測 df 新欄位，ALTER TABLE 新增

    new_cols = [c for c in df.columns if c not in db_cols]
    for col in new_cols:
        # 特定欄位 強制 TEXT
        if col == "分析師建議/analystRatingKey":
            col_type = "TEXT"
        else:
            col_type = "NUMERIC"
            # 使用 Pandas Nullable Float
            # float64 → 缺失值是 np.nan
            # "Float64" → 缺失值是 pd.NA
            # pd.NA 上傳 PostgreSQL → 直接變 NULL

        # ALTER TABLE 新增欄位
        alter_sql = f"""
            ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN "{col}" {col_type};
        """

        with engine.begin() as conn:
            conn.execute(text(alter_sql))

        print(f"新增欄位: {col} ({col_type})")

    # 4. 若有新欄位，二次反射確保 table 物件包含最新結構
    if new_cols:
        metadata = MetaData(schema=schema_name)
        table = Table(table_name, metadata, autoload_with=engine)

    # ---------------------------
    # 5.建立 Insert 語句

    # 預先處理 df 數值欄位, 排除指定欄位
    exclude_cols = {"分析師建議/analystRatingKey"}
    num_cols = [c for c in df.select_dtypes(include="number").columns if c not in exclude_cols]
    
    # apply() 可以「批量操作整欄or整列」
    df[num_cols] = (
        df[num_cols]
        .apply(pd.to_numeric, errors="coerce")  # pd.to_numeric() 只能處理一維(Series),每欄轉成數值,不能轉的變 NaN
        .apply(lambda x: np.trunc(x * 1000) / 1000)  # 截斷到小數點3位
        .astype("Float64")  # 轉成 Nullable Float
    )

    # 預先處理 df 數值欄位, 將所有 NaN 換成 Python None（SQL 只認識 NULL）
    records = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
    
    # ---------------------------
    # 6. Upsert ,若和舊的主鍵衝突(重複)時，不覆寫舊資料
    upsert_stmt = insert(table).values(records).on_conflict_do_nothing(
        index_elements=["紀錄日期", "股票代碼"]
    )

    # ---------------------------
    # 7.執行連線 輸入資料庫
    with engine.begin() as conn:
        conn.execute(upsert_stmt)

    print(f"Upsert 完成，共 {len(df)} 筆資料")

def main():
    try:
        data = get_StockFund(retries=3, max_delay=5)
        upsert_stock_fundamentals(
            df = data,
            engine=engine
        )
        time.sleep(2)
        print(data)

    except Exception as e:
        print("發生錯誤 !", e)
        return None
    
    return data

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()