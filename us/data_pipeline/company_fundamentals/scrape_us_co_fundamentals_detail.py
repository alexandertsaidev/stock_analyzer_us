import pandas as pd
import numpy as np

import yfinance as yf

import sys

import time

from ...utils.helpers import countdown

import time
from datetime import date

from ...config.db_conn import engine

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

    df_co_list = pd.read_sql(query, engine)
    # 取 "股票代碼" 欄位列表
    tickers = df_co_list["股票代碼"].tolist()

    print(f"總共有:{len(tickers)} 檔")
    print(df_co_list)
    print(tickers)

    return tickers

def upsert(df: pd.DataFrame,
            engine,
            schema_name: str,
            table_name: str,
            pk: list):
    """
    通用 upsert：
      1. 空資料檢查
      2. 反射現有 Table 結構，偵測 df 新欄位
      3. 對新欄位自動 ALTER TABLE ADD COLUMN
      4. 二次反射確保結構最新
      5. on_conflict_do_nothing upsert
    """

    # 1. 首次反射，取得現有欄位清單
    metadata = MetaData(schema=schema_name)
    table = Table(table_name, metadata, autoload_with=engine)

    db_cols = {col.name for col in table.columns}

    # 2. 偵測 df 新欄位，ALTER TABLE 新增
    new_cols = [c for c in df.columns if c not in db_cols]
    for col in new_cols:
        if any(k in col for k in ["Date", "date"]):
            col_type = "Date"
        
        elif any(k in col for k in [
            "period", "quarter", "Ownership",
            "Transaction", "description", "Text", "Name", "Holder", "Insider", "URL"
            ]):
            col_type = "TEXT"

        elif col == "Position":
            col_type = "TEXT"

        elif any(k in col for k in [
            # 價格 / 均線
            "Price", "price", "dividendRate",
            # 比率 / 百分比
            "pct", "Percent", "Margins", "Ratio", "Rate", "returnOn", "heldPercent",
            # 估值
            "forwardPE", "trailingPeg", "priceToSales",
            # EPS / 成長
            "EPS", "Eps", "eps", "avg", "low", "high", "growth", "Trend",
            "current", "daysAgo",
            # 財報數值
            "Equity"
            ]):
            col_type = "NUMERIC"
        
        else:
            col_type = "BIGINT"

        alter_sql = f"""
            ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN "{col}" {col_type};
        """
        
        with engine.begin() as conn:
            conn.execute(text(alter_sql))

        print(f"新增欄位: {col} ({col_type})")

    # 3. 若有新欄位，二次反射確保 table 物件包含最新結構
    if new_cols:
        metadata = MetaData(schema=schema_name)
        table = Table(table_name, metadata, autoload_with=engine)

    # 4. 將所有 NaN 換成 Python None（SQL 只認識 NULL）
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    # 5. Upsert
    stmt = insert(table).values(records).on_conflict_do_nothing(index_elements=pk)

    with engine.begin() as conn:
        conn.execute(stmt)

    print(f"Upsert 完成，共 {len(df)} 筆資料")

def clean_col(col: str) -> str:
    """欄位名稱：空白→底線"""

    return col.strip().replace(" ", "_")

# ═══════════════════════════════════════════════════════════
# 1. 財報三表（寬表轉置）
#    income_stmt / balance_sheet / cashflow  (年度 + 季度)
# ═══════════════════════════════════════════════════════════

def upsert_financial(raw_df: pd.DataFrame,
                     engine,
                     ticker: str,
                     schema_name: str,
                     **params):
    
    """
    原始 yfinance 財報 df（index=指標名, columns=日期）
    → 轉置 → upsert

    PK: ticker, record_date, frequency
    """
    df = raw_df.copy()
    df.columns = pd.to_datetime(df.columns)   # 確保欄位是 datetime
    df = df.T                                 # 轉置：列=日期, 欄=指標名
    df.index.name = "record_date"
    df = df.reset_index()
    df["record_date"] = pd.to_datetime(df["record_date"]).dt.date
    df["ticker"]      = ticker
    df["frequency"]   = params["frequency"]

    # 欄位名稱清理
    df.columns = [
        c if c in ("record_date", "ticker", "frequency")
        else clean_col(c)
        for c in df.columns
    ]

    # 對應欄位
    df = df.rename(columns={
        "Financial_Assets_Designatedas_Fair_Value_Through_Profitor_Loss_Total": "Fin_Assets_Designatedas_Fair_Value_Through_Profitor_Loss_Total"
    })

    # 取得要處理的數值欄位 (排除 non_num_cols)
    non_num_cols = {"record_date", "ticker", "frequency"}

    # 取得真正的數值欄位，排除非數值欄位
    num_cols = [c for c in df.select_dtypes(include="number").columns if c not in non_num_cols]

    # apply() 可以「批量操作整欄or整列」
    df[num_cols] = (
        df[num_cols]
        .apply(pd.to_numeric, errors="coerce")   # 每欄轉成數值,不能轉的變 NaN
        .apply(lambda x: np.trunc(x))   # 截斷小數點,不四捨五入
        .astype("Int64")   # nullable 整數
    )

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "record_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "frequency" VARCHAR(5) NOT NULL,
            PRIMARY KEY ("record_date", "ticker", "frequency")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["record_date", "ticker", "frequency"])

# ═══════════════════════════════════════════════════════════
# 2. period-label 表
#    earnings_estimate / revenue_estimate /
#    eps_trend / eps_revisions / growth_estimates
# ═══════════════════════════════════════════════════════════

def upsert_period_label(raw_df: pd.DataFrame,
                        engine,
                        ticker: str,
                        schema_name: str,
                        **params):
    """
    index = period label（0q / +1q / 0y / +1y / LTG）
    columns = 各指標

    PK: created_date, ticker, period
    """
    df = raw_df.copy()
    df.index.name = "period"
    df = df.reset_index() # 把 原本的 "period" index 降為普通"period"欄位
    df["created_date"] = date.today()
    df["ticker"] = ticker
    
    df.columns = [
        c if c in ("created_date", "ticker", "period")
        else clean_col(c)
        for c in df.columns
    ]

    # 取得要處理的數值欄位 (排除 non_num_cols)
    non_num_cols = {"created_date", "ticker", "period"}

    # 取得真正的數值欄位，排除非數值欄位
    num_cols = [c for c in df.select_dtypes(include="number").columns if c not in non_num_cols]

    # apply() 可以「批量操作整欄or整列」
    df[num_cols] = (
        df[num_cols]
        .apply(pd.to_numeric, errors="coerce")   # 每欄轉成數值,不能轉的變 NaN
        .apply(lambda x: np.trunc(x * 100) / 100)   # 截斷到小數點2位
        .astype("Float64")   # nullable 浮點數
    )

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "created_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "period" VARCHAR(5) NOT NULL,
            PRIMARY KEY ("created_date", "ticker", "period")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["created_date", "ticker", "period"])

# ═══════════════════════════════════════════════════════════
# 3. earnings_history
# ═══════════════════════════════════════════════════════════

def upsert_earnings_history(raw_df: pd.DataFrame,
                             engine,
                             ticker: str,
                             schema_name: str,
                             **params):
    """
    index = quarter date
    PK: record_date, ticker, frequency
    """
    df = raw_df.copy()
    df.index.name = "record_date"
    df = df.reset_index()
    df["record_date"] = pd.to_datetime(df["record_date"]).dt.date
    df["ticker"] = ticker
    df["frequency"] = "Q"

    df.columns = [
        c if c in ("record_date", "ticker", "frequency")
        else clean_col(c)
        for c in df.columns
    ]

    # 取得要處理的數值欄位 (排除 non_num_cols)
    non_num_cols = {"record_date", "ticker", "frequency"}

    # 取得真正的數值欄位，排除非數值欄位
    num_cols = [c for c in df.select_dtypes(include="number").columns if c not in non_num_cols]

    # apply() 可以「批量操作整欄or整列」
    df[num_cols] = (
        df[num_cols]
        .apply(pd.to_numeric, errors="coerce")   # 每欄轉成數值,不能轉的變 NaN
        .apply(lambda x: np.trunc(x * 100) / 100)   # 截斷到小數點2位
        .astype("Float64")  # nullable 浮點數
    )

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "record_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "frequency" VARCHAR(5) NOT NULL,
            PRIMARY KEY ("record_date", "ticker", "frequency")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["record_date", "ticker", "frequency"])

# ═══════════════════════════════════════════════════════════
# 4. recommendations_summary
# ═══════════════════════════════════════════════════════════

def upsert_recommendations_summary(raw_df: pd.DataFrame,
                                    engine,
                                    ticker: str,
                                    schema_name: str,
                                    **params):
    """
    columns: period / strongBuy / buy / hold / sell / strongSell
    PK: "created_date", "ticker", "period"
    """
    df = raw_df.copy().reset_index(drop=True)
    df["created_date"] = date.today()
    df["ticker"] = ticker

    df.columns = [
        c if c in ("created_date", "ticker", "period")
        else clean_col(c)
        for c in df.columns
    ]

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "created_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "period" VARCHAR(5) NOT NULL,
            PRIMARY KEY ("created_date", "ticker", "period")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["created_date", "ticker", "period"])

# ═══════════════════════════════════════════════════════════
# 5. institutional_holders / mutualfund_holders
# ═══════════════════════════════════════════════════════════

def upsert_holders(raw_df: pd.DataFrame,
                   engine,
                   ticker: str,
                   schema_name: str,
                   **params):
    """
    無自然唯一鍵 → 每次 insert 新快照（不 upsert）
    """

    df = raw_df.copy().reset_index(drop=True)
    df["created_date"] = date.today()
    df["ticker"]     = ticker
    

    df.columns = [clean_col(c) for c in df.columns]

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "created_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "Holder" TEXT NOT NULL,
            PRIMARY KEY ("created_date", "ticker", "Holder")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["created_date", "ticker", "Holder"])

# ═══════════════════════════════════════════════════════════
# 6. insider_transactions
# ═══════════════════════════════════════════════════════════

def upsert_insider_transactions(raw_df: pd.DataFrame,
                                 engine,
                                 ticker: str,
                                 schema_name: str,
                                 **params):
    """
    PK: ticker, insider_name, transaction_date (Start Date)
    """
    df = raw_df.copy().reset_index(drop=True)
    df["ticker"] = ticker

    df.columns = [clean_col(c) for c in df.columns]

    # 特定欄位 "缺失值" 調整
    cols = ["Value", "Shares"]
    df[cols] = df[cols].replace([None, ""], np.nan)

    # 對應欄位
    df = df.rename(columns={
        "Insider":    "Insider_name"
    })

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "Start_Date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "Insider_name" TEXT NOT NULL,
            "Value"        NUMERIC NOT NULL,
            "Shares"       BIGINT NOT NULL,
            PRIMARY KEY ("Start_Date", "ticker", "Insider_name", "Value", "Shares")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["Start_Date", "ticker", "Insider_name", "Value", "Shares"])

# ═══════════════════════════════════════════════════════════
# 7. insider_roster_holders
# ═══════════════════════════════════════════════════════════

def upsert_insider_roster_holders(raw_df: pd.DataFrame,
                                   engine,
                                   ticker: str,
                                   schema_name: str,
                                   **params):
    """
    PK: created_date, ticker
    """
    df = raw_df.copy().reset_index(drop=True)
    df["created_date"] = date.today()
    df["ticker"] = ticker

    df.columns = [clean_col(c) for c in df.columns]

    # 取得要處理的日期欄位
    date_keywords = {"Date"}
    date_cols =  [c for c in df.columns if any(k in c for k in date_keywords)]
    
    # 處理日期欄位
    df[date_cols] = (
        df[date_cols].apply(lambda x: pd.to_datetime(x, errors="coerce").dt.date)
    )    # 每欄轉成日期,不能轉的變 NaN

    # 取得要處理的數值欄位
    num_keywords = {"Directly", "Indirectly", "positionSummary"}
    num_cols =  [c for c in df.columns if any(k in c for k in num_keywords)]

    # apply() 可以「批量操作整欄or整列」
    df[num_cols] = (
        df[num_cols]
        .apply(pd.to_numeric, errors="coerce")   # 每欄轉成數值,不能轉的變 NaN
        .apply(lambda x: np.trunc(x))   # 截斷到小數點0位
        .astype("Int64")  # nullable 整數
    )

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "created_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "Name" TEXT NOT NULL,
            "Position_Direct_Date" DATE,
            "Latest_Transaction_Date" DATE NOT NULL,
            PRIMARY KEY ("created_date", "ticker", "Name", "Latest_Transaction_Date")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["created_date", "ticker", "Name", "Latest_Transaction_Date"])

# ═══════════════════════════════════════════════════════════
# 8. stock_info（來自 info dict）
# ═══════════════════════════════════════════════════════════

def upsert_info(info: dict,
                      engine,
                      ticker: str,
                      schema_name: str,
                      **params):
    """
    PK: created_date, ticker
    """
    keys = [
        "forwardPE", "trailingPegRatio", "priceToSalesTrailing12Months",
        "dividendRate",
        "targetHighPrice", "targetLowPrice", "targetMeanPrice",
        "grossMargins", "operatingMargins", "profitMargins",
        "returnOnEquity", "returnOnAssets",
        "heldPercentInsiders", "heldPercentInstitutions",
        "sharesOutstanding",
    ]

    row = {k: info.get(k) for k in keys}
    row["created_date"] = date.today()
    row["ticker"]      = ticker

    df = pd.DataFrame([row])

    # 1.建表（第一次用）
    # ---------------------------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{params["table_name"]}" (
            "created_date" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            PRIMARY KEY ("created_date", "ticker")
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # 2.更新並新增資料到表中
    # ---------------------------
    upsert(df, engine, schema_name, params["table_name"], pk=["created_date", "ticker"])

# ═══════════════════════════════════════════════════════════
# 一鍵執行：所有表

def upsert_all(engine,
            ticker,
            schema_name: str = "US"):
    
    # lambda 先把「取資料的方法」存起來，等之後真的有 stock 再執行
    tasks = {
        # 財報三表
        "income_statement_A": {
            "func": upsert_financial,
            "data": lambda stock: stock.income_stmt,
            "params": {"table_name": "income_statement", "frequency": "A"},
        },
        "income_statement_Q": {
            "func": upsert_financial,
            "data": lambda stock: stock.quarterly_income_stmt,
            "params": {"table_name": "income_statement", "frequency": "Q"},
        },
        "balance_sheet_A": {
            "func": upsert_financial,
            "data": lambda stock: stock.balance_sheet,
            "params": {"table_name": "balance_sheet", "frequency": "A"},
        },
        "balance_sheet_Q": {
            "func": upsert_financial,
            "data": lambda stock: stock.quarterly_balance_sheet,
            "params": {"table_name": "balance_sheet", "frequency": "Q"},
        },
        "cash_flow_A": {
            "func": upsert_financial,
            "data": lambda stock: stock.cashflow,
            "params": {"table_name": "cash_flow", "frequency": "A"},
        },
        "cash_flow_Q": {
            "func": upsert_financial,
            "data": lambda stock: stock.quarterly_cashflow,
            "params": {"table_name": "cash_flow", "frequency": "Q"},
        },

        # 分析師預估
        "earnings_estimate": {
            "func": upsert_period_label,
            "data": lambda stock: stock.earnings_estimate,
            "params": {"table_name": "earnings_estimate"},
        },
        "revenue_estimate": {
            "func": upsert_period_label,
            "data": lambda stock: stock.revenue_estimate,
            "params": {"table_name": "revenue_estimate"},
        },
        "eps_trend": {
            "func": upsert_period_label,
            "data": lambda stock: stock.eps_trend,
            "params": {"table_name": "eps_trend"},
        },
        "eps_revisions": {
            "func": upsert_period_label,
            "data": lambda stock: stock.eps_revisions,
            "params": {"table_name": "eps_revisions"},
        },
        "growth_estimates": {
            "func": upsert_period_label,
            "data": lambda stock: stock.growth_estimates,
            "params": {"table_name": "growth_estimates"},
        },

        # 歷史 + 評級
        "earnings_history": {
            "func": upsert_earnings_history,
            "data": lambda stock: stock.earnings_history,
            "params": {"table_name": "earnings_history"},
        },
        "recommendations_summary": {
            "func": upsert_recommendations_summary,
            "data": lambda stock: stock.recommendations_summary,
            "params": {"table_name": "recommendations_summary"},
        },

        # 持股
        "institutional_holders": {
            "func": upsert_holders,
            "data": lambda stock: stock.institutional_holders,
            "params": {"table_name": "institutional_holders"},
        },
        "mutualfund_holders": {
            "func": upsert_holders,
            "data": lambda stock: stock.mutualfund_holders,
            "params": {"table_name": "mutualfund_holders"},
        },
        "insider_transactions": {
            "func": upsert_insider_transactions,
            "data": lambda stock: stock.insider_transactions,
            "params": {"table_name": "insider_transactions"},
        },
        "insider_roster_holders": {
            "func": upsert_insider_roster_holders,
            "data": lambda stock: stock.insider_roster_holders,
            "params": {"table_name": "insider_roster_holders"},
        },

        # info
        "stock_info": {
            "func": upsert_info,
            "data": lambda stock: stock.get_info(),
            "params": {"table_name": "stock_info"},
        }
    }
    
    stock = yf.Ticker(ticker)

    for key, task in tasks.items():

        try:
            df = task["data"](stock)
            func = task["func"]
            params = task["params"]

            # hasattr, 先確認有沒有這個屬性，再決定要不要用
            if df is None or (hasattr(df, "empty") and df.empty):
                print(f" !! {key} 無資料,跳過")

            else:
                func(df, engine, ticker, schema_name, **params)
                
        except Exception as e:
            print(f" !! {key}: {e} ,發生錯誤,跳過")


    # # 財報三表
    # upsert_financial(stock.income_stmt,             engine, ticker, schema_name, "income_statement", "A")
    # upsert_financial(stock.quarterly_income_stmt,   engine, ticker, schema_name, "income_statement", "Q")
    # upsert_financial(stock.balance_sheet,           engine, ticker, schema_name, "balance_sheet",    "A")
    # upsert_financial(stock.quarterly_balance_sheet, engine, ticker, schema_name, "balance_sheet",    "Q")
    # upsert_financial(stock.cashflow,                engine, ticker, schema_name, "cash_flow",        "A")
    # upsert_financial(stock.quarterly_cashflow,      engine, ticker, schema_name, "cash_flow",        "Q")

    # # 分析師預估
    # upsert_period_label(stock.earnings_estimate, engine, ticker, schema_name, "earnings_estimate")
    # upsert_period_label(stock.revenue_estimate,  engine, ticker, schema_name, "revenue_estimate")
    # upsert_period_label(stock.eps_trend,         engine, ticker, schema_name, "eps_trend")
    # upsert_period_label(stock.eps_revisions,     engine, ticker, schema_name, "eps_revisions")
    # upsert_period_label(stock.growth_estimates,  engine, ticker, schema_name, "growth_estimates")

    # # 歷史財報 + 評級
    # upsert_earnings_history(        stock.earnings_history,        engine, ticker, schema_name, "earnings_history")
    # upsert_recommendations_summary( stock.recommendations_summary, engine, ticker, schema_name, "recommendations_summary")

    # # 持股
    # upsert_holders(               stock.institutional_holders,  engine, ticker, schema_name, "institutional_holders")
    # upsert_holders(               stock.mutualfund_holders,     engine, ticker, schema_name, "mutualfund_holders")
    # upsert_insider_transactions(  stock.insider_transactions,   engine, ticker, schema_name, "insider_transactions")
    # upsert_insider_roster_holders(stock.insider_roster_holders, engine, ticker, schema_name, "insider_roster_holders")

    # # stock_info
    # upsert_info(stock.get_info(), engine, ticker, schema_name, "stock_info")

def main():
    tickers = get_screened_list(engine)

    for idx1, ticker in enumerate(tickers, start=1):
        print(f"========================================")
        print(f"{idx1} ,正在蒐集 股票:{ticker} 基本面詳細資訊")

        try:
            upsert_all(engine=engine, 
                        ticker=ticker,
                        schema_name="US")
        
        except Exception as e:
            print(" !! 發生錯誤:", e)

        finally:
            time.sleep(1)

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()
