import talib
import pandas as pd

def get_volat_indicator(df):

    ### Volatility Indicator Functions


    # 1. Average True Range (平均波動幅度)
    df["ATR"] = talib.ATR(df["High"], df["Low"], df["Close"], timeperiod=14)

    # 2. Normalized Average True Range (標準化平均波動)
    # df["NATR"] = talib.NATR(df["High"], df["Low"], df["Close"], timeperiod=14)

    # 3. True Range (單日波動範圍)
    # df["TRANGE"] = talib.TRANGE(df["High"], df["Low"], df["Close"])

    print(df)
    return df