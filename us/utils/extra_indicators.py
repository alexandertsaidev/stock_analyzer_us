import talib
import pandas as pd

def get_extra_indicator(df):

    ###  Extra Indicator Functions
    # 1. Force Index 用來衡量價格變動結合成交量的「推動力量」
    df["FI"] = (df["Close"]- df["Close"].shift(1)) * df["Volume"]

    # 1-1
    df["FI_2"] = talib.EMA(df["FI"], timeperiod=2)
    # 1-2
    df["FI_13"] = talib.EMA(df["FI"], timeperiod=13)

    # 2.Elder-Ray (Bull / Bear Power)

    df["EMA13"] = talib.EMA(df["Close"], timeperiod=13)

    # Bull / Bear Power
    df["Bull_Power"] = df["High"] - df["EMA13"]
    df["Bear_Power"] = df["Low"]  - df["EMA13"]

    print(df)
    return df