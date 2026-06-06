import talib
import pandas as pd

def get_vol_indicator(df):

    ###### Volume Indicator Functions

    # 1. AD - Chaikin A/D Line (能量潮累積線)
    # df["AD"] = talib.AD(df["High"],df["Low"],df["Close"],df["Volume"])

    # 2. ADOSC - Chaikin A/D Oscillator (能量潮震盪指標)
    # df["ADOSC"] = talib.ADOSC(df["High"],df["Low"],df["Close"],df["Volume"],fastperiod=3,slowperiod=10)

    # 3. OBV - On Balance Volume (平衡交易量指標)
    # df["OBV"] = talib.OBV(df["Close"],df["Volume"])

    print(df)
    return df