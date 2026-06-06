import talib
import pandas as pd

def get_stat_indicator(df):

    ### Statistic Functions

    # # 1. Beta (需要兩個序列，例如收盤價1 vs 收盤價2)
    # df["beta"] = talib.BETA(df["High"], df["Low"], timeperiod=5)

    # # 2. Correlation (收盤價1 vs 收盤價2 的相關性)
    # df["correl"] = talib.CORREL(df["High"], df["Low"], timeperiod=30)

    # 3. Linear Regression 回歸線
    # df["Linear_Reg_Line"] = talib.LINEARREG(df["Close"], timeperiod=14)

    # 4. Linear Regression Angle (角度)
    # df["Linear_Reg_Ang"] = talib.LINEARREG_ANGLE(df["Close"], timeperiod=14)

    # 5. Linear Regression Intercept (截距)
    # df["Linear_Reg_Int"] = talib.LINEARREG_INTERCEPT(df["Close"], timeperiod=14)

    # 6. Linear Regression Slope (斜率)
    # df["Linear_Reg_Slope"] = talib.LINEARREG_SLOPE(df["Close"], timeperiod=14)

    # 7. Standard Deviation (標準差)
    # df["Std_Dev"] = talib.STDDEV(df["Close"], timeperiod=5, nbdev=1)

    # 8. Time Series Forecast (時間序列預測)
    # df["TSF"] = talib.TSF(df["Close"], timeperiod=14)

    # 9. Variance (變異數)
    # df["VAR"] = talib.VAR(df["Close"], timeperiod=5, nbdev=1)

    print(df)

    return df