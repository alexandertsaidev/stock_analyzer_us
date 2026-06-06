import talib
import pandas as pd

def get_overlap_indicator(df):

    ### Overlap Studies Functions
    
    # 1. ACCBANDS - Acceleration Bands (加速帶)
    # df["acc_upperband"], df["acc_middleband"], df["acc_lowerband"] = talib.ACCBANDS(df["Close"], timeperiod=20)

    # 2. BBANDS - 布林帶 (Bollinger Bands)
    # 布林帶是一個基於移動平均線和標準差的指標，通常用來衡量市場的波動性。
    df["upperband"], df["middleband"], df["lowerband"] = talib.BBANDS(df["Close"], timeperiod=13, nbdevup=2.7, nbdevdn=2.7, matype=1)
    # timeperiod=5: 計算布林帶的期數，這裡是5個周期。
    # nbdevup=2 和 nbdevdn=2: 上下帶的標準差倍數。
    # matype=0: 使用簡單移動平均 (SMA) 作為中間帶。
    # matype=1: 指数加权移动平均（EMA）
    # matype=2: 加权移动平均（WMA）

    """
    # 3. DEMA - 雙指數移動平均 (Double Exponential Moving Average)
    # DEMA 是對傳統移動平均的一種改進，通過加權來提高反應速度。
    df['DEMA30'] = talib.DEMA(df["Close"], timeperiod=30)
    # timeperiod=30: 30個周期的時間窗口。
    """

    # 4. EMA - 指數移動平均 (Exponential Moving Average)
    # EMA 是一種加權移動平均，更重視近期數據，通常用來捕捉市場的趨勢。
    df['EMA13'] = talib.EMA(df["Close"], timeperiod=13)
    df['EMA26'] = talib.EMA(df["Close"], timeperiod=26)
    # timeperiod=30: 30個周期的時間窗口。
    # 注意: EMA 在短期內可能會有不穩定的情況。
    

    # 5. HT_TRENDLINE - 希爾伯特變換即時趨勢線 (Hilbert Transform - Instantaneous Trendline)
    # 此指標用於捕捉市場的即時趨勢，並給出市場方向的預測。
    # df["HT_T"] = talib.HT_TRENDLINE(df["Close"])
    # 注意: 此指標會在初期階段有不穩定性。

    # 6. KAMA - Kaufman 自適應移動平均 (Kaufman Adaptive Moving Average)
    # KAMA 是根據市場的波動性來調整平滑因子，從而自適應調整計算周期。
    # df["KAMA"] = talib.KAMA(df["Close"], timeperiod=30)
    # 注意: KAMA 在某些情況下也會有不穩定性。

    # 7. MA - 移動平均 (Moving Average)
    # MA 是最基本的移動平均指標，通常用來平滑數據並捕捉長期趨勢。
    # df["MA30"] = talib.MA(df["Close"], timeperiod=30, matype=0)
    # timeperiod=30: 30個周期的時間窗口。
    # matype=0: 使用簡單移動平均 (SMA)。

    # 8. MAMA - MESA 自適應移動平均 (MESA Adaptive Moving Average)
    # MAMA 是一種自適應移動平均方法，通過計算平滑因子來適應市場的波動。
    # df["MAMA"], df["FAMA"] = talib.MAMA(df["Close"], fastlimit=0, slowlimit=0)
    # 注意: 此指標也有不穩定的周期。

    # 9. MAVP - 可變周期移動平均 (Moving Average with Variable Period)
    # 這是一種具有變動周期的移動平均，根據市場波動性調整周期。
    # df["MAVP30"] = talib.MAVP(df["Close"], periods=[7] , minperiod=2, maxperiod=30, matype=0)
    # periods: 變動周期的數據。
    # minperiod=2 和 maxperiod=30: 最小和最大周期。

    # 10. MIDPOINT - 中點價格 (MidPoint over period)
    # 計算某一段時間內的最高價與最低價的中點。
    # df["MIDPOINT"] = talib.MIDPOINT(df["Close"], timeperiod=14)
    # timeperiod=14: 14個周期的中點價格。

    # 11. MIDPRICE - 中點價格 (Midpoint Price over period)
    # 類似於 MIDPOINT，但用於計算給定周期內的中價（高低之間的中點）。
    # df["MIDPRICE"] = talib.MIDPRICE(df["High"] ,df["Low"], timeperiod=14)
    # high 和 low: 分別是最高價和最低價的數據。

    """
    # 12. SAR - 拋物線指標 (Parabolic SAR)
    # SAR 用於顯示價格的趨勢方向，並且當價格反轉時給出信號。
    df["SAR"] = talib.SAR(df["High"] ,df["Low"], acceleration=0, maximum=0)
    # acceleration=0 和 maximum=0: 這些是 SAR 的加速參數。

    # 13. SAREXT - 拋物線指標延伸 (Parabolic SAR - Extended)
    # 這是 SAR 的擴展版本，包含更多的自定義參數。
    df["SAREXT"] = talib.SAREXT(df["High"] ,df["Low"], startvalue=0, offsetonreverse=0, accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0, accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)
    # 這些參數允許更精細的控制 SAR 指標。

    """
    # 14. SMA - 簡單移動平均 (Simple Moving Average)
    # SMA 是一種最常見的移動平均指標，計算一定時間內的平均值。
    # df["SMA30"] = talib.SMA(df["Close"], timeperiod=30)
    # timeperiod=30: 30個周期的時間窗口。
    # df['SMA50'] = talib.SMA(df["Close"], timeperiod=50)   # Simple Moving Average
    # df['SMA200'] = talib.SMA(df["Close"], timeperiod=200) # Simple Moving Average

    # 15. T3 - 三重指數移動平均 (Triple Exponential Moving Average)
    # T3 是在 TEMA 基礎上進行的進一步平滑，通常用來捕捉趨勢並減少市場噪聲。
    # df["T3"] = talib.T3(df["Close"], timeperiod=5, vfactor=0)
    # 注意: T3 在某些情況下也會有不穩定性。

    # 16. TEMA - 三重指數移動平均 (Triple Exponential Moving Average)
    # TEMA 是在傳統的指數移動平均基礎上做了三次加權。
    # df["TEMA30"] = talib.TEMA(df["Close"], timeperiod=30)
    # timeperiod=30: 30個周期的時間窗口。

    # 17. TRIMA - 三角移動平均 (Triangular Moving Average)
    # TRIMA 是在普通移動平均的基礎上對數據進行進一步平滑。
    # df["TRIMA30"] = talib.TRIMA(df["Close"], timeperiod=30)
    # timeperiod=30: 30個周期的時間窗口。

    """
    # 18. WMA - 加權移動平均 (Weighted Moving Average)
    # WMA 是一種加權平均的移動平均方法，給予近期數據更高的權重。
    df["WMA30"] = talib.WMA(df["Close"], timeperiod=30)
    # timeperiod=30: 30個周期的時間窗口。

    """
    print(df)
    return df