import talib
import pandas as pd

def get_mom_indicator(df):

    # 1. ADX - Average Directional Movement Index (平均方向指標)
    df["ADX"] = talib.ADX(df["High"], df["Low"], df["Close"], timeperiod=14)
    # 說明：衡量趨勢強度

    # 2. ADXR - Average Directional Movement Index Rating (ADX 評級)
    # df["ADXR"] = talib.ADXR(df["High"], df["Low"], df["Close"], timeperiod=14)
    # 說明：ADX 平滑版本

    # 3. APO - Absolute Price Oscillator (絕對價格震盪指標)
    # df["APO"] = talib.APO(df["Close"], fastperiod=12, slowperiod=26, matype=0)
    # 說明：短期與長期 EMA 差值

    # 4. AROON - Aroon (阿隆指標)
    # df["AROON_down"], df["AROON_up"] = talib.AROON(df["High"], df["Low"], timeperiod=14)
    # 說明：判斷趨勢形成及強弱

    # 5. AROONOSC - Aroon Oscillator (阿隆震盪指標)
    # df["AROONOSC"] = talib.AROONOSC(df["High"], df["Low"], timeperiod=14)
    # 說明：AROON_up - AROON_down，正值上升趨勢

    # 6. BOP - Balance Of Power (力量平衡指標)
    # df["BOP"] = talib.BOP(df["Open"], df["High"], df["Low"], df["Close"])
    # 說明：買賣力量對價格影響

    # 7. CCI - Commodity Channel Index (商品通道指標)
    # df["CCI"] = talib.CCI(df["High"], df["Low"], df["Close"], timeperiod=14)
    # 說明：價格偏離均值程度

    # 8. CMO - Chande Momentum Oscillator (錢德動量)
    # df["CMO"] = talib.CMO(df["Close"], timeperiod=14)
    # 說明：價格上漲下跌力度差異

    # 9. DX - Directional Movement Index (方向性運動指標)
    # df["DX"] = talib.DX(df["High"], df["Low"], df["Close"], timeperiod=14)
    # 說明：趨勢強度指標

    # 10. IMI - Intraday Momentum Index (盤中動量指標)
    # df["IMI"] = talib.IMI(df["Open"], df["Close"], timeperiod=14)

    # 11. MACD - Moving Average Convergence/Divergence (移動平均收斂/發散)
    df["MACD"], df["MACD_signal"], df["MACD_hist"] = talib.MACD(df["Close"], fastperiod=12, slowperiod=26, signalperiod=9)
    # 說明：短期與長期 EMA 差值，判斷趨勢和訊號

    # 12. MACDEXT - 可控制 MA 類型 MACD
    # df["MACDEXT"], df["MACDEXT_signal"], df["MACDEXT_hist"] = talib.MACDEXT(df["Close"], fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)

    # 13. MACDFIX - MACD Fix 12/26
    # df["MACDFIX"], df["MACDFIX_signal"], df["MACDFIX_hist"] = talib.MACDFIX(df["Close"], signalperiod=9)

    # 14. MFI - Money Flow Index (資金流量指標)
    # df["MFI"] = talib.MFI(df["High"], df["Low"], df["Close"], df["Volume"], timeperiod=14)

    # 15. MINUS_DI - Minus Directional Indicator (負方向指標)
    df["MINUS_DI"] = talib.MINUS_DI(df["High"], df["Low"], df["Close"], timeperiod=13)

    # 16. MINUS_DM - Minus Directional Movement (負方向運動)
    # df["MINUS_DM"] = talib.MINUS_DM(df["High"], df["Low"], timeperiod=13)

    # 17. MOM - Momentum (動量指標)
    # df["MOM"] = talib.MOM(df["Close"], timeperiod=7)

    # 18. PLUS_DI - Plus Directional Indicator (正方向指標)
    df["PLUS_DI"] = talib.PLUS_DI(df["High"], df["Low"], df["Close"], timeperiod=14)

    # 19. PLUS_DM - Plus Directional Movement (正方向運動)
    # df["PLUS_DM"] = talib.PLUS_DM(df["High"], df["Low"], timeperiod=14)

    # 20. PPO - Percentage Price Oscillator (百分比價格震盪)
    # df["PPO"] = talib.PPO(df["Close"], fastperiod=12, slowperiod=26, matype=0)

    # 21-1. ROC - Rate of Change (變動率)
    # df["ROC"] = talib.ROC(df["Close"], timeperiod=7)
    
    # 21-2. SROC - Smoothed Rate of Change（平滑变动率）
    # df['EMA13'] = talib.EMA(df["Close"], timeperiod=13)
    
    # df["SROC"] = df['SROC'] = (df['EMA13'] - df['EMA13'].shift(21)) / df['EMA13'] * 100

    # 22. ROCP - Rate of Change Percentage (百分比變動)
    # df["ROCP"] = talib.ROCP(df["Close"], timeperiod=10)

    # 23. ROCR - Rate of Change Ratio (變化比率)
    # df["ROCR"] = talib.ROCR(df["Close"], timeperiod=10)

    # 24. ROCR100 - Rate of Change Ratio 100 scale (變化比率百分比)
    # df["ROCR100"] = talib.ROCR100(df["Close"], timeperiod=10)

    # 25. RSI - Relative Strength Index (相對強弱指標)
    # df["RSI"] = talib.RSI(df["Close"], timeperiod=14)

    # 26. STOCH - 隨機指標
    df["STOCH_slowk"], df["STOCH_slowd"] = talib.STOCH(df["High"], df["Low"], df["Close"], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)

    # 27. STOCHF - 隨機快線
    # df["STOCHF_fastk"], df["STOCHF_fastd"] = talib.STOCHF(df["High"], df["Low"], df["Close"], fastk_period=5, fastd_period=3, fastd_matype=0)

    # 28. STOCHRSI - 隨機 RSI 指標
    # df["STOCHRSI_fastk"], df["STOCHRSI_fastd"] = talib.STOCHRSI(df["Close"], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)

    # 29. TRIX - 三重指數平滑平均變動率
    # df["TRIX"] = talib.TRIX(df["Close"], timeperiod=30)

    # 30. ULTOSC - 終極震盪指標
    # df["ULTOSC"] = talib.ULTOSC(df["High"], df["Low"], df["Close"], timeperiod1=7, timeperiod2=14, timeperiod3=28)

    # 31. WILLR - 威廉指標
    df["WILLR"] = talib.WILLR(df["High"], df["Low"], df["Close"], timeperiod=7)

    print(df)

    return df