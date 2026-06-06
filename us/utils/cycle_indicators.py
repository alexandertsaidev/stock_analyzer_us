import talib
import pandas as pd

def get_cycle_indicator(df):

    ### Cycle Indicator Functions

    # 1. Dominant Cycle Period (主導週期長度)
    # df["HT_DCPERIOD"] = talib.HT_DCPERIOD(df["Close"])
    # ex.數值 = 20 → 表示目前價格波動的主導週期約 20 根 K 線
    # ex.你的 K 線是日線 → 20 日

    # 2. Dominant Cycle Phase (主導週期相位)
    # df["HT_DCPHASE"] = talib.HT_DCPHASE(df["Close"])
    # 例如：0度 → 波谷開始, 90度 → 波峰 ,180度 → 波谷結束

    # 3. Phasor Components (相位分量)
    # df["HT_PHASOR_INPHASE"], df["HT_PHASOR_QUADRATURE"] = talib.HT_PHASOR(df["Close"])
    # "HT_PHASOR_INPHASE" 與週期同步的分量 
    # "HT_PHASOR_QUADRATURE" 與週期相差 90° 的分量

    # 4. Sine Wave (正弦波 & 領先波)
    # df["HT_SINE"], df["HT_LEADSINE"] = talib.HT_SINE(df["Close"])
    # 正弦波序列 ； 正弦波提前 90° 的領先波

    # 5. Trend vs Cycle Mode (趨勢 vs 週期模式)
    # df["HT_TRENDMODE"] = talib.HT_TRENDMODE(df["Close"])
    # 趨勢模式=1 / 週期模式=0

    print(df)
    return df
