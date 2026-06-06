import talib
import pandas as pd

def get_pattern_indicator(df):

    ### Pattern Recognition Functions
    """
    # 1. 兩隻烏鴉，看跌反轉
    df["CDL2CROWS"] = talib.CDL2CROWS(df["Open"], df["High"], df["Low"], df["Close"])

    # 2. 三隻黑烏鴉，強烈看跌
    df["CDL3BLACKCROWS"] = talib.CDL3BLACKCROWS(df["Open"], df["High"], df["Low"], df["Close"])

    # 3. 三內部上升/下降，反轉型態
    df["CDL3INSIDE"] = talib.CDL3INSIDE(df["Open"], df["High"], df["Low"], df["Close"])

    # 4. 三線打擊，反轉訊號
    df["CDL3LINESTRIKE"] = talib.CDL3LINESTRIKE(df["Open"], df["High"], df["Low"], df["Close"])

    # 5. 三外側，反轉型態
    df["CDL3OUTSIDE"] = talib.CDL3OUTSIDE(df["Open"], df["High"], df["Low"], df["Close"])

    # 6. 南方三星，稀有，看漲反轉
    df["CDL3STARSINSOUTH"] = talib.CDL3STARSINSOUTH(df["Open"], df["High"], df["Low"], df["Close"])

    # 7. 三個白兵，強烈看漲
    df["CDL3WHITESOLDIERS"] = talib.CDL3WHITESOLDIERS(df["Open"], df["High"], df["Low"], df["Close"])

    # 8. 棄嬰形態，可靠反轉
    df["CDLABANDONEDBABY"] = talib.CDLABANDONEDBABY(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 9. 遞進阻擋，漲勢衰竭
    df["CDLADVANCEBLOCK"] = talib.CDLADVANCEBLOCK(df["Open"], df["High"], df["Low"], df["Close"])

    # 10. 捉腰帶線，方向強烈
    df["CDLBELTHOLD"] = talib.CDLBELTHOLD(df["Open"], df["High"], df["Low"], df["Close"])

    # 11. 脫離，反轉或延續
    df["CDLBREAKAWAY"] = talib.CDLBREAKAWAY(df["Open"], df["High"], df["Low"], df["Close"])

    # 12. 收盤光頭光腳，強烈方向
    df["CDLCLOSINGMARUBOZU"] = talib.CDLCLOSINGMARUBOZU(df["Open"], df["High"], df["Low"], df["Close"])

    # 13. 隱藏嬰兒吞沒，看跌
    df["CDLCONCEALBABYSWALL"] = talib.CDLCONCEALBABYSWALL(df["Open"], df["High"], df["Low"], df["Close"])

    # 14. 反擊線，可能反轉
    df["CDLCOUNTERATTACK"] = talib.CDLCOUNTERATTACK(df["Open"], df["High"], df["Low"], df["Close"])

    # 15. 烏雲蓋頂，看跌
    df["CDLDARKCLOUDCOVER"] = talib.CDLDARKCLOUDCOVER(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 16. 十字線，市場猶豫
    df["CDLDOJI"] = talib.CDLDOJI(df["Open"], df["High"], df["Low"], df["Close"])

    # 17. 十字星，可能反轉
    df["CDLDOJISTAR"] = talib.CDLDOJISTAR(df["Open"], df["High"], df["Low"], df["Close"])

    # 18. 蜻蜓十字，看漲
    df["CDLDRAGONFLYDOJI"] = talib.CDLDRAGONFLYDOJI(df["Open"], df["High"], df["Low"], df["Close"])

    # 19. 吞沒形態，強烈反轉
    df["CDLENGULFING"] = talib.CDLENGULFING(df["Open"], df["High"], df["Low"], df["Close"])

    # 20. 黃昏十字星，強烈看跌
    df["CDLEVENINGDOJISTAR"] = talib.CDLEVENINGDOJISTAR(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 21. 黃昏星，趨勢反轉
    df["CDLEVENINGSTAR"] = talib.CDLEVENINGSTAR(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 22. 向上/向下並列白線，趨勢延續
    df["CDLGAPSIDESIDEWHITE"] = talib.CDLGAPSIDESIDEWHITE(df["Open"], df["High"], df["Low"], df["Close"])

    # 23. 墓碑十字，看跌
    df["CDLGRAVESTONEDOJI"] = talib.CDLGRAVESTONEDOJI(df["Open"], df["High"], df["Low"], df["Close"])

    # 24. 錘子線，看漲反轉
    df["CDLHAMMER"] = talib.CDLHAMMER(df["Open"], df["High"], df["Low"], df["Close"])

    # 25. 吊人線，看跌
    df["CDLHANGINGMAN"] = talib.CDLHANGINGMAN(df["Open"], df["High"], df["Low"], df["Close"])

    # 26. 孕線，反轉
    df["CDLHARAMI"] = talib.CDLHARAMI(df["Open"], df["High"], df["Low"], df["Close"])

    # 27. 十字孕線，更強烈反轉
    df["CDLHARAMICROSS"] = talib.CDLHARAMICROSS(df["Open"], df["High"], df["Low"], df["Close"])

    # 28. 高浪線，市場不確定
    df["CDLHIGHWAVE"] = talib.CDLHIGHWAVE(df["Open"], df["High"], df["Low"], df["Close"])

    # 29. 陷阱線，假突破反轉
    df["CDLHIKKAKE"] = talib.CDLHIKKAKE(df["Open"], df["High"], df["Low"], df["Close"])

    # 30. 修正版陷阱線
    df["CDLHIKKAKEMOD"] = talib.CDLHIKKAKEMOD(df["Open"], df["High"], df["Low"], df["Close"])

    # 31. 歸巢鴿，反轉
    df["CDLHOMINGPIGEON"] = talib.CDLHOMINGPIGEON(df["Open"], df["High"], df["Low"], df["Close"])

    # 32. 平頭三烏鴉，強烈看跌
    df["CDLIDENTICAL3CROWS"] = talib.CDLIDENTICAL3CROWS(df["Open"], df["High"], df["Low"], df["Close"])

    # 33. 頸內線，看跌延續
    df["CDLINNECK"] = talib.CDLINNECK(df["Open"], df["High"], df["Low"], df["Close"])

    # 34. 倒錘子線，看漲
    df["CDLINVERTEDHAMMER"] = talib.CDLINVERTEDHAMMER(df["Open"], df["High"], df["Low"], df["Close"])

    # 35. 反攻，強烈反轉
    df["CDLKICKING"] = talib.CDLKICKING(df["Open"], df["High"], df["Low"], df["Close"])

    # 36. 反攻 (長影決定多空)
    df["CDLKICKINGBYLENGTH"] = talib.CDLKICKINGBYLENGTH(df["Open"], df["High"], df["Low"], df["Close"])

    # 37. 梯底，反轉看漲
    df["CDLLADDERBOTTOM"] = talib.CDLLADDERBOTTOM(df["Open"], df["High"], df["Low"], df["Close"])

    # 38. 長腳十字，市場高度猶豫
    df["CDLLONGLEGGEDDOJI"] = talib.CDLLONGLEGGEDDOJI(df["Open"], df["High"], df["Low"], df["Close"])

    # 39. 長蠟燭，趨勢強烈
    df["CDLLONGLINE"] = talib.CDLLONGLINE(df["Open"], df["High"], df["Low"], df["Close"])

    # 40. 光頭光腳，方向強烈
    df["CDLMARUBOZU"] = talib.CDLMARUBOZU(df["Open"], df["High"], df["Low"], df["Close"])

    # 41. 平底，反轉
    df["CDLMATCHINGLOW"] = talib.CDLMATCHINGLOW(df["Open"], df["High"], df["Low"], df["Close"])

    # 42. 鋪墊，趨勢延續
    df["CDLMATHOLD"] = talib.CDLMATHOLD(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 43. 晨星十字，強烈看漲
    df["CDLMORNINGDOJISTAR"] = talib.CDLMORNINGDOJISTAR(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 44. 晨星，看漲反轉
    df["CDLMORNINGSTAR"] = talib.CDLMORNINGSTAR(df["Open"], df["High"], df["Low"], df["Close"], penetration=0)

    # 45. 頸上線，看跌延續
    df["CDLONNECK"] = talib.CDLONNECK(df["Open"], df["High"], df["Low"], df["Close"])

    # 46. 刺透，看漲反轉
    df["CDLPIERCING"] = talib.CDLPIERCING(df["Open"], df["High"], df["Low"], df["Close"])

    # 47. 人力車夫，猶豫
    df["CDLRICKSHAWMAN"] = talib.CDLRICKSHAWMAN(df["Open"], df["High"], df["Low"], df["Close"])

    # 48. 上升/下降三法，趨勢延續
    df["CDLRISEFALL3METHODS"] = talib.CDLRISEFALL3METHODS(df["Open"], df["High"], df["Low"], df["Close"])

    # 49. 分離線，趨勢延續
    df["CDLSEPARATINGLINES"] = talib.CDLSEPARATINGLINES(df["Open"], df["High"], df["Low"], df["Close"])

    # 50. 射擊之星，看跌
    df["CDLSHOOTINGSTAR"] = talib.CDLSHOOTINGSTAR(df["Open"], df["High"], df["Low"], df["Close"])

    # 51. 短蠟燭，動能弱
    df["CDLSHORTLINE"] = talib.CDLSHORTLINE(df["Open"], df["High"], df["Low"], df["Close"])

    # 52. 旋轉陀螺，市場猶豫
    df["CDLSPINNINGTOP"] = talib.CDLSPINNINGTOP(df["Open"], df["High"], df["Low"], df["Close"])

    # 53. 停頓形態，可能反轉
    df["CDLSTALLEDPATTERN"] = talib.CDLSTALLEDPATTERN(df["Open"], df["High"], df["Low"], df["Close"])

    # 54. 三明治，可能看漲
    df["CDLSTICKSANDWICH"] = talib.CDLSTICKSANDWICH(df["Open"], df["High"], df["Low"], df["Close"])

    # 55. 探水竿，強烈看漲
    df["CDLTAKURI"] = talib.CDLTAKURI(df["Open"], df["High"], df["Low"], df["Close"])

    # 56. 跳空並列，趨勢延續
    df["CDLTASUKIGAP"] = talib.CDLTASUKIGAP(df["Open"], df["High"], df["Low"], df["Close"])

    # 57. 插入線，弱反彈續跌
    df["CDLTHRUSTING"] = talib.CDLTHRUSTING(df["Open"], df["High"], df["Low"], df["Close"])

    # 58. 三星，反轉訊號
    df["CDLTRISTAR"] = talib.CDLTRISTAR(df["Open"], df["High"], df["Low"], df["Close"])

    # 59. 特殊三河床，反轉
    df["CDLUNIQUE3RIVER"] = talib.CDLUNIQUE3RIVER(df["Open"], df["High"], df["Low"], df["Close"])

    # 60. 上升缺口兩隻烏鴉，看跌
    df["CDLUPSIDEGAP2CROWS"] = talib.CDLUPSIDEGAP2CROWS(df["Open"], df["High"], df["Low"], df["Close"])

    # 61. 上下跳空三法，趨勢延續
    df["CDLXSIDEGAP3METHODS"] = talib.CDLXSIDEGAP3METHODS(df["Open"], df["High"], df["Low"], df["Close"])
    """
    print(df)
    return df
# %%
