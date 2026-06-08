# test_fetcher.py
import asyncio
from history_fetcher import PriceFetcher, fetch_all

# 單一 ticker
result = PriceFetcher("AAPL").fetch()
print(result["status"], result["df"].shape)

tickers = [
    # 原始有效
    "AAPL", "MSFT",

    # 科技
    "NVDA", "GOOGL", "GOOG", "META", "AMZN", "TSLA", "AVGO", "ORCL",
    "AMD", "INTC", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC",
    "ADI", "MRVL", "NOW", "CRM", "ADBE", "SNPS", "CDNS", "PANW",
    "FTNT", "CRWD", "ZS", "OKTA", "DDOG",

    # 金融
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW", "AXP", "COF",
    "USB", "PNC", "TFC", "CME", "ICE",

    # 醫療
    "UNH", "JNJ", "LLY", "ABBV", "MRK", "PFE", "TMO", "ABT", "DHR",
    "MDT", "ISRG", "VRTX", "REGN", "GILD", "BMY",

    # 消費 / 零售
    "WMT", "HD", "COST", "TGT", "LOW", "MCD", "SBUX", "NKE", "TJX",

    # 工業 / 能源
    "XOM", "CVX", "COP", "SLB", "GE", "HON", "CAT", "DE", "LMT", "RTX",
    "UPS", "FDX", "NSC", "UNP",

    # 電信 / 公用
    "VZ", "T", "TMUS",

    # 其他大型股
    "BRK-B", "V", "MA", "PG", "KO", "PEP", "PM", "MO",
]

# 多 ticker（含無效 ticker 測試失敗路徑）
success, retry, failed = asyncio.run(fetch_all(tickers))
print(f"success={len(success)}\nfailed={len(failed)}\nretry={len(retry)}")

# for s in success:
#     print(s)
# for r in retry :
#     print(r)
# for f in failed:
#     print(f)