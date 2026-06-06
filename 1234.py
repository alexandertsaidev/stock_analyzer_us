import requests

API_KEY = "d8gpmchr01qhjpmp42mgd8gpmchr01qhjpmp42n0"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

for symbol in SYMBOLS:
    res = requests.get(
        "https://finnhub.io/api/v1/quote",
        params={"symbol": symbol, "token": API_KEY}
    )
    print(f"{symbol}: {res.status_code} {res.json()}")