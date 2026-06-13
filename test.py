from kafka import KafkaProducer

# try:
#     producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
#     print("✅ 連線成功")
#     producer.close()
# except Exception as e:
#     print(f"❌ 連線失敗: {e}")

import pandas as pd
import yfinance as yf

ticker = "AAPL"
stock = yf.Ticker(ticker)
df = stock.history(period='max')

# 1.將 index 變成普通欄位，並自動生成新的整數 index。
df = df.reset_index()
# 2. 將日期轉換為 datetime 類型
df["Date"] = pd.to_datetime(df["Date"]).dt.date

print(df)