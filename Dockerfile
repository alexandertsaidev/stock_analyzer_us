FROM python:3.11-slim
WORKDIR /app

# 設定 PYTHONPATH
ENV PYTHONPATH=/app/stock_analyzer_us

# 安裝 Python 套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製整個專案
COPY . /app/stock_analyzer_us/

# 確保可 import
RUN touch /app/stock_analyzer_us/__init__.py

# 開發測試用
CMD ["bash"]

# 生產環境用（擇一取消註解）
# CMD ["python", "scripts/fetch_stock.py"]
# CMD ["python", "scheduler.py"]