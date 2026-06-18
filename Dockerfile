FROM python:3.11-slim AS dev
WORKDIR /app

ENV PYTHONPATH=/app/stock_analyzer_us

RUN apt-get update && apt-get install -y libta-lib-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock ./
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root

COPY . /app/stock_analyzer_us/
RUN touch /app/stock_analyzer_us/__init__.py

CMD ["bash"]


FROM python:3.11-slim AS prod
WORKDIR /app

ENV PYTHONPATH=/app/stock_analyzer_us

RUN apt-get update && apt-get install -y libta-lib-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock ./
RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root --only main

COPY . /app/stock_analyzer_us/
RUN touch /app/stock_analyzer_us/__init__.py

CMD ["bash"]