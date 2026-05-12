FROM apache/airflow:2.10.2

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root

COPY requirements.txt .

RUN uv pip install --no-cache-dir --system -r requirements.txt

USER airflow
