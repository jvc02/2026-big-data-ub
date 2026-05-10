FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends tree && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "pyiceberg[pyarrow,duckdb,sql-sqlite]" \
    duckdb \
    pyarrow \
    pandas \
    jupyter

EXPOSE 8888
