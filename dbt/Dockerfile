# dbt(Gold) 실행용 경량 이미지 — DuckDB 어댑터.
# requirements.txt 와 동일 버전 고정. 리눅스라 cp949 이슈 없음(PYTHONUTF8 불필요).
FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-duckdb==1.8.4 duckdb==1.1.0

WORKDIR /dbt
ENV PYTHONUTF8=1
ENTRYPOINT ["dbt"]
