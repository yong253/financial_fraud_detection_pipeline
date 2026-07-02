# dbt(Gold) 실행용 경량 이미지 — BigQuery 어댑터(단일 웨어하우스).
# requirements.txt 와 동일 버전 고정. 리눅스라 cp949 이슈 없음(PYTHONUTF8 불필요).
FROM python:3.11-slim

# dbt-core 명시 고정 필수 — 미지정 시 pip이 최신(1.11)을 끌어와 1.8 어댑터와 불일치 발생.
RUN pip install --no-cache-dir dbt-core==1.8.7 dbt-bigquery==1.8.3

WORKDIR /dbt
ENV PYTHONUTF8=1
ENTRYPOINT ["dbt"]
