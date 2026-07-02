-- Silver External Table — GCS parquet를 복사 없이 BigQuery 테이블로 노출.
-- tx_date는 Hive 파티션(폴더명 tx_date=YYYY-MM-DD)에서 DATE로 선언, 나머지 컬럼은 parquet 스키마 자동감지.
-- uris를 tx_date=*/ 로 스코프 → 버킷 루트의 _SUCCESS·quarantine/ 배제
--   (dbt stg_silver_transactions 의 read_parquet('.../tx_date=*/**/*.parquet') 와 동일한 배제 규칙).
-- 재생성: bq.cmd query --use_legacy_sql=false < bigquery/silver_external_table.sql
CREATE OR REPLACE EXTERNAL TABLE `financial-pipeline-501007.fraud_silver.silver_transactions`
WITH PARTITION COLUMNS (
  tx_date DATE
)
OPTIONS (
  format = 'PARQUET',
  hive_partition_uri_prefix = 'gs://financial-pipeline-501007-silver',
  uris = ['gs://financial-pipeline-501007-silver/tx_date=*']
);
