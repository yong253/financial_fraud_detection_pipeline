-- Silver External Table — GCS parquet를 복사 없이 BigQuery 테이블로 노출.
-- tx_date는 Hive 파티션(폴더명 tx_date=YYYY-MM-DD)에서 DATE로 선언, 나머지 컬럼은 parquet 스키마 자동감지.
-- uris를 tx_date=*/ 로 스코프 → 버킷 루트의 _SUCCESS·quarantine/ 배제
--   (dbt stg_silver_transactions 의 read_parquet('.../tx_date=*/**/*.parquet') 와 동일한 배제 규칙).
-- 컬럼을 선언하지 않고 parquet 스키마를 자동 감지하므로, tx_date=* 에 파일이 하나도 없으면
--   "matched no files" 로 실패한다 → **첫 Silver 적재 이후에** 실행할 것.
-- 플레이스홀더는 .env 값으로 치환해서 실행한다(리터럴 프로젝트 ID를 커밋하지 않기 위함).
-- 재생성: set -a && . ./.env && set +a
--         envsubst < bigquery/silver_external_table.sql | bq query --use_legacy_sql=false
--         (Windows Git Bash 에서 bq 가 Python 스텁에 걸리면 bq.cmd 사용)
CREATE OR REPLACE EXTERNAL TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_SILVER}.silver_transactions`
WITH PARTITION COLUMNS (
  tx_date DATE
)
OPTIONS (
  format = 'PARQUET',
  hive_partition_uri_prefix = 'gs://${GCS_BUCKET_SILVER}',
  uris = ['gs://${GCS_BUCKET_SILVER}/tx_date=*']
);
