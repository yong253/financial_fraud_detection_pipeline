-- Bronze External Table — Kafka Connect GCS Sink(JSON)를 복사 없이 BigQuery 테이블로 노출.
-- date는 Hive 파티션(폴더명 date=YYYY-MM-DD)에서 DATE로 선언. **이벤트일(거래일)**이며
-- 인제스트일이 아니다 — producer가 넣는 event_time을 커넥터가 RecordField로 읽어 나눈다.
-- payload 필드는 명시 스키마(producer가 전부 문자열로 직렬화).
-- kafka_timestamp는 Kafka Connect SMT(InsertField$Value)가 넣는 epoch millis(INT64) — 실측 후
-- 다른 타입으로 나오면(예: ISO 문자열) 이 파일의 타입을 맞춰 재생성한다.
-- uris를 topics/transactions/date=* 로 스코프 → 버킷 루트의 다른 토픽/오브젝트 배제.
-- 플레이스홀더는 .env 값으로 치환해서 실행한다(리터럴 프로젝트 ID를 커밋하지 않기 위함).
-- 재생성: set -a && . ./.env && set +a
--         envsubst < bigquery/bronze_external_table.sql | bq query --use_legacy_sql=false
--         (Windows Git Bash 에서 bq 가 Python 스텁에 걸리면 bq.cmd 사용)
CREATE OR REPLACE EXTERNAL TABLE `${GCP_PROJECT_ID}.${BQ_DATASET_BRONZE}.bronze_transactions`
(
  step            STRING,
  type            STRING,
  amount          STRING,
  nameOrig        STRING,
  oldbalanceOrg   STRING,
  newbalanceOrig  STRING,
  nameDest        STRING,
  oldbalanceDest  STRING,
  newbalanceDest  STRING,
  isFraud         STRING,
  isFlaggedFraud  STRING,
  kafka_timestamp INT64,
  -- producer가 step에서 계산한 이벤트 시각(epoch millis). 커넥터의 파티셔닝 키.
  event_time      INT64,
  -- NULL이면 거래, 'eod_marker'면 그날 완결 신호(파티션당 1건). 마커는 거래 스트림 안에
  -- 실려 오므로(같은 writer를 타야 순서 보장이 성립) Bronze에 섞인다 — 소비하는 쪽에서
  -- 명시적으로 걸러야 한다. `bronze_sensor`는 이 값이 'eod_marker'인 행만 세어 완결 판정.
  record_type     STRING,
  -- ↓ EOD 마커 전용 필드(거래 행에서는 전부 NULL).
  --   tx_date          : 이 마커가 완결을 알리는 이벤트일
  --   kafka_partition  : 어느 파티션이 끝났는지 (DISTINCT 세어 완결 판정)
  --   total_partitions : 그날 기준 토픽 파티션 수 — 센서가 하드코딩 없이 판정을 닫게 해준다
  tx_date          STRING,
  kafka_partition  INT64,
  total_partitions INT64
)
WITH PARTITION COLUMNS (
  date DATE
)
OPTIONS (
  format = 'JSON',
  hive_partition_uri_prefix = 'gs://${GCS_BUCKET_BRONZE}/topics/transactions',
  uris = ['gs://${GCS_BUCKET_BRONZE}/topics/transactions/date=*']
);
