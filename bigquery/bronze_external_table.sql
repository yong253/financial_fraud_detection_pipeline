-- Bronze External Table — Kafka Connect GCS Sink(JSON)를 복사 없이 BigQuery 테이블로 노출.
-- date는 Hive 파티션(폴더명 date=YYYY-MM-DD, Kafka record timestamp=인제스트일 기준)에서
-- DATE로 선언, payload 필드는 명시 스키마(producer가 전부 문자열로 직렬화).
-- kafka_timestamp는 Kafka Connect SMT(InsertField$Value)가 넣는 epoch millis(INT64) — 실측 후
-- 다른 타입으로 나오면(예: ISO 문자열) 이 파일의 타입을 맞춰 재생성한다.
-- uris를 topics/transactions/date=* 로 스코프 → 버킷 루트의 다른 토픽/오브젝트 배제.
-- 재생성: bq.cmd query --use_legacy_sql=false < bigquery/bronze_external_table.sql
CREATE OR REPLACE EXTERNAL TABLE `financial-pipeline-501007.fraud_bronze.bronze_transactions`
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
  kafka_timestamp INT64
)
WITH PARTITION COLUMNS (
  date DATE
)
OPTIONS (
  format = 'JSON',
  hive_partition_uri_prefix = 'gs://financial-pipeline-501007-bronze/topics/transactions',
  uris = ['gs://financial-pipeline-501007-bronze/topics/transactions/date=*']
);
