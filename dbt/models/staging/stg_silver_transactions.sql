{{ config(materialized='view') }}

-- Silver를 노출하는 staging view. Gold 모델은 ref()로 이 모델을 참조한다.
-- Silver = BigQuery External Table(fraud_silver.silver_transactions, GCS parquet 위).
-- quarantine 제외는 외부테이블 uris(tx_date=*)에서 이미 처리됨
--   (Medallion 규칙: 불량 격리 데이터는 Gold 집계에서 배제. 기존 룰 미탐지 사기 집계 오염 방지).
SELECT * FROM {{ source('silver', 'silver_transactions') }}
