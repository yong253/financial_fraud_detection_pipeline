{{ config(materialized='view') }}

-- Silver Parquet을 DuckDB view로 노출. Gold 모델은 ref()로 이 모델을 참조한다.
-- 🔴 quarantine 제외(필수): glob을 tx_date=*/ 로 한정 → silver/quarantine/ 는 절대 읽지 않음.
--    (Medallion 규칙: 불량 격리 데이터는 Gold 집계에서 배제. 미탐지 사기 스토리 오염 방지)
-- 경로: read_parquet은 dbt 실행 위치(dbt/) 기준 상대경로 → 기본값 ../datalake/silver
SELECT *
FROM read_parquet(
    '{{ env_var("SILVER_PATH", "../datalake/silver") }}/tx_date=*/**/*.parquet',
    hive_partitioning = true
)
