# dbt/ — Gold 레이어 (BigQuery)

Medallion **Gold** 레이어 (dbt-bigquery, 데이터셋 `fraud_gold`):

- `hourly_summary`: 시간대별 거래량/사기 건수 집계
- `undetected_fraud`: 기존 룰 미탐지 사기 상세 집계 (`isFraud=1 & isFlaggedFraud=0`)
- `account_risk`: 계좌별 누적 위험도 점수

구성:
- `models/staging/` — `stg_silver_transactions`(BQ source 단일), `sources.yml`(silver source)
- `models/gold/` — 위 3모델 + `schema.yml`(테스트)
- `profiles.yml` — BigQuery 단일(target=prod). 실행:
  `docker compose -f docker/docker-compose.yml --env-file .env run --rm dbt run --profiles-dir .`

규칙:
- **DBT 모델에는 `not_null`, `unique` 테스트를 반드시 작성한다.** (루트 개발 규칙)
- 크로스-DB 잔재 주의: BQ 방언 사용(`dbt.date_trunc`, `::DOUBLE` 금지). DuckDB는 제거됨(BigQuery 단일).

정합성 검증 절차는 `verify-reconciliation` 스킬 참조.
