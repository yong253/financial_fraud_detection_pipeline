# Financial Fraud Detection Pipeline

PaySim 기반 금융 거래 사기 탐지 데이터 파이프라인. 포트폴리오 프로젝트.

**핵심 스토리:** `isFraud=1 & isFlaggedFraud=0` — 기존 룰 기반 시스템이 탐지하지 못한 사기 거래를 파이프라인으로 식별.

---

## 기술 스택

| 역할 | 기술 |
|------|------|
| 메시지 큐 | Apache Kafka |
| 스트리밍 | Spark Structured Streaming |
| 배치 | Apache Spark |
| 오케스트레이션 | Apache Airflow |
| Data Lake | GCS (Medallion Architecture) |
| Data Warehouse | BigQuery |
| 데이터 모델링 | DBT |
| 모니터링 | Prometheus + Grafana |
| 인프라 | Docker Compose (로컬) |
| CI/CD | GitHub Actions |
| 언어 | Python |

---

## 세부 규칙 위치 (컴포넌트별 nested CLAUDE.md)

상세는 각 디렉토리에서 작업할 때 로딩된다:

| 디렉토리 | 내용 |
|------|------|
| `kafka/CLAUDE.md` | 수집 흐름(CSV→Kafka→Bronze) + 과도기 |
| `spark/CLAUDE.md` | Silver 레이어 상세(변환·품질검증·Quarantine·is_suspicious) |
| `dbt/CLAUDE.md` | Gold 3모델 + DBT 테스트 규칙 |
| `airflow/CLAUDE.md` | 배치 흐름·DAG 스코프·이벤트시간 일별 증분 |
| `prometheus/CLAUDE.md` | 모니터링 스택·기동·포트 |

정합성 검증 절차: `verify-reconciliation` 스킬.

---

## 데이터셋

- 파일: `Synthetic_Financial_datasets_log.csv` (경로: `data/raw/`)
- 규모: 630만 건, ~500MB
- 컬럼: `step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud`
- 사기는 `CASH_OUT`, `TRANSFER` 유형에서만 발생

---

## Medallion 레이어 (요약)

- **Bronze (GCS)** — Kafka 원본 JSON 그대로. `kafka_timestamp`만 추가 허용. **절대 수정/삭제 금지(append only)**. 파티션 `date=YYYY-MM-DD`.
- **Silver (GCS→BigQuery)** — 품질검증 + Quarantine + `is_suspicious` 플래그. 상세: `spark/CLAUDE.md`.
- **Gold (BigQuery — DBT)** — `hourly_summary`/`undetected_fraud`/`account_risk`. 상세: `dbt/CLAUDE.md`.

---

## 환경변수 (.env)

```
GCP_PROJECT_ID=
GCS_BUCKET_BRONZE=
GCS_BUCKET_SILVER=
GCS_BUCKET_GOLD=
BQ_DATASET_SILVER=fraud_silver
BQ_DATASET_GOLD=fraud_gold
GCP_CREDENTIALS_PATH=./credentials/service_account.json
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions
```

---

## 개발 규칙

- 시크릿/키 파일은 절대 커밋하지 않는다 (`credentials/`, `.env`)
- Bronze 레이어 데이터는 절대 수정하지 않는다
- Quarantine으로 격리된 데이터는 삭제하지 않고 보존한다
- DBT 모델에는 `not_null`, `unique` 테스트를 반드시 작성한다
- 파이프라인은 GCP 기반으로 재현 가능해야 한다 (gcloud + 서비스계정 키). 웨어하우스는 BigQuery 단일 — 로컬 임시 대체재(DuckDB)는 제거됨
- **전 컴포넌트는 무손실 + 무중복으로 설정한다**: Kafka RF=3·min.insync=2·unclean.leader.election=false·멱등 Producer(acks=all), Spark checkpoint·멱등 재처리(partitionOverwrite dynamic)·dedup 키, Airflow 멱등 태스크. 금액은 float 드리프트 방지 위해 문자열/decimal 직렬화. (설정 기준: `TODO.md` "컴포넌트별 정합성 설정")
- **95% 이상 확실하지 않은 결정/가정은 임의로 진행하지 않는다.** 진행 전 사용자에게 확인하고 확정한다.

---

## 진행 상황

`TODO.md` 참고.
