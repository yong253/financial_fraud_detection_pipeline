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

## 아키텍처

```
수집 흐름 (Bronze 적재):
CSV → Kafka Producer → Kafka (topic: transactions) → Spark Streaming → Bronze
  ※ 과도기: 현재 Spark Streaming(사실상 passthrough)으로 적재.
    GCP 전환 시 Kafka Connect GCS Sink로 교체 예정(전용 도구, 객체스토리지 내구성).

배치 흐름 (Airflow 오케스트레이션, ⑤):
Bronze → Spark Batch → Silver → DBT → Gold → Grafana
  ※ DAG 스코프 = 배치만(Silver→Gold). Kafka→Bronze 적재는 DAG 밖(상시 인제스트).
    DAG가 Bronze 스토리지에서 출발하므로 위 적재 방식 교체와 무관(불변).
  ※ 처리 모델 = 이벤트시간(tx_date) 일별 증분: DAG run 1개 = 하루치({{ ds }}), catchup 백필.

모니터링:
Kafka/Spark metrics → Prometheus → Grafana
```

---

## 디렉토리 구조

```
financial_fraud_detection_pipeline/
├── kafka/
│   ├── producer.py          # CSV → Kafka 발행
│   └── config.py
├── spark/
│   ├── streaming_bronze.py  # Kafka → GCS Bronze
│   └── batch_silver.py      # Bronze → Silver (Quarantine 포함)
├── airflow/
│   └── dags/
│       └── fraud_pipeline_dag.py
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   │       ├── hourly_summary.sql
│   │       ├── undetected_fraud.sql
│   │       └── account_risk.sql
│   └── profiles.yml
├── grafana/
│   └── dashboards/
├── docker/
│   └── docker-compose.yml
├── data/
│   └── raw/                 # PaySim CSV (gitignore)
├── credentials/             # GCP 서비스 계정 키 (gitignore)
├── .env                     # 환경변수 (gitignore)
├── .env.example
├── requirements.txt
├── TODO.md
└── CLAUDE.md
```

---

## 데이터셋

- 파일: `Synthetic_Financial_datasets_log.csv` (경로: `data/raw/`)
- 규모: 630만 건, ~500MB
- 컬럼: `step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud`
- 사기는 `CASH_OUT`, `TRANSFER` 유형에서만 발생

---

## Medallion 레이어 규칙

### Bronze (GCS)
- Kafka에서 받은 원본 JSON 그대로 저장
- `kafka_timestamp` 필드만 추가 허용
- **절대 수정/삭제 금지** — 항상 append only
- 파티션: `date=YYYY-MM-DD`

### Silver (GCS → BigQuery)
- `step` → `timestamp` 변환 (step = 1시간 단위)
- 데이터 품질 검증 (null, amount < 0, 잘못된 type 등)
- **Quarantine 패턴**: 불량 데이터는 `silver/quarantine/` 로 격리, 메인 Silver에서 제외
- `is_suspicious` 플래그 추가: `isFraud=1 AND isFlaggedFraud=0`

### Gold (BigQuery — DBT)
- `hourly_summary`: 시간대별 거래량/사기 건수 집계
- `undetected_fraud`: 미탐지 사기 상세 (핵심 테이블)
- `account_risk`: 계좌별 누적 위험도 점수

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
- Docker Compose로 로컬에서 전체 파이프라인이 재현 가능해야 한다
- **전 컴포넌트는 무손실 + 무중복으로 설정한다**: Kafka RF=3·min.insync=2·unclean.leader.election=false·멱등 Producer(acks=all), Spark checkpoint·멱등 재처리(partitionOverwrite dynamic)·dedup 키, Airflow 멱등 태스크. 금액은 float 드리프트 방지 위해 문자열/decimal 직렬화. (설정 기준: `TODO.md` "컴포넌트별 정합성 설정")
- **95% 이상 확실하지 않은 결정/가정은 임의로 진행하지 않는다.** 진행 전 사용자에게 확인하고 확정한다.

---

## 진행 상황

`TODO.md` 참고.
