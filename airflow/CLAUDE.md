# airflow/ — 배치 오케스트레이션

배치 흐름 (Airflow가 GCP 리소스 제어, Cloud Composer 미사용):

```
Bronze → Spark Batch(Dataproc) → Silver → DBT → Gold → Grafana
```

- **DAG 스코프 = 배치만(Silver→Gold).** Kafka→Bronze 적재는 DAG 밖(상시 인제스트).
  DAG가 Bronze 스토리지에서 출발하므로 수집 방식 교체와 무관(불변).
- **처리 모델 = 이벤트시간(tx_date) 일별 증분:** DAG run 1개 = 하루치(`{{ ds }}`), catchup 백필.
- 태스크: `bronze_sensor`와 `upload_spark_code`가 **병렬**로 `spark_silver`에 들어간 뒤
  `→ dbt_run → dbt_test → reconcile → push_metrics` (센서가 데이터 도착을 기다리는 동안
  코드 업로드가 끝나므로 대기 시간을 낭비하지 않는다).
  - `spark_silver`: DataprocCreateBatchOperator(Serverless). `upload_spark_code`가 코드→GCS 동기화.
    인자는 `--bronze-path`/`--silver-path`/`--target-tx-date` 3개. **`--step-epoch`는 넘기지
    않는다** — batch_silver가 tx_date를 event_time에서 직접 파생한다(`spark/CLAUDE.md` 참조).
  - `reconcile`: **Bronze→Silver 무손실·무중복 검증** — Bronze(품질통과·`row_id` 유니크) 행수
    == Silver 행수를 이벤트일별로 대조(`tx_date <= ds`), 불일치 시 DAG 실패. Bronze 쪽 집계는
    Spark 코드와 별개로 SQL에 미러링한 독립 교차검증이고, 날짜도 `date=` 파티션이 아니라
    `step`에서 재계산한다(같은 출처끼리 비교하면 교차검증이 성립하지 않으므로).
    `STEP_EPOCH`는 이 SQL에서만 쓰인다.
  - `push_metrics`: 배치 records + 사기 KPI를 Pushgateway로 push(모니터링은 `prometheus/CLAUDE.md`).

파일: `dags/fraud_pipeline_dag.py`.
멱등 태스크 원칙 등 상위 정합성 규칙은 루트 `CLAUDE.md`·`TODO.md` 참조.
