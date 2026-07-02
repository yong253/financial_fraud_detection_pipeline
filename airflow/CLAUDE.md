# airflow/ — 배치 오케스트레이션

배치 흐름 (Airflow가 GCP 리소스 제어, Cloud Composer 미사용):

```
Bronze → Spark Batch(Dataproc) → Silver → DBT → Gold → Grafana
```

- **DAG 스코프 = 배치만(Silver→Gold).** Kafka→Bronze 적재는 DAG 밖(상시 인제스트).
  DAG가 Bronze 스토리지에서 출발하므로 수집 방식 교체와 무관(불변).
- **처리 모델 = 이벤트시간(tx_date) 일별 증분:** DAG run 1개 = 하루치(`{{ ds }}`), catchup 백필.
- 태스크: `bronze_sensor → spark_silver({{ds}}) → dbt_run → dbt_test → reconcile → push_metrics`
  - `spark_silver`: DataprocCreateBatchOperator(Serverless). `upload_spark_code`가 코드→GCS 동기화.
  - `reconcile`: **undetected_fraud == silver is_suspicious** 등식 검증, 불일치 시 DAG 실패
    (검증 절차·명령은 `verify-reconciliation` 스킬 참조).
  - `push_metrics`: 배치 records + 사기 KPI를 Pushgateway로 push(모니터링은 `prometheus/CLAUDE.md`).

파일: `dags/fraud_pipeline_dag.py`.
멱등 태스크 원칙 등 상위 정합성 규칙은 루트 `CLAUDE.md`·`TODO.md` 참조.
