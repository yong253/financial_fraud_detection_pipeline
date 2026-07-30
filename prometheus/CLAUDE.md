# prometheus/ + grafana/ — 모니터링 (profile=monitoring)

모니터링 흐름:

```
Kafka 인제스트 → kafka-exporter ─┐
Airflow 운영(StatsD) → statsd-exporter ─┼→ Prometheus → Grafana(단일 데이터소스)
배치 records + 사기 KPI → DAG가 Pushgateway로 push ─┘
```

- Gold(BigQuery)를 Grafana에 직접 붙일 수도 있으나, 상세 행 대신 Top-N/카운트 게이지로 표현하기 위해
  DAG `push_metrics` 태스크가 집계 카운트를 Pushgateway(Prometheus 게이지)로 전송.

기동:
```
docker compose --profile monitoring up -d
```

**모니터링은 진짜 선택이다 — 없어도 파이프라인은 완주한다.** Pushgateway가 안 떠 있으면
`push_metrics`가 `AirflowSkipException`으로 **SKIPPED** 처리되고 DAG run은 성공한다.
(예전엔 이 태스크가 Pushgateway에 하드 의존해서, 모니터링 스택 미기동만으로 31일 백필이
`BackfillUnfinished`로 통째로 죽었다. 관측 실패가 데이터 파이프라인을 멈추면 안 된다.)

다만 SKIPPED는 "메트릭이 안 나갔다"는 뜻이므로, **전량 백필처럼 오래 도는 작업에서 대시보드가
필요하면 `--profile monitoring`을 함께 띄울 것.** 성능 측정(Dataproc DCU-초 / 태스크 소요시간)은
Pushgateway가 아니라 Dataproc API와 Airflow 메타DB에서 나오므로 이것과 무관하다.

포트: Prometheus 9090 / Pushgateway 9091 / Grafana 3000(admin/admin) / kafka-exporter 9308 / statsd-exporter 9102

파일:
- `prometheus/prometheus.yml` — 스크레이프 설정(pushgateway/kafka/airflow)
- `prometheus/statsd_mapping.yml` — Airflow StatsD → Prometheus 라벨 매핑
- `grafana/dashboards/` — `fraud_overview.json`(대시보드 모델)
- `grafana/provisioning/` — 데이터소스/대시보드 자동 등록
