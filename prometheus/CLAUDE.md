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
docker compose -f docker/docker-compose.yml --profile monitoring up -d
```

포트: Prometheus 9090 / Pushgateway 9091 / Grafana 3000(admin/admin) / kafka-exporter 9308 / statsd-exporter 9102

파일:
- `prometheus/prometheus.yml` — 스크레이프 설정(pushgateway/kafka/airflow)
- `prometheus/statsd_mapping.yml` — Airflow StatsD → Prometheus 라벨 매핑
- `grafana/dashboards/` — `fraud_overview.json`(대시보드 모델)
- `grafana/provisioning/` — 데이터소스/대시보드 자동 등록
