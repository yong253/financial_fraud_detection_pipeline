# kafka/ — 수집 (Bronze 적재)

수집 흐름:

```
CSV → Kafka Producer → Kafka (topic: transactions) → Kafka Connect GCS Sink → Bronze(GCS)
```

- Kafka Connect GCS Sink(Confluent, `docker/docker-compose.yml`의 `kafka-connect`/
  `connect-init` 서비스, profile=`connect`)로 Bronze(GCS)에 상시 적재 — E단계 완료.
  설정: `kafka/connect/Dockerfile`, `kafka/connect/gcs-sink-bronze.json`.
- 수집은 **DAG 밖 상시 인제스트**다(배치 DAG 스코프 아님). DAG는 Bronze 스토리지(GCS/BQ)에서
  출발하므로 위 적재 방식 교체와 무관(불변).

파일: `producer.py`(CSV → Kafka 발행), `config.py`, `connect/`(Kafka Connect GCS Sink 설정),
`Dockerfile`(producer 컨테이너 이미지 — 코드는 bind-mount, `requirements.txt` 버전 고정).

Producer 실행:
- 호스트: `python kafka/producer.py --limit 1000`
- 컨테이너: `docker compose -f docker/docker-compose.yml --profile producer run --rm
  producer python producer.py --realtime --done-marker gs://...` (profile 지정, 기본 `up`엔
  안 뜸 — CSV 발행은 명시적 트리거). 컨테이너 내부 Kafka 주소는 `kafka1:29092,...`로 자동
  오버라이드됨.

※ 상위 불변 규칙(무손실·무중복, Bronze append-only 등)은 루트 `CLAUDE.md` 참조.
