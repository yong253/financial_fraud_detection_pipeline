# kafka/ — 수집 (Bronze 적재)

수집 흐름:

```
CSV → Kafka Producer → Kafka (topic: transactions) → Spark Streaming → Bronze
```

- **과도기:** 현재 Spark Streaming(사실상 passthrough)으로 Bronze 적재.
  GCP 전환 시 **Kafka Connect GCS Sink**로 교체 예정(전용 도구, 객체스토리지 내구성).
- 수집은 **DAG 밖 상시 인제스트**다(배치 DAG 스코프 아님). DAG는 Bronze 스토리지에서 출발하므로
  위 적재 방식 교체와 무관(불변).

파일: `producer.py`(CSV → Kafka 발행), `config.py`.

※ 상위 불변 규칙(무손실·무중복, Bronze append-only 등)은 루트 `CLAUDE.md` 참조.
