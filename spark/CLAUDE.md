# spark/ — Bronze → Silver 배치

Medallion **Silver** 레이어 규칙 (Bronze → Silver 변환):

- `step` → `timestamp` 변환 (step = 1시간 단위)
- 데이터 품질 검증 (null, amount < 0, 잘못된 type 등)
- **Quarantine 패턴**: 불량 데이터는 `silver/quarantine/` 로 격리, 메인 Silver에서 제외
  (격리 데이터는 삭제하지 않고 보존 — 루트 개발 규칙)
- `is_suspicious` 플래그 추가: **`isFraud=1 AND isFlaggedFraud=0`** (원본 라벨 파생: 기존 룰이
  놓친 사기 — 새 탐지기가 아니라 기존 룰 성능 집계용)

파일:
- `batch_silver.py` — Bronze → Silver (Quarantine 포함). Dataproc Serverless 전용(DAG의
  `spark_silver` 태스크가 제출). 인자는 3개뿐이다: `--bronze-path`/`--silver-path`(gs:// 경로
  필수 — Part2: 로컬 datalake 대체재 제거), `--target-tx-date`(선택).
- **`--step-epoch`는 받지 않는다.** `tx_timestamp`/`tx_date`를 Bronze의 `event_time`에서 직접
  파생하므로 step→시각 변환 기준값이 필요 없다. STEP_EPOCH를 여기로 넘기면 Dataproc엔 해당
  env가 없어 폴백이 발동하고 DAG의 계산과 조용히 어긋난다 — 애초에 의존을 끊었다.
  (STEP_EPOCH는 producer의 event_time 생성과 DAG의 reconcile SQL에서만 쓰인다.)

멱등: Spark checkpoint + 동적 파티션 덮어쓰기(partitionOverwrite dynamic) + dedup 키.
금액은 float 드리프트 방지 위해 문자열/decimal 직렬화. (상위 정합성 규칙은 루트 `CLAUDE.md`·`TODO.md`.)
