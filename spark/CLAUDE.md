# spark/ — Bronze → Silver 배치

Medallion **Silver** 레이어 규칙 (Bronze → Silver 변환):

- `step` → `timestamp` 변환 (step = 1시간 단위)
- 데이터 품질 검증 (null, amount < 0, 잘못된 type 등)
- **Quarantine 패턴**: 불량 데이터는 `silver/quarantine/` 로 격리, 메인 Silver에서 제외
  (격리 데이터는 삭제하지 않고 보존 — 루트 개발 규칙)
- `is_suspicious` 플래그 추가: **`isFraud=1 AND isFlaggedFraud=0`** (핵심 스토리 = 미탐지 사기)

파일:
- `batch_silver.py` — Bronze → Silver (Quarantine 포함). argparse 지원(`--bronze-path`, `--silver-path`,
  `--target-tx-date`, env 폴백). 로컬 docker Spark / Dataproc Serverless 공용(경로만 인자로 분기).
- `streaming_bronze.py` — Kafka → Bronze (과도기 passthrough, Part2에서 제거 예정).

멱등: Spark checkpoint + 동적 파티션 덮어쓰기(partitionOverwrite dynamic) + dedup 키.
금액은 float 드리프트 방지 위해 문자열/decimal 직렬화. (상위 정합성 규칙은 루트 `CLAUDE.md`·`TODO.md`.)
