"""Bronze → Silver Spark 배치 (Bronze JSON → Silver parquet).

Medallion Silver 규칙:
  - Bronze는 Kafka Connect GCS Sink가 쓴 평탄 JSON(payload 필드 top-level) + 메타 3개
    (kafka_timestamp / event_time / record_type) — 타입 변환 + 품질 검증
  - Bronze는 **이벤트일**로 파티셔닝(date=YYYY-MM-DD)되어 있고, DAG가 처리 대상 하루치
    폴더만 --bronze-path 로 넘긴다 → 매 배치 전량 스캔 없음
  - EOD 마커(record_type='eod_marker')는 거래가 아니라 완결 신호다. 품질 검사 **앞에서**
    걸러낸다(뒤에 두면 parse_error 로 Quarantine에 쌓이고 삭제도 못 한다)
  - Quarantine 패턴: 불량 데이터 → silver/quarantine/ (삭제 금지)
  - is_suspicious 플래그: isFraud=1 AND isFlaggedFraud=0 (원본 라벨 파생: 기존 룰 미탐지 사기 집계)
  - row_id: SHA-256(nameOrig|step|type|amount|nameDest) — dedup 키
  - partitionOverwriteMode=dynamic → 멱등 재처리

무손실 검증은 여기가 아니라 Airflow `reconcile` 이 한다 — Bronze(품질통과·유니크) ↔ Silver 를
일자별로 대조한다. 예전 이 파일에 있던 "bronze == valid + quarantine" 검사는 reject_reason 의
isNull/isNotNull 이 상보적이라 **항상 참**이어서 아무것도 잡지 못했고, 전량 스캔만 2회 썼다.

실행: Dataproc Serverless(Airflow DAG의 `spark_silver` 태스크)로 제출.
  `--bronze-path`/`--silver-path`는 gs:// 경로 필수(Part2: 로컬 datalake 대체재 제거).
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType


# 설정 소스: CLI 인자(필수) — Dataproc Serverless 제출 시 DAG가 gs:// 경로를 전달.
#   (Part2: 로컬 ./datalake 대체재 제거 — 로컬 경로 폴백/환경변수 기본값 없음. GCS 전용.)
# parse_known_args → Spark 런타임이 붙이는 잉여 인자는 무시.
def _parse_config():
    p = argparse.ArgumentParser(description="Bronze→Silver Spark 배치")
    # --bronze-path 는 이제 **하루치 파티션**을 가리킨다(gs://.../transactions/date=YYYY-MM-DD).
    # Bronze가 이벤트일로 파티셔닝되므로 DAG가 처리 대상 날짜 폴더만 지정한다 →
    # 매 배치 전량(6.36M행) 스캔이 사라진다.
    p.add_argument("--bronze-path",    required=True)
    p.add_argument("--silver-path",    required=True)  # gs://...-silver
    p.add_argument("--target-tx-date", default=os.getenv("TARGET_TX_DATE"))
    args, _ = p.parse_known_args()
    return args


_cfg = _parse_config()
BRONZE_PATH      = _cfg.bronze_path
SILVER_PATH      = _cfg.silver_path
SILVER_QUAR_PATH = SILVER_PATH.rstrip("/") + "/quarantine"
# Model 2(이벤트시간 일별 증분): 지정 시 해당 tx_date 1일치 valid 행만 Silver로 기록.
# 미지정(None)이면 전체 처리 — 기존 동작 유지(하위호환). Airflow가 {{ ds }}를 주입.
TARGET_TX_DATE   = _cfg.target_tx_date  # "YYYY-MM-DD" 또는 None

VALID_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]

# Bronze payload 필드 — Producer가 csv.DictReader로 읽어 str 직렬화, Kafka Connect가
# 평탄 JSON(top-level)으로 그대로 씀(Schema Registry 없이 schemaless JSON 유지).
PAYLOAD_SCHEMA = StructType([
    StructField("step",           StringType()),
    StructField("type",           StringType()),
    StructField("amount",         StringType()),
    StructField("nameOrig",       StringType()),
    StructField("oldbalanceOrg",  StringType()),
    StructField("newbalanceOrig", StringType()),
    StructField("nameDest",       StringType()),
    StructField("oldbalanceDest", StringType()),
    StructField("newbalanceDest", StringType()),
    StructField("isFraud",        StringType()),
    StructField("isFlaggedFraud", StringType()),
])

# E단계: Bronze 전체 스키마 = payload 필드(top-level) + 메타 3개.
#   kafka_timestamp — Kafka Connect SMT(InsertField$Value)가 넣는 Kafka record timestamp
#                     (epoch millis, LongType). 적재 시각.
#   event_time      — producer가 step에서 계산한 이벤트 시각(epoch millis). Connect의
#                     파티셔닝 키이자 tx_timestamp 의 원천.
#   record_type     — NULL이면 거래, 'eod_marker'면 그날 완결 신호. 마커는 거래와 같은
#                     writer를 타야 순서 보장이 성립하므로 Bronze에 섞여 들어온다.
BRONZE_SCHEMA = StructType(
    PAYLOAD_SCHEMA.fields + [
        StructField("kafka_timestamp", LongType()),
        StructField("event_time",      LongType()),
        StructField("record_type",     StringType()),
    ]
)

MARKER_RECORD_TYPE = "eod_marker"


def _reject_reason():
    """품질 검증 표현식. 첫 번째 매칭 조건을 reject_reason으로, 정상은 NULL.

    Bronze가 평탄 JSON(payload 필드 top-level)이라 프리픽스 없이 직접 참조.
    이전(Parquet+from_json 파싱 시절)의 "null_value"(value 컬럼 자체가 없음)와
    "parse_error"(일부 필드 파싱 실패)는 평탄 스키마에서 더 이상 구분되지 않아
    parse_error로 통합(다른 곳에서 "null_value" 문자열 참조 없음을 grep으로 확인).
    """
    return (
        F.when(
            F.col("step").isNull()     | F.col("amount").isNull()   |
            F.col("nameOrig").isNull() | F.col("type").isNull()    |
            F.col("nameDest").isNull(),
            "parse_error",
        )
         # event_time 은 tx_timestamp/tx_date 의 유일한 원천이다. NULL이면 tx_date가 NULL이 되어
         # __HIVE_DEFAULT_PARTITION__ 으로 조용히 새므로, 격리해서 눈에 보이게 만든다.
         # (producer 없이 적재된 옛 데이터나 event_time 주입 누락을 잡는다.)
         .when(F.col("event_time").isNull(), "missing_event_time")
         .when(
             F.col("amount").cast("double").isNull() |
             (F.col("amount").cast("double") <= 0),
             "invalid_amount",
         )
         .when(
             F.col("step").cast("int").isNull()  |
             (F.col("step").cast("int") <= 0)    |
             (F.col("step").cast("int") > 743),
             "invalid_step",
         )
         .when(~F.col("type").isin(VALID_TYPES), "invalid_type")
         .when(~F.col("isFraud").isin("0", "1"), "invalid_flag")
         .when(~F.col("isFlaggedFraud").isin("0", "1"), "invalid_flag")
         .when(F.col("oldbalanceOrg").cast("double") < 0, "negative_balance")
         .when(F.col("newbalanceOrig").cast("double") < 0, "negative_balance")
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("batch_silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Bronze 읽기 (평탄 JSON, 명시 스키마) ────────────────────────
    # BRONZE_PATH는 하루치 파티션(date=YYYY-MM-DD)이라 전량 스캔이 아니다.
    bronze_df    = spark.read.schema(BRONZE_SCHEMA).json(BRONZE_PATH)

    # ── Step 2: EOD 마커 분리 — 반드시 품질 검사 **앞**에서 ─────────────────
    # 마커는 거래가 아니라 "그날 데이터가 다 왔다"는 신호다. 품질 검사에 넘기면 필수 필드가
    # 없어 parse_error 로 Quarantine에 쌓이고, 프로젝트 규칙상 삭제할 수도 없다.
    tx_df        = bronze_df.filter(F.col("record_type").isNull())
    marker_count = bronze_df.filter(F.col("record_type") == MARKER_RECORD_TYPE).count()
    bronze_count = tx_df.count()
    print(f"[silver] bronze 읽기: 거래 {bronze_count}행 (EOD 마커 {marker_count}건 제외)")

    # ── Step 3: valid / quarantine 분리 (payload 필드가 이미 top-level) ─────
    # (이전의 "bronze == valid + quarantine" 검사는 제거했다. reject_reason 의 isNull/
    #  isNotNull 이 상보적이라 **수학적으로 항상 참**이었고 — 발동할 수 있는 경로가 없다 —
    #  대신 전량 스캔 2회를 쓰고 있었다. 실제 무손실 검증은 Airflow `reconcile` 태스크가
    #  Bronze(품질통과·유니크) ↔ Silver 를 일자별로 대조해 수행한다.)
    labeled    = tx_df.withColumn("reject_reason", _reject_reason())
    valid_raw  = labeled.filter(F.col("reject_reason").isNull())
    quarantine = labeled.filter(F.col("reject_reason").isNotNull())

    # ── Step 4: Silver 컬럼 변환 ─────────────────────────────────────────
    # tx_timestamp: producer가 step에서 계산해 실어 보낸 event_time(epoch millis).
    #   예전엔 여기서 STEP_EPOCH + (step-1)h 를 다시 계산했지만, 이제 기준시각을 아는 곳은
    #   producer 하나다. Spark가 따로 계산하면 Connect의 date= 파티션(=event_time 기반)과
    #   Silver tx_date가 어긋날 수 있다 — 같은 값에서 파생시켜 그 여지를 없앤다.
    #   (step ↔ event_time 의 일치 자체는 Airflow reconcile 이 교차검증한다.)
    tx_ts = F.timestamp_millis(F.col("event_time"))
    # kafka_timestamp: epoch millis(Long) → Spark timestamp
    kafka_ts = F.timestamp_millis(F.col("kafka_timestamp"))

    # row_id: Spark 재처리 중복 방지용 SHA-256 해시
    row_id = F.sha2(
        F.concat_ws("|",
            F.col("nameOrig"), F.col("step"),
            F.col("type"),     F.col("amount"),
            F.col("nameDest"),
        ), 256
    )

    is_susp = (
        (F.col("isFraud").cast("int") == 1) &
        (F.col("isFlaggedFraud").cast("int") == 0)
    )

    dec = DecimalType(18, 2)
    silver_df = valid_raw.select(
        row_id.alias("row_id"),
        F.col("step").cast("int").alias("step"),
        tx_ts.alias("tx_timestamp"),
        F.to_date(tx_ts).alias("tx_date"),      # 파티션 키 (2016-01-xx)
        kafka_ts.alias("kafka_timestamp"),       # 적재 시각 (운영 모니터링용)
        F.col("type").alias("type"),
        F.col("amount").cast(dec).alias("amount"),
        F.col("nameOrig").alias("nameOrig"),
        F.col("oldbalanceOrg").cast(dec).alias("oldbalanceOrg"),
        F.col("newbalanceOrig").cast(dec).alias("newbalanceOrig"),
        F.col("nameDest").alias("nameDest"),
        F.col("oldbalanceDest").cast(dec).alias("oldbalanceDest"),
        F.col("newbalanceDest").cast(dec).alias("newbalanceDest"),
        F.col("isFraud").cast("int").alias("isFraud"),
        F.col("isFlaggedFraud").cast("int").alias("isFlaggedFraud"),
        is_susp.alias("is_suspicious"),
    )

    # ── Step 4b: Model 2 — 지정된 tx_date 1일치만 선택 (멱등 일별 증분) ──
    # BRONZE_PATH 가 이미 하루치 파티션이라 대부분 걸릴 게 없지만, event_time 과 폴더가
    # 어긋난 행(잘못 적재)을 걸러내는 안전망으로 남긴다 → dynamic overwrite가 그 파티션만 덮어씀.
    if TARGET_TX_DATE:
        silver_df = silver_df.filter(
            F.col("tx_date") == F.to_date(F.lit(TARGET_TX_DATE))
        )
        print(f"[silver] TARGET_TX_DATE={TARGET_TX_DATE} → 해당 일자만 기록")

    # ── Step 5: dedup (Kafka Connect at-least-once로 인한 Bronze 중복 흡수) ──
    pre_dedup_count = silver_df.count()          # (날짜 필터 적용 후) 기록 후보 수
    silver_df       = silver_df.dropDuplicates(["row_id"])
    silver_count    = silver_df.count()
    dedup_removed   = pre_dedup_count - silver_count

    # ── Step 6: Silver 저장 (dynamic overwrite, 멱등) ────────────────────
    silver_df.write \
        .mode("overwrite") \
        .partitionBy("tx_date") \
        .parquet(SILVER_PATH)

    # ── Step 7: Quarantine 저장 (dynamic overwrite, 멱등) ────────────────
    # 원본 payload 필드를 JSON으로 재구성해 value 컬럼으로 보존(평탄 스키마라 원본
    # value 문자열 컬럼이 더 이상 없음) + reject_reason + ingest_date 파티션.
    payload_cols = [f.name for f in PAYLOAD_SCHEMA.fields]
    quarantine \
        .select(
            F.to_json(F.struct(*payload_cols)).alias("value"),
            kafka_ts.alias("kafka_timestamp"),
            "reject_reason",
        ) \
        .withColumn("ingest_date", F.to_date(F.col("kafka_timestamp"))) \
        .write \
        .mode("overwrite") \
        .partitionBy("ingest_date") \
        .parquet(SILVER_QUAR_PATH)

    # ── Step 8: 완료 출력 ────────────────────────────────────────────────
    valid_raw_count  = valid_raw.count()
    quar_count       = quarantine.count()
    suspicious_count = silver_df.filter("is_suspicious").count()
    print(f"[silver] bronze_tx={bronze_count}  eod_markers={marker_count}")
    print(f"[silver] valid_raw={valid_raw_count}  quarantine={quar_count}  dedup_removed={dedup_removed}")
    print(f"[silver] silver_written={silver_count}  is_suspicious={suspicious_count}")
    print(f"[silver] 완료 → {SILVER_PATH}")


if __name__ == "__main__":
    main()
