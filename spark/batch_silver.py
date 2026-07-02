"""Bronze → Silver Spark 배치 (parquet → Silver parquet).

Medallion Silver 규칙:
  - Bronze value(JSON string) 파싱 + 타입 변환 + 품질 검증
  - Quarantine 패턴: 불량 데이터 → silver/quarantine/ (삭제 금지)
  - 건수 정합성: Bronze == valid_raw + quarantine (불일치 시 중단)
  - is_suspicious 플래그: isFraud=1 AND isFlaggedFraud=0 (핵심 스토리)
  - row_id: SHA-256(nameOrig|step|type|amount|nameDest) — dedup 키
  - partitionOverwriteMode=dynamic → 멱등 재처리

컨테이너 실행:
  docker compose -f docker/docker-compose.yml run --rm spark-silver
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StructField, StructType, StringType


# 설정 소스 우선순위: CLI 인자(--bronze-path 등) > 환경변수 > 기본값.
#   - 로컬 docker: 환경변수(.env)로 주입 → 인자 없이 그대로 동작(하위호환).
#   - Dataproc Serverless: env 주입이 어려워 스크립트 인자로 전달 (`-- --bronze-path=gs://...`).
# parse_known_args → Spark 런타임이 붙이는 잉여 인자는 무시.
def _parse_config():
    p = argparse.ArgumentParser(description="Bronze→Silver Spark 배치")
    p.add_argument("--bronze-path",    default=os.getenv("BRONZE_PATH", "/datalake/bronze"))
    p.add_argument("--silver-path",    default=os.getenv("SILVER_PATH", "/datalake/silver"))
    p.add_argument("--step-epoch",     default=os.getenv("STEP_EPOCH", "2016-01-01 00:00:00"))
    p.add_argument("--target-tx-date", default=os.getenv("TARGET_TX_DATE"))
    args, _ = p.parse_known_args()
    return args


_cfg = _parse_config()
BRONZE_PATH      = _cfg.bronze_path
SILVER_PATH      = _cfg.silver_path
SILVER_QUAR_PATH = SILVER_PATH.rstrip("/") + "/quarantine"
STEP_EPOCH       = _cfg.step_epoch
# Model 2(이벤트시간 일별 증분): 지정 시 해당 tx_date 1일치 valid 행만 Silver로 기록.
# 미지정(None)이면 전체 처리 — 기존 동작 유지(하위호환). Airflow가 {{ ds }}를 주입.
TARGET_TX_DATE   = _cfg.target_tx_date  # "YYYY-MM-DD" 또는 None

VALID_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]

# Bronze value 안 JSON 필드 — Producer가 csv.DictReader로 읽어 str 직렬화
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


def _reject_reason():
    """품질 검증 표현식. 첫 번째 매칭 조건을 reject_reason으로, 정상은 NULL."""
    p = "payload"
    return (
        F.when(F.col("value").isNull(), "null_value")
         .when(
             F.col(f"{p}.step").isNull()    | F.col(f"{p}.amount").isNull()   |
             F.col(f"{p}.nameOrig").isNull() | F.col(f"{p}.type").isNull()    |
             F.col(f"{p}.nameDest").isNull(),
             "parse_error",
         )
         .when(
             F.col(f"{p}.amount").cast("double").isNull() |
             (F.col(f"{p}.amount").cast("double") <= 0),
             "invalid_amount",
         )
         .when(
             F.col(f"{p}.step").cast("int").isNull()  |
             (F.col(f"{p}.step").cast("int") <= 0)    |
             (F.col(f"{p}.step").cast("int") > 743),
             "invalid_step",
         )
         .when(~F.col(f"{p}.type").isin(VALID_TYPES), "invalid_type")
         .when(~F.col(f"{p}.isFraud").isin("0", "1"), "invalid_flag")
         .when(~F.col(f"{p}.isFlaggedFraud").isin("0", "1"), "invalid_flag")
         .when(F.col(f"{p}.oldbalanceOrg").cast("double") < 0, "negative_balance")
         .when(F.col(f"{p}.newbalanceOrig").cast("double") < 0, "negative_balance")
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

    # ── Step 1: Bronze 읽기 ───────────────────────────────────────────────
    bronze_df    = spark.read.parquet(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"[silver] bronze 읽기: {bronze_count}행")

    # ── Step 2: value JSON 파싱 ───────────────────────────────────────────
    parsed = bronze_df.withColumn(
        "payload", F.from_json(F.col("value"), PAYLOAD_SCHEMA)
    )

    # ── Step 3: valid / quarantine 분리 ──────────────────────────────────
    labeled    = parsed.withColumn("reject_reason", _reject_reason())
    valid_raw  = labeled.filter(F.col("reject_reason").isNull())
    quarantine = labeled.filter(F.col("reject_reason").isNotNull())

    # ── Step 4: 건수 정합성 검증 (저장 전) ──────────────────────────────
    valid_raw_count = valid_raw.count()
    quar_count      = quarantine.count()
    if valid_raw_count + quar_count != bronze_count:
        raise RuntimeError(
            f"[silver] 건수 불일치 — "
            f"bronze={bronze_count}, valid={valid_raw_count}, "
            f"quarantine={quar_count}, 합계={valid_raw_count + quar_count}"
        )
    print(f"[silver] 정합성 OK — valid={valid_raw_count}, quarantine={quar_count}")

    # ── Step 5: Silver 컬럼 변환 ─────────────────────────────────────────
    # tx_timestamp: 2016-01-01 00:00:00 + (step-1) hours
    epoch_unix = F.unix_timestamp(F.lit(STEP_EPOCH))
    tx_ts = F.to_timestamp(
        epoch_unix + (F.col("payload.step").cast("long") - 1) * 3600
    )

    # row_id: Spark 재처리 중복 방지용 SHA-256 해시
    row_id = F.sha2(
        F.concat_ws("|",
            F.col("payload.nameOrig"), F.col("payload.step"),
            F.col("payload.type"),     F.col("payload.amount"),
            F.col("payload.nameDest"),
        ), 256
    )

    is_susp = (
        (F.col("payload.isFraud").cast("int") == 1) &
        (F.col("payload.isFlaggedFraud").cast("int") == 0)
    )

    dec = DecimalType(18, 2)
    silver_df = valid_raw.select(
        row_id.alias("row_id"),
        F.col("payload.step").cast("int").alias("step"),
        tx_ts.alias("tx_timestamp"),
        F.to_date(tx_ts).alias("tx_date"),      # 파티션 키 (2016-01-xx)
        F.col("kafka_timestamp"),                # 적재 시각 (운영 모니터링용)
        F.col("payload.type").alias("type"),
        F.col("payload.amount").cast(dec).alias("amount"),
        F.col("payload.nameOrig").alias("nameOrig"),
        F.col("payload.oldbalanceOrg").cast(dec).alias("oldbalanceOrg"),
        F.col("payload.newbalanceOrig").cast(dec).alias("newbalanceOrig"),
        F.col("payload.nameDest").alias("nameDest"),
        F.col("payload.oldbalanceDest").cast(dec).alias("oldbalanceDest"),
        F.col("payload.newbalanceDest").cast(dec).alias("newbalanceDest"),
        F.col("payload.isFraud").cast("int").alias("isFraud"),
        F.col("payload.isFlaggedFraud").cast("int").alias("isFlaggedFraud"),
        is_susp.alias("is_suspicious"),
    )

    # ── Step 5b: Model 2 — 지정된 tx_date 1일치만 선택 (멱등 일별 증분) ──
    # 정합성 검증(Step 4)은 전체 Bronze 기준으로 이미 수행됨(파싱 무손실 확인).
    # 여기서는 "기록 대상"만 해당 날짜로 좁힌다 → dynamic overwrite가 그 파티션만 덮어씀.
    if TARGET_TX_DATE:
        silver_df = silver_df.filter(
            F.col("tx_date") == F.to_date(F.lit(TARGET_TX_DATE))
        )
        print(f"[silver] TARGET_TX_DATE={TARGET_TX_DATE} → 해당 일자만 기록")

    # ── Step 6: dedup (Spark 재시작으로 인한 Bronze→Silver 중복 흡수) ────
    pre_dedup_count = silver_df.count()          # (날짜 필터 적용 후) 기록 후보 수
    silver_df       = silver_df.dropDuplicates(["row_id"])
    silver_count    = silver_df.count()
    dedup_removed   = pre_dedup_count - silver_count

    # ── Step 7: Silver 저장 (dynamic overwrite, 멱등) ────────────────────
    silver_df.write \
        .mode("overwrite") \
        .partitionBy("tx_date") \
        .parquet(SILVER_PATH)

    # ── Step 8: Quarantine 저장 (dynamic overwrite, 멱등) ────────────────
    # 원본 value 보존 + reject_reason + ingest_date 파티션
    quarantine \
        .select("value", "kafka_timestamp", "reject_reason") \
        .withColumn("ingest_date", F.to_date(F.col("kafka_timestamp"))) \
        .write \
        .mode("overwrite") \
        .partitionBy("ingest_date") \
        .parquet(SILVER_QUAR_PATH)

    # ── Step 9: 완료 출력 ────────────────────────────────────────────────
    suspicious_count = silver_df.filter("is_suspicious").count()
    print(f"[silver] bronze={bronze_count}")
    print(f"[silver] valid_raw={valid_raw_count}  quarantine={quar_count}  dedup_removed={dedup_removed}")
    print(f"[silver] silver_written={silver_count}  is_suspicious={suspicious_count}")
    print(f"[silver] 완료 → {SILVER_PATH}")


if __name__ == "__main__":
    main()
