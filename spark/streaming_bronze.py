"""Kafka → GCS/로컬 Bronze (Spark Structured Streaming).

Medallion Bronze 규칙: Kafka 원본 그대로 저장 + kafka_timestamp 만 추가.
스키마 변환 / dedup / 검증은 Silver(다음 단계)에서.

정합성:
  - checkpointLocation     → 오프셋+상태 저장, 재시작 무손실/무중복
  - failOnDataLoss=true    → 필요 오프셋 유실 시 조용히 넘기지 않고 실패
  - maxOffsetsPerTrigger   → 배치 크기 제한(OOM 방지/백프레셔)
  - 파일 싱크 + checkpoint → 부분파일/재시도 중복 무시(exactly-once to sink)

컨테이너 실행:
  docker compose -f docker/docker-compose.yml run --rm spark-streaming
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:29092,kafka2:29092,kafka3:29092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/datalake/bronze")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/datalake/_checkpoints/bronze")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000")
BRONZE_FORMAT = os.getenv("BRONZE_FORMAT", "parquet")  # 검증 시 json, 운영은 parquet


def main() -> None:
    spark = (
        SparkSession.builder.appName("streaming_bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    # Bronze: 원본(value) + kafka_timestamp 만. 파티션용 date 파생.
    bronze = raw.select(
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("date", F.to_date(F.col("kafka_timestamp")))

    query = (
        bronze.writeStream.format(BRONZE_FORMAT)
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("date")
        .outputMode("append")
        .trigger(availableNow=True)  # 쌓인 것 처리 후 종료(슬라이스 검증). 상시는 이후 전환.
        .start()
    )
    query.awaitTermination()
    print(f"[bronze] 완료 → {BRONZE_PATH}")


if __name__ == "__main__":
    main()
