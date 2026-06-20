"""Kafka 공유 설정 로더.

.env 에서 값을 읽어 Producer 설정/토픽/CSV 경로를 제공한다.
코드에 경로·주소를 하드코딩하지 않기 위한 단일 진입점.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# 호스트(로컬 Producer) → 3-broker. 컨테이너 실행 시 kafka1:29092,... 로 교체.
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094",
)
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
RAW_CSV_PATH = os.getenv(
    "RAW_CSV_PATH", "./data/raw/Synthetic_Financial_datasets_log.csv"
)


def producer_config() -> dict:
    """금융 정합성 보장 Producer 설정.

    enable.idempotence=true 가 acks=all / retries 무한급 / max.in.flight<=5 를
    자동으로 강제 → 무손실 + 무중복(단일 세션).
    """
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,   # 무중복(PID+seq), acks=all 등 자동 강제
        "acks": "all",                # 명시 (모든 ISR 복제 확인 후 ack)
        "compression.type": "lz4",    # 성능
        "linger.ms": 50,              # 소량 배치로 처리량↑
        # message.timeout.ms 기본 300000(5분) — 재시도 시간 한도. 필요 시 상향.
    }
