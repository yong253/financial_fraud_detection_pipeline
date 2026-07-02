"""CSV → Kafka Producer (confluent-kafka, 멱등 + 무손실).

금융 정합성:
  - enable.idempotence + acks=all  → 무손실 + 무중복(단일 세션)
  - key=nameOrig                    → 계좌별 같은 파티션 = 순서 보장
  - 값은 모두 문자열 유지            → 금액 float 드리프트 방지(원본 충실)
  - 비동기 produce + delivery 콜백 + poll + 종료 flush + BufferError 백프레셔
                                     → 비동기여도 유실 없음

사용:
  python kafka/producer.py --limit 1000           # 관통 슬라이스
  python kafka/producer.py                        # 전체(6.3M)
  python kafka/producer.py --resume               # Kafka offset 이후부터 이어서 전송
  python kafka/producer.py --resume --limit 500   # 이어서 최대 500행
"""
import argparse
import collections
import csv
import itertools
import json
import signal
import sys
import time

from confluent_kafka import Consumer, Producer, TopicPartition

import config

# delivery 콜백이 갱신하는 카운터
_stats = {"delivered": 0, "failed": 0}


def get_kafka_offset_count(bootstrap_servers: str, topic: str) -> int:
    """Kafka topic 파티션별 end offset 합 = 현재 총 메시지 수 (방법 B resume 기준)."""
    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"offset-checker-{__import__('os').getpid()}",
        "enable.auto.commit": "false",
    })
    try:
        meta = c.list_topics(topic, timeout=10)
        if topic not in meta.topics:
            return 0
        total = 0
        for pid in meta.topics[topic].partitions:
            _, high = c.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
            total += high
        return total
    finally:
        c.close()


def _on_delivery(err, msg):
    """비동기 전송 결과 콜백. 실패를 반드시 여기서 잡는다(미확인=유실)."""
    if err is not None:
        _stats["failed"] += 1
        sys.stderr.write(f"[FAIL] key={msg.key()} err={err}\n")
    else:
        _stats["delivered"] += 1


def main() -> int:
    parser = argparse.ArgumentParser(description="PaySim CSV → Kafka producer")
    parser.add_argument("--limit", type=int, default=1000,
                        help="이번 실행에서 발행할 행 수 (0=전체). 기본 1000")
    parser.add_argument("--rate", type=float, default=0,
                        help="초당 최대 발행 수 (0=제한 없음)")
    parser.add_argument("--resume", action="store_true",
                        help="Kafka 현재 offset 조회 후 그 다음 행부터 이어서 전송 (방법 B)")
    parser.add_argument("--realtime", action="store_true",
                        help="이벤트일(tx_date) 단위로 흘려보냄: 날짜 바뀌면 flush+지연 (실시간 흐름 시뮬레이션)")
    parser.add_argument("--day-delay", type=float, default=20.0,
                        help="--realtime 시 이벤트일 사이 대기(초). 기본 20")
    parser.add_argument("--max-days", type=int, default=0,
                        help="--realtime 시 발행할 이벤트일 수 상한(0=무제한). 예: 3 → step 1~72만 발행")
    parser.add_argument("--done-marker", type=str, default="",
                        help="--realtime 종료 시 생성할 완료 마커 파일 경로(마지막 날 센서 완결 신호)")
    args = parser.parse_args()

    producer = Producer(config.producer_config())

    # SIGINT(Ctrl+C) 시에도 잔여분 flush 후 종료 (유실 방지)
    def _graceful(signum, frame):
        sys.stderr.write("\n[SIGINT] flush 후 종료...\n")
        producer.flush()
        _report()
        sys.exit(130)

    signal.signal(signal.SIGINT, _graceful)

    # --resume: Kafka에 이미 있는 메시지 수만큼 CSV를 건너뜀
    skip_rows = 0
    if args.resume:
        skip_rows = get_kafka_offset_count(config.BOOTSTRAP_SERVERS, config.TOPIC)
        print(f"[resume] kafka_count={skip_rows} start_row={skip_rows}")

    sent = 0
    start = time.time()
    with open(config.RAW_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)  # 모든 값이 문자열 → 원본 그대로
        # skip_rows만큼 효율적으로 건너뜀 (메모리에 적재 없이)
        collections.deque(itertools.islice(reader, skip_rows), maxlen=0)
        prev_day = None
        for row in reader:
            if args.limit and sent >= args.limit:
                break
            # --realtime: 이벤트일(tx_date = (step-1)//24)이 바뀌면 그 날치를 flush 후 지연.
            # → Kafka로 하루치씩 흘러들어가고, Spark Streaming이 Bronze에 실시간 적재.
            if args.realtime:
                day = (int(row["step"]) - 1) // 24
                # 이벤트일 수 상한: day 인덱스가 상한에 도달하면 즉시 중단(CSV는 step 오름차순).
                # 직전까지 발행한 마지막 날치는 루프 후 flush(30) 배리어가 전송 보장.
                if args.max_days and day >= args.max_days:
                    print(f"[realtime] max-days={args.max_days} 도달(day={day}) → 발행 중단 sent={sent}", flush=True)
                    break
                if prev_day is not None and day != prev_day:
                    producer.flush()
                    print(f"[realtime] day idx={prev_day} 전송완료 sent={sent} → {args.day_delay}s 대기", flush=True)
                    time.sleep(args.day_delay)
                prev_day = day
            key = row["nameOrig"].encode("utf-8")
            value = json.dumps(row, ensure_ascii=False).encode("utf-8")

            # 비동기 produce + BufferError 백프레셔(드롭 금지)
            while True:
                try:
                    producer.produce(
                        config.TOPIC, key=key, value=value, on_delivery=_on_delivery
                    )
                    break
                except BufferError:
                    producer.poll(0.5)  # 로컬 큐 가득 → 비우고 재시도

            producer.poll(0)  # delivery 콜백 서빙
            sent += 1

            if args.rate:
                elapsed = time.time() - start
                expected = sent / args.rate
                if expected > elapsed:
                    time.sleep(expected - elapsed)

    # 종료 전 잔여분 전송 보장 (동기 배리어)
    remaining = producer.flush(30)
    if remaining:
        sys.stderr.write(f"[WARN] flush 후에도 {remaining}건 미전송\n")
    _report(sent, time.time() - start)

    # --realtime 마지막 날 완결 신호: 센서가 "다음날 없음 → done-marker"로 마지막 tx_date 완결 판정.
    # gs:// 경로면 GCS에 직접 업로드(E단계: 수집 완전 자동화), 아니면 기존 로컬 파일 유지.
    if args.realtime and args.done_marker:
        marker_body = f"delivered={_stats['delivered']} failed={_stats['failed']}\n"
        if args.done_marker.startswith("gs://"):
            from google.cloud import storage

            bucket_name, _, blob_name = args.done_marker[len("gs://"):].partition("/")
            client = storage.Client.from_service_account_json(config.GCP_CREDENTIALS_PATH)
            client.bucket(bucket_name).blob(blob_name).upload_from_string(marker_body)
        else:
            import os
            os.makedirs(os.path.dirname(args.done_marker) or ".", exist_ok=True)
            with open(args.done_marker, "w", encoding="utf-8") as mf:
                mf.write(marker_body)
        print(f"[realtime] 완료 마커 생성 → {args.done_marker}", flush=True)

    return 0 if _stats["failed"] == 0 else 1


def _report(sent=None, elapsed=None):
    line = f"delivered={_stats['delivered']} failed={_stats['failed']}"
    if sent is not None:
        line += f" sent={sent}"
    if elapsed:
        line += f" elapsed={elapsed:.1f}s"
    print(line)


if __name__ == "__main__":
    sys.exit(main())
