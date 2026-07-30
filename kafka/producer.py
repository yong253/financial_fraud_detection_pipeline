"""CSV → Kafka Producer (confluent-kafka, 멱등 + 무손실).

금융 정합성:
  - enable.idempotence + acks=all  → 무손실 + 무중복(단일 세션)
  - key=nameOrig                    → 계좌별 같은 파티션 = 순서 보장
  - 값은 모두 문자열 유지            → 금액 float 드리프트 방지(원본 충실)
  - 비동기 produce + delivery 콜백 + poll + 종료 flush + BufferError 백프레셔
                                     → 비동기여도 유실 없음

이 producer가 하는 두 가지 "추가" 작업:

  ① `event_time` 주입 — step(1부터의 시간 번호)을 절대 시각(epoch millis)으로 계산해
     레코드에 넣는다. Kafka Connect GCS Sink가 이 필드로 Bronze를 **이벤트일**
     (`date=2016-01-03/`)로 파티셔닝한다. 넣지 않으면 Connect는 인제스트 시각밖에 몰라
     하루치 프루닝이 불가능해지고 Spark가 매 배치 Bronze 전량을 스캔하게 된다.

  ② EOD(end-of-day) 마커 — 하루치 전송이 끝나면 **모든 파티션에 각각** 마커 메시지를
     하나씩 발행한다. Airflow `bronze_sensor`가 "그날 마커 수 == 파티션 수"로 완결을
     판정한다.

     왜 스트림 안에 끼워넣는가: 완결 신호는 **데이터와 같은 경로로 흘러야** 그 경로의
     완료를 증명한다. 예전 `--done-marker`는 producer가 GCS에 직접 썼기 때문에 Kafka와
     Connect를 건너뛴 신호였고, "Kafka에 다 넣었다"만 증명할 뿐 Bronze 도착과는 무관했다.
     마커를 파티션에 끼워넣으면 파티션 내 순서 보장에 의해
     "마커가 Bronze에 보임 ⟹ 그 앞의 데이터도 전부 Bronze에 있음"이 성립한다.

사용:
  python kafka/producer.py --limit 1000     # 관통 슬라이스
  python kafka/producer.py --limit 0        # 전체(6.36M)
  python kafka/producer.py --limit 0 --resume   # 마지막 완결일 다음날부터 이어서
"""
import argparse
import collections
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import config
from confluent_kafka import Consumer, Producer, TopicPartition

# delivery 콜백이 갱신하는 카운터
_stats = {"delivered": 0, "failed": 0, "markers": 0}

MARKER_RECORD_TYPE = "eod_marker"

# step=1 의 절대 시각. config가 .env(→ docker-compose)에서 읽는다.
_EPOCH = datetime.strptime(config.STEP_EPOCH, "%Y-%m-%d %H:%M:%S").replace(
    tzinfo=timezone.utc
)


def _day_index(step: int) -> int:
    """step(1-base 시간 번호) → 이벤트일 인덱스(0-base). step 1~24 → 0일차."""
    return (step - 1) // 24


def _event_time_ms(step: int) -> int:
    """step → 이벤트 시각(epoch millis). Connect가 이 값으로 date= 폴더를 만든다."""
    return int((_EPOCH + timedelta(hours=step - 1)).timestamp() * 1000)


def _day_date(day_idx: int) -> str:
    """이벤트일 인덱스 → 'YYYY-MM-DD'."""
    return (_EPOCH + timedelta(days=day_idx)).date().isoformat()


def _day_end_event_time_ms(day_idx: int) -> int:
    """그날 23시의 epoch millis — 마커가 **그날 폴더**에 들어가도록 하는 값.

    마커는 데이터와 같은 writer(같은 토픽·파티션·폴더)를 타야 순서 보장이 성립한다.
    폴더가 갈리면 Connect의 writer가 분리돼 flush 순서 관계가 사라진다.
    """
    return int((_EPOCH + timedelta(days=day_idx, hours=23)).timestamp() * 1000)


def _on_delivery(err, msg):
    """비동기 전송 결과 콜백. 실패를 반드시 여기서 잡는다(미확인=유실)."""
    if err is not None:
        _stats["failed"] += 1
        sys.stderr.write(f"[FAIL] key={msg.key()} err={err}\n")
    else:
        _stats["delivered"] += 1


def get_partition_count(producer: Producer, topic: str) -> int:
    """토픽의 실제 파티션 수. 마커를 몇 개 찍을지와 센서의 완결 기준이 여기서 나온다.

    하드코딩하지 않는다 — 파티션 수가 바뀌면 완결 판정이 조용히 틀어진다.
    """
    meta = producer.list_topics(topic, timeout=10)
    if topic not in meta.topics:
        raise RuntimeError(f"토픽 없음: {topic}")
    return len(meta.topics[topic].partitions)


def emit_day_markers(producer: Producer, day_idx: int, n_parts: int) -> None:
    """day_idx 일차 완결 마커를 **모든 파티션에** 하나씩 발행.

    호출 시점이 중요하다 — 그날 데이터를 전부 produce 한 **뒤**여야 한다. 그래야 각
    파티션에서 마커의 오프셋이 그날 데이터보다 뒤에 온다(파티션 내 순서 보장).
    """
    tx_date = _day_date(day_idx)
    ev_ms = _day_end_event_time_ms(day_idx)
    for pid in range(n_parts):
        payload = {
            "record_type": MARKER_RECORD_TYPE,
            "tx_date": tx_date,
            "kafka_partition": pid,
            "total_partitions": n_parts,   # 센서가 마커만 보고 완결 판정을 닫을 수 있게
            "event_time": ev_ms,
        }
        producer.produce(
            config.TOPIC,
            key=f"__eod__{tx_date}".encode(),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            partition=pid,                 # 키 해싱이 아니라 파티션 지정
            on_delivery=_on_delivery,
        )
        _stats["markers"] += 1
    producer.flush()
    print(f"[marker] {tx_date} 완결 마커 {n_parts}개 발행", flush=True)


def find_last_complete_day(bootstrap_servers: str, topic: str, lookback: int) -> int:
    """마커가 **모든 파티션**에 찍힌 마지막 이벤트일 인덱스. 없으면 -1.

    각 파티션의 끝에서 `lookback` 만큼만 거슬러 스캔한다(전량 소비는 발행만큼 느리다).
    마지막 완결일의 마커는 "그날 + 진행 중이던 다음날" 분량 안에 있으므로 하루치
    최대 행수(약 455k / 3파티션 ≈ 152k)의 몇 배면 충분하다.

    **정확도가 정합성을 좌우하지 않는다** — 못 찾으면 처음부터 다시 보내고, 겹치는 구간은
    Silver의 row_id dedup이 흡수한다. 즉 이 함수는 최적화이지 안전장치가 아니다.
    """
    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"eod-scanner-{os.getpid()}",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
    })
    try:
        meta = c.list_topics(topic, timeout=10)
        if topic not in meta.topics:
            return -1
        pids = sorted(meta.topics[topic].partitions)
        n_parts = len(pids)

        assign, ends = [], {}
        for pid in pids:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
            ends[pid] = hi
            assign.append(TopicPartition(topic, pid, max(lo, hi - lookback)))
        if all(ends[p] == 0 for p in pids):
            return -1
        c.assign(assign)

        seen = collections.defaultdict(set)   # tx_date → {파티션}
        pending = {p for p in pids if ends[p] > 0}
        while pending:
            msg = c.poll(timeout=10.0)
            if msg is None:
                break                          # 더 안 오면 중단(부분 결과로 판단)
            if msg.error():
                continue
            if msg.offset() >= ends[msg.partition()] - 1:
                pending.discard(msg.partition())
            try:
                rec = json.loads(msg.value())
            except (ValueError, TypeError):
                continue
            if rec.get("record_type") == MARKER_RECORD_TYPE:
                seen[rec["tx_date"]].add(msg.partition())

        complete = [d for d, parts in seen.items() if len(parts) == n_parts]
        if not complete:
            return -1
        last = max(complete)
        return (datetime.strptime(last, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                - _EPOCH).days
    finally:
        c.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="PaySim CSV → Kafka producer")
    parser.add_argument("--limit", type=int, default=1000,
                        help="이번 실행에서 발행할 행 수 (0=전체). 기본 1000")
    parser.add_argument("--rate", type=float, default=0,
                        help="초당 최대 발행 수 (0=제한 없음)")
    parser.add_argument("--resume", action="store_true",
                        help="마커가 완결된 마지막 날 **다음날**부터 이어서 전송")
    parser.add_argument("--resume-lookback", type=int, default=500_000,
                        help="--resume 시 파티션당 뒤에서 스캔할 메시지 수. 기본 500000")
    parser.add_argument("--realtime", action="store_true",
                        help="이벤트일 사이에 --day-delay 만큼 쉬며 실시간 유입을 흉내")
    parser.add_argument("--day-delay", type=float, default=20.0,
                        help="--realtime 시 이벤트일 사이 대기(초). 기본 20")
    parser.add_argument("--max-days", type=int, default=0,
                        help="발행할 이벤트일 수 상한(0=무제한). 예: 3 → step 1~72만 발행")
    args = parser.parse_args()

    producer = Producer(config.producer_config())
    n_parts = get_partition_count(producer, config.TOPIC)
    print(f"[init] topic={config.TOPIC} partitions={n_parts} step_epoch={config.STEP_EPOCH}")

    # SIGINT(Ctrl+C) 시에도 잔여분 flush 후 종료 (유실 방지)
    def _graceful(signum, frame):
        sys.stderr.write("\n[SIGINT] flush 후 종료...\n")
        producer.flush()
        _report()
        sys.exit(130)

    signal.signal(signal.SIGINT, _graceful)
    signal.signal(signal.SIGTERM, _graceful)  # docker stop(SIGTERM) 시에도 flush 후 종료

    # --resume: 완결 마커 기준으로 "다음 날"부터. 행 단위가 아니라 **날짜 단위**다.
    #   행 단위(Kafka 총 메시지 수 = CSV 행 수)는 마커가 끼면서 성립하지 않는다 —
    #   마커도 메시지라 그만큼 CSV를 더 건너뛰어 조용히 유실된다.
    start_day = 0
    if args.resume:
        last_done = find_last_complete_day(
            config.BOOTSTRAP_SERVERS, config.TOPIC, args.resume_lookback
        )
        start_day = last_done + 1
        if last_done < 0:
            print("[resume] 완결된 이벤트일 없음 → 처음부터 전송")
        else:
            print(f"[resume] 마지막 완결일={_day_date(last_done)} → "
                  f"{_day_date(start_day)}부터 전송")

    sent = 0
    start = time.time()
    with open(config.RAW_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)  # 모든 값이 문자열 → 원본 그대로
        prev_day = None
        for row in reader:
            if args.limit and sent >= args.limit:
                break

            step = int(row["step"])
            day = _day_index(step)

            # --resume: 이미 완결된 날짜는 건너뛴다(CSV가 step 오름차순이라 앞부분).
            if day < start_day:
                continue
            # 이벤트일 수 상한: 상한에 도달하면 즉시 중단.
            if args.max_days and day >= start_day + args.max_days:
                print(f"[limit] max-days={args.max_days} 도달(day={day}) → 중단 sent={sent}",
                      flush=True)
                break

            # 날짜 경계: 직전 날의 데이터를 전부 보냈으므로 그 날의 완결 마커를 찍는다.
            # (--realtime 여부와 무관하게 항상 찍는다 — 마커는 센서의 유일한 완결 근거다.)
            if prev_day is not None and day != prev_day:
                producer.flush()
                emit_day_markers(producer, prev_day, n_parts)
                if args.realtime:
                    print(f"[realtime] day={_day_date(prev_day)} 전송완료 sent={sent} "
                          f"→ {args.day_delay}s 대기", flush=True)
                    time.sleep(args.day_delay)
            prev_day = day

            row["event_time"] = _event_time_ms(step)   # Connect가 이 값으로 날짜 폴더 결정
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

    # 마지막 날의 완결 마커 — 이게 없으면 그 날짜 DAG run이 영원히 대기한다.
    if prev_day is not None:
        producer.flush()
        emit_day_markers(producer, prev_day, n_parts)

    # 종료 전 잔여분 전송 보장 (동기 배리어)
    remaining = producer.flush(30)
    if remaining:
        sys.stderr.write(f"[WARN] flush 후에도 {remaining}건 미전송\n")
    _report(sent, time.time() - start)

    return 0 if _stats["failed"] == 0 else 1


def _report(sent=None, elapsed=None):
    line = (f"delivered={_stats['delivered']} failed={_stats['failed']} "
            f"markers={_stats['markers']}")
    if sent is not None:
        line += f" sent={sent}"
    if elapsed:
        line += f" elapsed={elapsed:.1f}s"
    print(line)


if __name__ == "__main__":
    sys.exit(main())
