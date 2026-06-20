"""
파이프라인 데이터 정합성 테스트

구간 1 — Producer → Kafka
  TC1: 정상 전송 (무손실)        delivered == Kafka 메시지 증가량
  TC2: resume (이어받기)          500 전송 중단 → resume → 총 1000, 갭 없음
  TC3: 내용 무결성                Kafka value 필드 구성 + amount 타입(str) 검증

구간 2 — Kafka → Spark → Bronze
  TC4: 행 수 일치                 Kafka 메시지 수 == Bronze 행 수
  TC5: Spark 재실행 무중복        재실행 후 Bronze 행 수 불변 (checkpoint 검증)

실행:
  pytest tests/test_integrity.py -v
"""
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "kafka"))
import config

BOOTSTRAP = config.BOOTSTRAP_SERVERS
TOPIC = config.TOPIC
CSV_PATH = config.RAW_CSV_PATH
BRONZE_PATH = ROOT / "datalake" / "bronze"
CHECKPOINT_PATH = ROOT / "datalake" / "_checkpoints" / "bronze"
COMPOSE_FILE = str(ROOT / "docker" / "docker-compose.yml")
PYTHON = sys.executable


# ── 공통 헬퍼 ────────────────────────────────────────────────────────────────

def get_kafka_count(topic: str = TOPIC) -> int:
    """Kafka topic 파티션별 end offset 합 = 총 메시지 수."""
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"test-offset-{os.getpid()}-{time.time_ns()}",
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


def reset_topic() -> None:
    """토픽 삭제 후 동일 설정으로 재생성 (테스트 격리)."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    futures = admin.delete_topics([TOPIC], operation_timeout=30)
    for f in futures.values():
        try:
            f.result()
        except Exception:
            pass
    time.sleep(3)
    new_topic = NewTopic(
        TOPIC,
        num_partitions=3,
        replication_factor=3,
        config={
            "min.insync.replicas": "2",
            "retention.ms": "-1",
            "unclean.leader.election.enable": "false",
        },
    )
    futures = admin.create_topics([new_topic])
    for f in futures.values():
        f.result()
    time.sleep(2)


def run_producer(*extra_args: str) -> tuple[int, str]:
    """producer.py 실행 후 (exit_code, stdout+stderr) 반환."""
    result = subprocess.run(
        [PYTHON, str(ROOT / "kafka" / "producer.py"), *extra_args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=ROOT,
    )
    return result.returncode, result.stdout + result.stderr


def consume_all(max_msgs: int, timeout_empty: int = 5) -> list[dict]:
    """Kafka topic에서 최대 max_msgs개 메시지 소비 (earliest부터)."""
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"test-consume-{os.getpid()}-{time.time_ns()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    })
    c.subscribe([TOPIC])
    msgs, empty = [], 0
    while len(msgs) < max_msgs and empty < timeout_empty:
        m = c.poll(1.0)
        if m is None:
            empty += 1
            continue
        if m.error():
            continue
        msgs.append(json.loads(m.value().decode("utf-8")))
        empty = 0
    c.close()
    return msgs


def clear_bronze() -> None:
    """Bronze 데이터 + Spark checkpoint 삭제."""
    for p in [BRONZE_PATH, CHECKPOINT_PATH]:
        if p.exists():
            shutil.rmtree(p)


def run_spark_bronze() -> tuple[int, str]:
    """Spark Streaming Bronze 실행, (exit_code, output) 반환.
    BRONZE_FORMAT 환경변수를 컨테이너에 전달 (json 또는 parquet).
    """
    bronze_format = os.getenv("BRONZE_FORMAT", "parquet")
    result = subprocess.run(
        [
            "docker", "compose", "-f", COMPOSE_FILE, "run", "--rm",
            "-e", f"BRONZE_FORMAT={bronze_format}",
            "spark-streaming",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=ROOT,
    )
    return result.returncode, result.stdout + result.stderr


def get_bronze_count() -> int:
    """Bronze 총 행 수. BRONZE_FORMAT 환경변수에 따라 json/parquet 자동 분기."""
    if not BRONZE_PATH.exists():
        return 0
    bronze_format = os.getenv("BRONZE_FORMAT", "parquet")
    if bronze_format == "json":
        glob = str(BRONZE_PATH / "**" / "*.json").replace("\\", "/")
        query = f"SELECT count(*) FROM read_json_auto('{glob}')"
    else:
        glob = str(BRONZE_PATH / "**" / "*.parquet").replace("\\", "/")
        query = f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true)"
    return duckdb.connect().execute(query).fetchone()[0]


# ── 구간 1: Producer → Kafka ─────────────────────────────────────────────────

class TestProducerToKafka:

    def setup_method(self):
        """각 TC 전 토픽 초기화 → Kafka 0 메시지 보장."""
        reset_topic()
        assert get_kafka_count() == 0, "토픽 초기화 실패"

    def test_tc1_no_loss(self):
        """TC1: 1000행 전송 후 delivered == Kafka 메시지 증가량 == 1000."""
        code, out = run_producer("--limit", "1000")

        assert code == 0,            f"[TC1] Producer exit code != 0\n{out}"
        assert "failed=0" in out,    f"[TC1] 전송 실패 발생\n{out}"
        assert "delivered=1000" in out, f"[TC1] delivered != 1000\n{out}"

        kafka_count = get_kafka_count()
        assert kafka_count == 1000, (
            f"[TC1] Kafka 메시지 수 불일치: delivered=1000 != kafka={kafka_count}\n"
            f"→ Producer → Kafka 구간 유실 발생"
        )

    def test_tc2_resume(self):
        """TC2: 500행 중단 → resume → 총 1000행, 중복/갭 없음."""
        # 1단계: 500행 전송 (중단 시뮬레이션)
        code, out = run_producer("--limit", "500")
        assert code == 0 and "failed=0" in out, f"[TC2] 1단계 실패\n{out}"
        assert get_kafka_count() == 500, "[TC2] 1단계 후 Kafka != 500"

        # 2단계: resume으로 500행 추가 (row 500~999)
        code, out = run_producer("--resume", "--limit", "500")
        assert code == 0 and "failed=0" in out, f"[TC2] resume 단계 실패\n{out}"
        assert "kafka_count=500" in out, (
            f"[TC2] resume 로그에서 kafka_count=500 미확인\n{out}"
        )
        assert "start_row=500" in out, (
            f"[TC2] resume 로그에서 start_row=500 미확인\n{out}"
        )

        total = get_kafka_count()
        assert total == 1000, (
            f"[TC2] resume 후 Kafka 총 메시지 불일치: {total} != 1000\n"
            f"→ 중복({total > 1000}) 또는 갭({total < 1000}) 발생"
        )

    def test_tc3_content_integrity(self):
        """TC3: Kafka 메시지의 필드 구성 + amount 타입(str) + 유효 type 값 검증."""
        run_producer("--limit", "1000")
        msgs = consume_all(max_msgs=1000)

        assert len(msgs) == 1000, f"[TC3] Kafka에서 읽은 메시지 수: {len(msgs)} != 1000"

        required = {
            "step", "type", "amount", "nameOrig", "nameDest",
            "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest",
            "newbalanceDest", "isFraud", "isFlaggedFraud",
        }
        valid_types = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}
        errors = []

        for i, msg in enumerate(msgs):
            missing = required - set(msg.keys())
            if missing:
                errors.append(f"msg[{i}] 필드 누락: {missing}")
            if not isinstance(msg.get("amount"), str):
                errors.append(f"msg[{i}] amount 타입={type(msg.get('amount')).__name__} (str 필요)")
            if msg.get("type") not in valid_types:
                errors.append(f"msg[{i}] 유효하지 않은 type={msg.get('type')!r}")

        assert not errors, "[TC3] 내용 무결성 오류:\n" + "\n".join(errors[:10])


# ── 구간 2: Kafka → Spark → Bronze ──────────────────────────────────────────

class TestKafkaToBronze:

    def setup_method(self):
        """각 TC 전 토픽 초기화 + Bronze 초기화 + 1000행 발행."""
        reset_topic()
        clear_bronze()
        code, out = run_producer("--limit", "1000")
        assert code == 0 and "failed=0" in out, f"setup 중 Producer 실패\n{out}"

    def test_tc4_count_match(self):
        """TC4: Kafka 메시지 수 == Bronze 행 수 (Kafka → Bronze 구간 유실 없음)."""
        kafka_count = get_kafka_count()

        code, out = run_spark_bronze()
        assert code == 0, f"[TC4] Spark 실행 실패\n{out}"

        bronze_count = get_bronze_count()
        assert bronze_count == kafka_count, (
            f"[TC4] Kafka({kafka_count}) != Bronze({bronze_count})\n"
            f"→ Spark 처리 중 유실 발생"
        )

    def test_tc5_no_duplicate_on_rerun(self):
        """TC5: Spark 재실행 시 Bronze 행 수 불변 (checkpoint 무중복 검증)."""
        code, out = run_spark_bronze()
        assert code == 0, f"[TC5] Spark 1차 실패\n{out}"
        count_first = get_bronze_count()
        assert count_first == 1000, f"[TC5] 1차 실행 후 Bronze != 1000: {count_first}"

        code, out = run_spark_bronze()
        assert code == 0, f"[TC5] Spark 2차 실패\n{out}"
        count_second = get_bronze_count()

        assert count_first == count_second, (
            f"[TC5] Spark 재실행 후 Bronze 증가: {count_first} → {count_second}\n"
            f"→ checkpoint 미동작, 동일 offset 중복 처리"
        )


# ── 구간 3: 3-세션 이어받기 증분 검증 ────────────────────────────────────────
#
# 시나리오: Producer 1000 → Spark / --resume 2000 → Spark / --resume 3000 → Spark
# 각 단계마다 Kafka 수 == Bronze 수를 검증.
# BRONZE_FORMAT=json 환경변수 필요: $env:BRONZE_FORMAT="json" (PowerShell)
#
# 실행:
#   $env:BRONZE_FORMAT="json"
#   pytest tests/test_integrity.py::TestIncrementalSession -v -s

class TestIncrementalSession:

    @classmethod
    def setup_class(cls):
        """클래스 전체 1회 실행: Kafka topic + Bronze + checkpoint 완전 초기화."""
        reset_topic()
        clear_bronze()
        assert get_kafka_count() == 0, "Kafka topic 초기화 실패"
        assert get_bronze_count() == 0, "Bronze 초기화 실패"

    def test_inc1_session1(self):
        """INC-1: 1차 세션 — Producer 1000 → Spark → Kafka=1000, Bronze=1000."""
        code, out = run_producer("--limit", "1000")
        assert code == 0 and "failed=0" in out, f"[INC-1] Producer 실패\n{out}"

        kafka_count = get_kafka_count()
        assert kafka_count == 1000, f"[INC-1] Kafka != 1000: {kafka_count}"

        code, out = run_spark_bronze()
        assert code == 0, f"[INC-1] Spark 실패\n{out}"

        bronze_count = get_bronze_count()
        assert bronze_count == kafka_count, (
            f"[INC-1] Bronze({bronze_count}) != Kafka({kafka_count})\n→ 1차 세션 후 유실"
        )

    def test_inc2_session2_resume(self):
        """INC-2: 2차 세션 — --resume 이어받기 → Kafka=2000, Bronze=2000."""
        code, out = run_producer("--resume", "--limit", "1000")
        assert code == 0 and "failed=0" in out, f"[INC-2] Producer resume 실패\n{out}"
        assert "start_row=1000" in out, f"[INC-2] start_row=1000 미확인\n{out}"

        kafka_count = get_kafka_count()
        assert kafka_count == 2000, f"[INC-2] Kafka != 2000: {kafka_count}"

        code, out = run_spark_bronze()
        assert code == 0, f"[INC-2] Spark 실패\n{out}"

        bronze_count = get_bronze_count()
        assert bronze_count == kafka_count, (
            f"[INC-2] Bronze({bronze_count}) != Kafka({kafka_count})\n→ 2차 세션 후 유실"
        )

    def test_inc3_session3_resume(self):
        """INC-3: 3차 세션 — --resume 이어받기 → Kafka=3000, Bronze=3000."""
        code, out = run_producer("--resume", "--limit", "1000")
        assert code == 0 and "failed=0" in out, f"[INC-3] Producer resume 실패\n{out}"
        assert "start_row=2000" in out, f"[INC-3] start_row=2000 미확인\n{out}"

        kafka_count = get_kafka_count()
        assert kafka_count == 3000, f"[INC-3] Kafka != 3000: {kafka_count}"

        code, out = run_spark_bronze()
        assert code == 0, f"[INC-3] Spark 실패\n{out}"

        bronze_count = get_bronze_count()
        assert bronze_count == kafka_count, (
            f"[INC-3] Bronze({bronze_count}) != Kafka({kafka_count})\n→ 3차 세션 후 유실"
        )

    def test_inc4_no_dup_on_rerun(self):
        """INC-4: Spark 재실행 후 Bronze 행 수 불변 (checkpoint 무중복, 3000 기준)."""
        before = get_bronze_count()
        assert before == 3000, f"[INC-4] 전제 실패 — Bronze != 3000: {before}"

        code, out = run_spark_bronze()
        assert code == 0, f"[INC-4] Spark 재실행 실패\n{out}"

        after = get_bronze_count()
        assert after == before, (
            f"[INC-4] 재실행 후 Bronze 변화: {before} → {after}\n"
            f"→ checkpoint 미동작, 중복 처리 발생"
        )

    def test_inc5_bronze_column_integrity(self):
        """INC-5: Bronze JSON 컬럼 검증 — 필드 완전성, null, amount=str, type 유효값.

        BRONZE_FORMAT=json 환경변수가 설정되어 있어야 한다.
        json 포맷이 아니면 이 TC는 건너뛴다.
        """
        bronze_format = os.getenv("BRONZE_FORMAT", "parquet")
        if bronze_format != "json":
            pytest.skip("BRONZE_FORMAT=json 일 때만 실행 (현재: parquet)")

        required_fields = {
            "step", "type", "amount", "nameOrig", "nameDest",
            "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest",
            "newbalanceDest", "isFraud", "isFlaggedFraud",
        }
        valid_types = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}
        valid_flag_values = {"0", "1"}
        errors: list[str] = []
        total = 0

        json_files = sorted(BRONZE_PATH.glob("**/*.json"))
        assert json_files, "[INC-5] Bronze JSON 파일이 없음"

        for jf in json_files:
            for line_no, line in enumerate(jf.read_text(encoding="utf-8").splitlines(), 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    outer = json.loads(line)
                except json.JSONDecodeError as e:
                    errors.append(f"{jf.name}:{line_no} JSON 파싱 오류: {e}")
                    continue

                # Bronze outer 필드 검증
                # date는 partitionBy 키이므로 파일 내부에 없음 (디렉토리명 date=YYYY-MM-DD 에 인코딩)
                if outer.get("kafka_timestamp") is None:
                    errors.append(f"{jf.name}:{line_no} kafka_timestamp=null")

                # 내부 value(원본 Kafka 메시지) 파싱
                raw_value = outer.get("value")
                if raw_value is None:
                    errors.append(f"{jf.name}:{line_no} value=null")
                    total += 1
                    continue
                try:
                    inner = json.loads(raw_value)
                except json.JSONDecodeError as e:
                    errors.append(f"{jf.name}:{line_no} value JSON 파싱 오류: {e}")
                    total += 1
                    continue

                # 필드 완전성
                missing = required_fields - set(inner.keys())
                if missing:
                    errors.append(f"{jf.name}:{line_no} 필드 누락: {missing}")

                # null 체크 (required 필드)
                for field in required_fields:
                    if inner.get(field) is None:
                        errors.append(f"{jf.name}:{line_no} {field}=null")

                # amount 타입 (문자열이어야 함 — float 드리프트 방지)
                if not isinstance(inner.get("amount"), str):
                    errors.append(
                        f"{jf.name}:{line_no} amount 타입={type(inner.get('amount')).__name__} (str 필요)"
                    )

                # type 유효값
                if inner.get("type") not in valid_types:
                    errors.append(f"{jf.name}:{line_no} 유효하지 않은 type={inner.get('type')!r}")

                # isFraud / isFlaggedFraud 유효값
                for flag_field in ("isFraud", "isFlaggedFraud"):
                    if inner.get(flag_field) not in valid_flag_values:
                        errors.append(
                            f"{jf.name}:{line_no} {flag_field}={inner.get(flag_field)!r} (0/1 필요)"
                        )

                total += 1
                if len(errors) >= 20:  # 오류 20개 이상이면 조기 중단
                    break
            if len(errors) >= 20:
                break

        assert not errors, (
            f"[INC-5] 컬럼 무결성 오류 {len(errors)}건 (최대 20개 표시):\n"
            + "\n".join(errors[:20])
        )
        assert total == 3000, f"[INC-5] 검증한 행 수={total} (기대: 3000)"
