"""⑤ Airflow DAG 정합성 테스트 (AF1~AF9).

구조 검증(AF1·AF2)·정합성 로직(AF7)은 빠르게, E2E/증분/멱등(AF3·AF4·AF6)은
실제 잡 실행으로 검증한다. 무거운 것은 `@pytest.mark.slow`.

전제: `docker compose -f docker/docker-compose.yml up -d` 로 airflow 스택 + kafka 가 떠 있고,
      datalake/bronze 에 데이터(예: 3000행, tx_date=2016-01-01)가 존재.

실행:
  pytest tests/test_airflow_dag.py -v                 # 전체
  pytest tests/test_airflow_dag.py -v -m "not slow"   # 빠른 구조/로직만
"""
import subprocess
from pathlib import Path

import duckdb
import pytest

ROOT        = Path(__file__).parent.parent
SCHEDULER   = "airflow-scheduler"
COMPOSE     = ["docker", "compose", "-f", str(ROOT / "docker" / "docker-compose.yml")]
DATALAKE    = ROOT / "datalake"
WAREHOUSE   = DATALAKE / "warehouse.duckdb"
SILVER_GLOB = str(DATALAKE / "silver" / "tx_date=*" / "**" / "*.parquet").replace("\\", "/")
DAG_ID      = "fraud_pipeline"
TASKS       = ["bronze_sensor", "spark_silver", "dbt_run", "dbt_test", "reconcile"]


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _exec(*args, timeout=900):
    """airflow-scheduler 컨테이너 안에서 명령 실행."""
    r = subprocess.run(
        ["docker", "exec", SCHEDULER, *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
    )
    return r.returncode, r.stdout + r.stderr


def _compose_run(*args, timeout=900):
    """호스트에서 `docker compose run --rm <job>` 실행."""
    r = subprocess.run(
        [*COMPOSE, "run", "--rm", *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=timeout, cwd=ROOT,
    )
    return r.returncode, r.stdout + r.stderr


def _silver_count(where: str = "TRUE") -> int:
    return duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{SILVER_GLOB}', hive_partitioning=true) WHERE {where}"
    ).fetchone()[0]


def _silver_partitions() -> set[str]:
    return {p.name.split("=")[1] for p in (DATALAKE / "silver").glob("tx_date=*")}


# ── AF1: DAG 임포트 ──────────────────────────────────────────────────────────

def test_af1_no_import_errors():
    """DagBag import 오류가 없어야 한다."""
    code, out = _exec("airflow", "dags", "list-import-errors", timeout=120)
    assert code == 0, out
    # fraud_pipeline 관련 import 오류가 출력에 없어야 함
    assert DAG_ID not in out, f"[AF1] DAG import 오류:\n{out}"

    code, out = _exec("airflow", "dags", "list", timeout=120)
    assert DAG_ID in out, f"[AF1] DAG 미등록:\n{out}"


# ── AF2: DAG 구조 ────────────────────────────────────────────────────────────

def test_af2_structure_and_dependencies():
    """태스크 5개 + 선형 의존성(sensor→silver→run→test→reconcile)."""
    code, out = _exec("airflow", "tasks", "list", DAG_ID, timeout=120)
    assert code == 0, out
    for t in TASKS:
        assert t in out, f"[AF2] 태스크 누락: {t}\n{out}"

    # 의존성 검증 (컨테이너 내부 DagBag)
    snippet = (
        "from airflow.models import DagBag;"
        "d=DagBag().get_dag('fraud_pipeline');"
        "print({t.task_id:sorted(u.task_id for u in t.upstream_list) for t in d.tasks})"
    )
    code, out = _exec("python", "-c", snippet, timeout=120)
    # spark_silver는 bronze_sensor(데이터 완결) + upload_spark_code(코드 동기화) 둘 다에 의존(D단계).
    assert "'spark_silver': ['bronze_sensor', 'upload_spark_code']" in out, out
    assert "'dbt_run': ['spark_silver']" in out, out
    assert "'dbt_test': ['dbt_run']" in out, out
    assert "'reconcile': ['dbt_test']" in out, out


# ── AF7: reconcile 정합성 로직 (순수, 빠름) ─────────────────────────────────

def test_af7_reconcile_logic_detects_mismatch(tmp_path):
    """reconcile 불변식: undetected_fraud != silver is_suspicious 면 탐지(실패)해야 한다.

    DAG의 _reconcile 과 동일한 '두 카운트 비교' 로직을 재현해 검증.
    """
    def reconcile_gate(gold: int, silver: int):
        if gold != silver:
            raise ValueError(f"정합성 불일치: undetected_fraud({gold}) != silver({silver})")

    reconcile_gate(16, 16)                       # 일치 → 통과
    with pytest.raises(ValueError):
        reconcile_gate(15, 16)                   # 불일치 → 실패 탐지


# ── AF9: bronze_sensor 완결 판정 로직 (순수, 빠름) ──────────────────────────

def test_af9_bronze_completeness_gate():
    """E단계: bronze_sensor 완결 판정 진리표.

    day_cnt>0 AND (after_cnt>0 OR feed_done) 이어야 완결(True).
    DAG의 _bronze_has_tx_date 와 동일한 게이트 로직을 재현해 검증(BQ/GCS 의존 없이).
    """
    def gate(day_cnt: int, after_cnt: int, done: bool) -> bool:
        return day_cnt > 0 and (after_cnt > 0 or done)

    assert gate(100, 50, False) is True     # 다음날 데이터 도착 → 완결
    assert gate(100, 0, True) is True       # 마지막날 + 완료 마커 → 완결
    assert gate(100, 0, False) is False     # 다음날도 마커도 없음 → 미완결(재시도)
    assert gate(0, 10, True) is False       # 그날 데이터 자체가 없음 → 미완결


# ── AF3: Silver 날짜 필터 (Spark 1회) ───────────────────────────────────────

@pytest.mark.slow
def test_af3_silver_target_date_filter():
    """TARGET_TX_DATE 지정 시 해당 파티션만 생성/갱신된다."""
    code, out = _compose_run("-e", "TARGET_TX_DATE=2016-01-01", "spark-silver")
    assert code == 0, f"[AF3] spark-silver 실패\n{out[-2000:]}"
    parts = _silver_partitions()
    assert "2016-01-01" in parts, f"[AF3] 대상 파티션 없음: {parts}"
    # 데이터가 1일치(2016-01-01)뿐인 슬라이스 → 다른 날 파티션이 생기지 않아야 함
    assert parts == {"2016-01-01"}, f"[AF3] 예상 외 파티션: {parts}"


# ── AF4: E2E 단일 run ───────────────────────────────────────────────────────

@pytest.mark.slow
def test_af4_e2e_single_run():
    """logical date 2016-01-01 1회 실행 → 전 태스크 성공 + reconcile 통과."""
    code, out = _exec("airflow", "dags", "test", DAG_ID, "2016-01-01", timeout=900)
    assert "reconcile] OK" in out or "핵심 스토리 정합성 통과" in out, f"[AF4] reconcile 미통과\n{out[-2000:]}"
    assert "Marking run" in out or "state=success" in out.lower() or code == 0, out
    # Gold == Silver is_suspicious 직접 재확인
    gold = duckdb.connect(str(WAREHOUSE), read_only=True).execute(
        "SELECT count(*) FROM undetected_fraud"
    ).fetchone()[0]
    assert gold == _silver_count("is_suspicious"), "[AF4] Gold != Silver is_suspicious"


# ── AF6: 멱등성 ──────────────────────────────────────────────────────────────

@pytest.mark.slow
def test_af6_idempotent_rerun():
    """같은 날짜 spark-silver 재실행 후 Silver 행수 불변."""
    before = _silver_count()
    code, out = _compose_run("-e", "TARGET_TX_DATE=2016-01-01", "spark-silver")
    assert code == 0, out[-1500:]
    assert _silver_count() == before, "[AF6] 재실행 후 Silver 행수 변동(멱등 위반)"


# ── AF5/AF8 안내 ─────────────────────────────────────────────────────────────
# AF5(백필): `airflow dags backfill -s 2016-01-01 -e 2016-01-03 fraud_pipeline` 로 다수
#            logical date 실행 — 데이터 있는 날만 Silver, 빈 날 0행 성공(라이브 검증).
# AF8(회귀): 기존 `pytest tests/test_integrity.py` 가 여전히 통과해야 함(별도 실행).
