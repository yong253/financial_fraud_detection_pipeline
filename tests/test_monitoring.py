"""⑥ 모니터링 테스트 (MON1~MON7).

설정/구조 검증(MON1·MON2·MON3)은 빠르게, 헬스/푸시 E2E(MON4·MON5·MON6)는 실제
컨테이너로 검증한다. 무거운 것은 `@pytest.mark.slow`.

전제(slow):
  docker compose -f docker/docker-compose.yml --profile monitoring up -d  # 모니터링 스택
  + ⑤ 파이프라인이 1회 실행되어 BigQuery Gold(`fraud_gold.undetected_fraud`)가 존재.

실행:
  pytest tests/test_monitoring.py -v                 # 전체
  pytest tests/test_monitoring.py -v -m "not slow"   # 빠른 설정/구조만
"""
import json
import os
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest
import yaml

ROOT       = Path(__file__).parent.parent
COMPOSE    = ["docker", "compose", "-f", str(ROOT / "docker" / "docker-compose.yml")]
SCHEDULER  = "airflow-scheduler"
DS_UID     = "fraud-prometheus"   # provisioning 데이터소스 uid (대시보드가 참조)

# MON6: Gold(BigQuery) 대조용. DAG의 기본값과 동일(env 폴백).
GCP_PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "financial-pipeline-501007")
BQ_DATASET_GOLD = os.getenv("BQ_DATASET_GOLD", "fraud_gold")

PROM_YML   = ROOT / "prometheus" / "prometheus.yml"
STATSD_YML = ROOT / "prometheus" / "statsd_mapping.yml"
DS_YML     = ROOT / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
PROV_YML   = ROOT / "grafana" / "provisioning" / "dashboards" / "provider.yml"
DASH_JSON  = ROOT / "grafana" / "dashboards" / "fraud_overview.json"

MON_SERVICES = {"prometheus", "pushgateway", "grafana", "kafka-exporter", "statsd-exporter"}


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _http(url: str, timeout: int = 10) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def _exec(*args, timeout=300):
    r = subprocess.run(
        ["docker", "exec", SCHEDULER, *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
    )
    return r.returncode, r.stdout + r.stderr


def _iter_panels(dash: dict):
    for p in dash.get("panels", []):
        yield p
        for sub in p.get("panels", []):
            yield sub


# ── MON1: 설정 파일 파싱 ─────────────────────────────────────────────────────

def test_mon1_config_files_parse():
    """prometheus/statsd/grafana provisioning YAML + 대시보드 JSON 이 파싱돼야 한다."""
    for f in (PROM_YML, STATSD_YML, DS_YML, PROV_YML):
        assert f.exists(), f"[MON1] 누락: {f}"
        yaml.safe_load(f.read_text(encoding="utf-8"))
    dash = json.loads(DASH_JSON.read_text(encoding="utf-8"))
    assert dash["uid"] == "fraud-overview", "[MON1] 대시보드 uid 불일치"

    # prometheus 스크레이프에 3개 핵심 job 존재
    prom = yaml.safe_load(PROM_YML.read_text(encoding="utf-8"))
    jobs = {j["job_name"] for j in prom["scrape_configs"]}
    assert {"pushgateway", "kafka", "airflow"} <= jobs, f"[MON1] scrape job 누락: {jobs}"


# ── MON2: 데이터소스 uid 일관성 (대시보드 ↔ provisioning) ───────────────────

def test_mon2_dashboard_datasource_uid_consistency():
    """provisioning 데이터소스 uid 와 대시보드 패널/타깃 datasource uid 가 일치해야 한다."""
    ds = yaml.safe_load(DS_YML.read_text(encoding="utf-8"))
    prov_uid = ds["datasources"][0]["uid"]
    assert prov_uid == DS_UID, f"[MON2] provisioning uid={prov_uid}"

    dash = json.loads(DASH_JSON.read_text(encoding="utf-8"))
    refs = []
    for p in _iter_panels(dash):
        if p.get("type") == "row":
            continue
        if isinstance(p.get("datasource"), dict):
            refs.append(p["datasource"].get("uid"))
        for t in p.get("targets", []):
            if isinstance(t.get("datasource"), dict):
                refs.append(t["datasource"].get("uid"))
    assert refs, "[MON2] 패널 datasource 참조 없음"
    bad = {u for u in refs if u != DS_UID}
    assert not bad, f"[MON2] uid 불일치 참조: {bad} (기대 {DS_UID})"


# ── MON3: compose 렌더 + 모니터링 서비스 존재 ───────────────────────────────

def test_mon3_compose_profile_renders():
    """`docker compose --profile monitoring config` 가 5개 서비스를 포함해 렌더돼야 한다."""
    r = subprocess.run(
        [*COMPOSE, "--profile", "monitoring", "config", "--services"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=120, cwd=ROOT,
    )
    assert r.returncode == 0, f"[MON3] config 실패\n{r.stderr}"
    services = set(r.stdout.split())
    assert MON_SERVICES <= services, f"[MON3] 모니터링 서비스 누락: {MON_SERVICES - services}"


# ── MON4: promtool 설정 검증 (prom 컨테이너) ────────────────────────────────

@pytest.mark.slow
def test_mon4_promtool_check_config():
    """prometheus.yml 이 promtool 검증을 통과해야 한다."""
    r = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "promtool",
         "-v", f"{PROM_YML.as_posix()}:/p.yml:ro",
         "prom/prometheus:v2.53.0", "check", "config", "/p.yml"],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300,
    )
    assert r.returncode == 0, f"[MON4] promtool 실패\n{r.stdout}{r.stderr}"
    assert "SUCCESS" in (r.stdout + r.stderr)


# ── MON5: 모니터링 스택 헬스 ────────────────────────────────────────────────

@pytest.mark.slow
def test_mon5_stack_health():
    """Prometheus/Pushgateway/Grafana/kafka-exporter 헬스 엔드포인트가 살아있어야 한다."""
    code, _ = _http("http://localhost:9090/-/healthy")
    assert code == 200, "[MON5] prometheus 비정상"

    code, _ = _http("http://localhost:9091/-/healthy")
    assert code == 200, "[MON5] pushgateway 비정상"

    code, body = _http("http://localhost:3000/api/health")
    assert code == 200, "[MON5] grafana 비정상"

    code, body = _http("http://localhost:9308/metrics")
    assert code == 200 and "kafka_brokers" in body, "[MON5] kafka-exporter 메트릭 없음"


# ── MON6: push_metrics E2E (Pushgateway + Prometheus 스크랩) ─────────────────

@pytest.mark.slow
def test_mon6_push_metrics_e2e():
    """push_metrics 태스크 실행 → Pushgateway 에 fraud_undetected_total 노출 + Prometheus 스크랩."""
    code, out = _exec(
        "airflow", "tasks", "test", "fraud_pipeline", "push_metrics", "2016-01-01", timeout=300
    )
    assert "push_metrics] tx_date=2016-01-01" in out, f"[MON6] push 실행 로그 없음\n{out[-1500:]}"

    code, body = _http("http://localhost:9091/metrics")
    assert code == 200 and "fraud_undetected_total" in body, "[MON6] Pushgateway 에 KPI 없음"

    # Prometheus query API 로 스크랩 확인 (값이 Gold undetected_fraud 와 동일해야 함).
    # 스크레이프 간격(15s) 때문에 푸시 직후엔 비어 있을 수 있어 최대 ~45s 폴링.
    import time

    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    gold = list(client.query(
        f"SELECT count(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.undetected_fraud`"
    ).result())[0][0]
    results = []
    for _ in range(15):
        code, body = _http("http://localhost:9090/api/v1/query?query=fraud_undetected_total")
        results = json.loads(body)["data"]["result"]
        if results:
            break
        time.sleep(3)
    assert results, "[MON6] Prometheus 가 스크랩하지 않음(45s 대기 후)"
    assert int(float(results[0]["value"][1])) == gold, "[MON6] Prometheus 값 != Gold undetected_fraud"


# ── MON8: 룰 탐지 성능 KPI 메트릭 노출 ──────────────────────────────────────

@pytest.mark.slow
def test_mon8_rule_performance_metrics():
    """push_metrics 실행 후 혼동행렬/Precision·Recall 메트릭이 Pushgateway에 노출돼야 한다."""
    code, out = _exec(
        "airflow", "tasks", "test", "fraud_pipeline", "push_metrics", "2016-01-01", timeout=300
    )
    assert "precision=" in out and "recall=" in out, f"[MON8] push 로그에 KPI 없음\n{out[-1500:]}"

    code, body = _http("http://localhost:9091/metrics")
    assert code == 200, "[MON8] Pushgateway 응답 없음"
    for metric in ("fraud_actual_total", "fraud_flagged_total", "fraud_true_positive_total",
                   "fraud_rule_precision", "fraud_rule_recall"):
        assert metric in body, f"[MON8] 메트릭 누락: {metric}"


# ── MON7 안내 ────────────────────────────────────────────────────────────────
# MON7(회귀): 기존 `pytest tests/test_airflow_dag.py` 가 여전히 통과해야 함(별도 실행).
#            push_metrics 추가 후 DAG 구조/정합성 불변 확인(test_integrity.py는 Part2에서 제거됨).
