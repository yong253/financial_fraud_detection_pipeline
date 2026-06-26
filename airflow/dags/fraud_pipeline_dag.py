"""⑤ Fraud Pipeline DAG — Bronze→Silver→Gold 이벤트시간 일별 증분 배치.

설계(사용자 확정):
  - DAG 스코프 = 배치만. Kafka→Bronze 적재(Spark Streaming, 과도기)는 DAG 밖.
    → DAG는 Bronze 스토리지에서 출발하므로, 향후 Kafka Connect GCS Sink로 교체해도 DAG 불변.
  - 처리 모델 = Model 2(이벤트시간 일별 증분): run 1개 = tx_date 하루치({{ ds }}).
    start_date=2016-01-01 + catchup=True 로 데이터셋 30일 구간을 하루씩 백필.
    (end_date로 한정 — 안 그러면 현재까지 수천 run 생성됨. PaySim=744 step≈31일.)
  - 실행: BashOperator + `docker compose run --rm` (docker.sock). 잡 컨테이너 마운트는
    절대 호스트경로(HOST_PROJECT_DIR)로 해석되어 docker-out-of-docker 경로 문제 없음.

태스크: bronze_sensor → spark_silver({{ds}}) → dbt_run → dbt_test → reconcile
정합성: reconcile 가 undetected_fraud == silver is_suspicious(누적) 검증, 불일치 시 DAG 실패.
"""
from __future__ import annotations

import glob
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

# 컨테이너 내부 경로 (airflow 서비스에 마운트됨)
DATALAKE     = "/datalake"
BRONZE_GLOB  = f"{DATALAKE}/bronze/**/*.parquet"
SILVER_GLOB  = f"{DATALAKE}/silver/tx_date=*/**/*.parquet"
WAREHOUSE    = f"{DATALAKE}/warehouse.duckdb"
FEED_DONE    = f"{DATALAKE}/_feed/ALL_DONE"   # producer --realtime 종료 마커(마지막날 완결 신호)
STEP_EPOCH   = "2016-01-01 00:00:00"          # batch_silver 와 동일 기준(step→tx_date)

PROJECT_DIR  = "/opt/airflow/project"
COMPOSE      = "docker compose -f docker/docker-compose.yml"
PUSHGATEWAY  = "pushgateway:9091"   # ⑥ 모니터링: 배치 records + 사기 KPI push 대상
TOP_N_ACCOUNTS = 10                 # account_risk Top-N 게이지
# 형제 잡(spark/dbt) 컨테이너의 바인드 마운트는 '호스트 절대경로'여야 한다(docker-out-of-docker).
# 스케줄러에 상속된 HOST_PROJECT_DIR(=.. 가능)에 가려지지 않도록 .env에서 직접 export 해 덮어쓴다.
ENV_PREFIX   = f"cd {PROJECT_DIR} && export $(grep -E '^HOST_PROJECT_DIR=' .env) && "

default_args = {
    "owner": "fraud-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _bronze_has_tx_date(ds: str) -> bool:
    """day-by-day 실시간 흐름: 해당 tx_date 데이터가 Bronze에 '완결 도착'했는지 센싱.

    Bronze는 원본 JSON(value)만 보존(ingest 파티션) → step을 파싱해 이벤트시간 tx_date 계산.
    완결 판정(워터마크): 그날 데이터 존재 AND (다음날 데이터도 도착 OR 피드 완료 마커).
      - 다음날이 Bronze에 보이면 = Kafka 오프셋 순서상 그날은 이미 전부 도착(완결).
      - 마지막날은 다음날이 없으므로 producer가 남긴 FEED_DONE 마커로 완결 판정.
    스트리밍이 동시에 쓰는 중 읽기 실패는 False 반환 → 다음 poke 재시도.
    """
    import os

    import duckdb

    if not glob.glob(BRONZE_GLOB, recursive=True):
        return False
    try:
        day_cnt, after_cnt = duckdb.connect().execute(
            f"""
            WITH b AS (
                SELECT (TIMESTAMP '{STEP_EPOCH}'
                        + (CAST(json_extract_string(value, '$.step') AS BIGINT) - 1)
                          * INTERVAL 1 HOUR)::DATE AS tx_date
                FROM read_parquet('{BRONZE_GLOB}')
            )
            SELECT
                COALESCE(SUM(CASE WHEN tx_date = DATE '{ds}' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN tx_date > DATE '{ds}' THEN 1 ELSE 0 END), 0)
            FROM b
            """
        ).fetchone()
    except Exception as e:  # 스트리밍 동시 쓰기 등 일시적 읽기 실패 → 재시도
        print(f"[bronze_sensor] ds={ds} Bronze 읽기 일시 실패: {e} → 다음 poke 재시도")
        return False

    done = os.path.exists(FEED_DONE)
    ok = day_cnt > 0 and (after_cnt > 0 or done)
    print(f"[bronze_sensor] ds={ds} day_cnt={day_cnt} after_cnt={after_cnt} feed_done={done} → {ok}")
    return ok


def _reconcile() -> None:
    """누적 불변식: Gold undetected_fraud 행수 == Silver is_suspicious 행수. 불일치 시 실패."""
    import duckdb

    con = duckdb.connect(WAREHOUSE, read_only=True)
    gold = con.execute("SELECT count(*) FROM undetected_fraud").fetchone()[0]
    con.close()

    silver = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{SILVER_GLOB}', hive_partitioning=true) "
        f"WHERE is_suspicious"
    ).fetchone()[0]

    print(f"[reconcile] undetected_fraud={gold}  silver_is_suspicious={silver}")
    if gold != silver:
        raise ValueError(
            f"정합성 불일치: undetected_fraud({gold}) != silver is_suspicious({silver})"
        )
    print("[reconcile] OK — 핵심 스토리 정합성 통과")


def _push_metrics(ds: str) -> None:
    """⑥ 모니터링: 배치 records + 사기 KPI를 Pushgateway로 push(Prometheus가 스크랩).

    - 일별 그룹(grouping_key=tx_date): 그날 처리/사기 건수를 일자별 series로 보존(백필 누적).
    - 전역 그룹: 누적 headline(미탐지 사기 총건수/마지막 성공 시각) + Top-N 계좌 + 시간대 분포.
    Prometheus는 상세 행에 약하므로 비즈니스 상세는 카운트/Top-N 게이지로 표현(설계 합의).
    """
    import time

    import duckdb
    from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

    # ── 1) 그날 1일치 Silver 집계(이벤트시간 tx_date 파티션) ──
    day = duckdb.connect().execute(
        f"SELECT count(*), "
        f"       COALESCE(SUM(CASE WHEN \"isFraud\"=1 THEN 1 ELSE 0 END),0), "
        f"       COALESCE(SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END),0) "
        f"FROM read_parquet('{SILVER_GLOB}', hive_partitioning=true) "
        f"WHERE CAST(tx_date AS VARCHAR) = '{ds}'"
    ).fetchone()
    day_rows, day_fraud, day_undetected = (day[0] or 0), (day[1] or 0), (day[2] or 0)

    day_reg = CollectorRegistry()
    Gauge("fraud_silver_rows", "tx_date 1일치 Silver 기록 행수", registry=day_reg).set(day_rows)
    Gauge("fraud_day_fraud", "tx_date 1일치 사기(isFraud=1) 건수", registry=day_reg).set(day_fraud)
    Gauge("fraud_day_undetected", "tx_date 1일치 미탐지 사기 건수", registry=day_reg).set(day_undetected)
    pushadd_to_gateway(
        PUSHGATEWAY, job="fraud_pipeline", registry=day_reg, grouping_key={"tx_date": ds}
    )

    # ── 2) 전역 누적 KPI(Gold) ──
    con = duckdb.connect(WAREHOUSE, read_only=True)
    undetected_total = con.execute("SELECT count(*) FROM undetected_fraud").fetchone()[0]
    top_accounts = con.execute(
        "SELECT account_id, fraud_amount FROM account_risk "
        "ORDER BY fraud_amount DESC LIMIT ?", [TOP_N_ACCOUNTS]
    ).fetchall()
    hourly = con.execute(
        "SELECT EXTRACT(hour FROM tx_hour) AS h, SUM(tx_count), SUM(fraud_count) "
        "FROM hourly_summary GROUP BY 1 ORDER BY 1"
    ).fetchall()
    con.close()

    # ── 기존 룰 시스템(isFlaggedFraud) 혼동행렬 — 탐지 성능 검증 KPI ──
    #   actual=실제사기, flagged=기존룰 탐지, tp=맞춘것, fp=오탐, fn=놓침(=is_suspicious=미탐지)
    #   Silver 1회 스캔으로 누적 집계(전역). PaySim은 flagged가 극히 드묾 → recall≈0(스토리).
    cm = duckdb.connect().execute(
        f"SELECT "
        f"  COALESCE(SUM(CASE WHEN \"isFraud\"=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN \"isFlaggedFraud\"=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN \"isFraud\"=1 AND \"isFlaggedFraud\"=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN \"isFraud\"=0 AND \"isFlaggedFraud\"=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END),0) "
        f"FROM read_parquet('{SILVER_GLOB}', hive_partitioning=true)"
    ).fetchone()
    actual, flagged, tp, fp, fn = (cm[0], cm[1], cm[2], cm[3], cm[4])
    precision = (tp / flagged) if flagged else 0.0   # 탐지한 것 중 진짜 사기 비율
    recall    = (tp / actual) if actual else 0.0      # 실제 사기 중 잡은 비율

    g_reg = CollectorRegistry()
    Gauge("fraud_undetected_total", "미탐지 사기 누적 총건수(FN)", registry=g_reg).set(undetected_total)
    Gauge("fraud_suspicious_total", "Silver is_suspicious 누적 건수", registry=g_reg).set(fn)
    Gauge("fraud_actual_total", "실제 사기 누적 건수(isFraud=1)", registry=g_reg).set(actual)
    Gauge("fraud_flagged_total", "기존 룰 탐지 누적 건수(isFlaggedFraud=1)", registry=g_reg).set(flagged)
    Gauge("fraud_true_positive_total", "기존 룰이 맞춘 사기(TP)", registry=g_reg).set(tp)
    Gauge("fraud_false_positive_total", "기존 룰 오탐(FP)", registry=g_reg).set(fp)
    Gauge("fraud_rule_precision", "기존 룰 정밀도 TP/flagged(0~1)", registry=g_reg).set(precision)
    Gauge("fraud_rule_recall", "기존 룰 재현율 TP/actual(0~1)", registry=g_reg).set(recall)
    Gauge(
        "fraud_batch_last_success_timestamp_seconds",
        "마지막 배치 성공 unixtime", registry=g_reg,
    ).set(time.time())

    risk = Gauge("fraud_account_risk_score", "계좌별 누적 사기 금액(Top-N)", ["account"], registry=g_reg)
    for account_id, fraud_amount in top_accounts:
        risk.labels(account=str(account_id)).set(float(fraud_amount or 0))

    tx_by_hour   = Gauge("fraud_hourly_tx", "시간대별 거래 건수", ["hour"], registry=g_reg)
    fraud_by_hour = Gauge("fraud_hourly_fraud", "시간대별 사기 건수", ["hour"], registry=g_reg)
    for hour, tx_count, fraud_count in hourly:
        h = str(int(hour))
        tx_by_hour.labels(hour=h).set(float(tx_count or 0))
        fraud_by_hour.labels(hour=h).set(float(fraud_count or 0))

    pushadd_to_gateway(PUSHGATEWAY, job="fraud_pipeline", registry=g_reg)

    print(
        f"[push_metrics] tx_date={ds} rows={day_rows} day_undetected={day_undetected} | "
        f"actual={actual} flagged={flagged} tp={tp} fp={fp} fn={fn} "
        f"precision={precision:.3f} recall={recall:.3f} | "
        f"undetected_total={undetected_total} top_accounts={len(top_accounts)} hours={len(hourly)} → Pushgateway"
    )


with DAG(
    dag_id="fraud_pipeline",
    description="Bronze→Silver→Gold 이벤트시간 일별 증분 배치 (Medallion)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 1, 4),   # 3일치 흐름 점검용(ds 01-01/02/03) — 전량 검증 시 2016-02-01로 복원
    catchup=True,
    max_active_runs=1,               # 같은 파티션 동시 처리 방지
    tags=["fraud", "medallion", "batch"],
) as dag:

    # {{ ds }} = 처리할 tx_date. 해당 일자 데이터가 Bronze에 완결 도착할 때까지 대기(reschedule).
    bronze_sensor = PythonSensor(
        task_id="bronze_sensor",
        python_callable=_bronze_has_tx_date,
        op_kwargs={"ds": "{{ ds }}"},
        mode="reschedule",               # 대기 중 워커 슬롯 반납(실시간 피드 대기에 적합)
        poke_interval=30,
        timeout=60 * 60 * 2,             # 2h: 해당 일자 데이터 유입까지 충분히 대기
        execution_timeout=timedelta(hours=2),  # default_args의 30m가 센서를 죽이지 않도록 상향
    )

    # {{ ds }} = 논리 날짜 = 처리할 tx_date. f-string 아님(Airflow 템플릿 보존).
    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            ENV_PREFIX + COMPOSE + " run --rm -e TARGET_TX_DATE={{ ds }} spark-silver"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=ENV_PREFIX + COMPOSE + " run --rm dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=ENV_PREFIX + COMPOSE + " run --rm dbt test --profiles-dir .",
    )

    reconcile = PythonOperator(
        task_id="reconcile",
        python_callable=_reconcile,
    )

    # ⑥ 모니터링: 정합성 통과 후 메트릭 push. {{ ds }} = 처리한 tx_date.
    push_metrics = PythonOperator(
        task_id="push_metrics",
        python_callable=_push_metrics,
        op_kwargs={"ds": "{{ ds }}"},
    )

    bronze_sensor >> spark_silver >> dbt_run >> dbt_test >> reconcile >> push_metrics
