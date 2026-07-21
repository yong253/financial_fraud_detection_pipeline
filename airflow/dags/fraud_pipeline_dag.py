"""⑤ Fraud Pipeline DAG — Bronze→Silver→Gold 이벤트시간 일별 증분 배치.

설계(사용자 확정):
  - DAG 스코프 = 배치만. Kafka→Bronze 적재(Kafka Connect GCS Sink, E단계)는 DAG 밖.
    → DAG는 Bronze 스토리지(GCS/BQ)에서 출발하므로 적재 방식 교체와 무관(불변).
  - 처리 모델 = Model 2(이벤트시간 일별 증분): run 1개 = tx_date 하루치({{ ds }}).
    start_date=2016-01-01 + catchup=True 로 데이터셋 30일 구간을 하루씩 백필.
    (end_date로 한정 — 안 그러면 현재까지 수천 run 생성됨. PaySim=744 step≈31일.)
  - 실행: BashOperator + `docker compose run --rm` (docker.sock). 잡 컨테이너 마운트는
    절대 호스트경로(HOST_PROJECT_DIR)로 해석되어 docker-out-of-docker 경로 문제 없음.

태스크: bronze_sensor → spark_silver({{ds}}) → dbt_run → dbt_test → reconcile
정합성: reconcile 가 레이어 간 무손실·무중복을 검증 — undetected_fraud(Gold) ==
  silver is_suspicious(Silver, 누적) 등식으로 Silver→Gold 이동 중 행 유실/중복이 없는지 확인,
  불일치 시 DAG 실패.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

STEP_EPOCH = "2016-01-01 00:00:00"   # batch_silver 와 동일 기준(step→tx_date)

# 단일 웨어하우스 = BigQuery. reconcile/push_metrics/bronze_sensor 가 fraud_bronze/fraud_silver/fraud_gold 조회.
GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "financial-pipeline-501007")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "fraud_bronze")
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "fraud_silver")
BQ_DATASET_GOLD   = os.getenv("BQ_DATASET_GOLD", "fraud_gold")
BQ_BRONZE     = f"`{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.bronze_transactions`"
BQ_SILVER     = f"`{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.silver_transactions`"
BQ_UNDETECTED = f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.undetected_fraud`"
BQ_ACCOUNT    = f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.account_risk`"
BQ_HOURLY     = f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.hourly_summary`"

# E단계: producer --realtime 마지막 날 완결 마커(GCS). Bronze 버킷(JSON 전용) 오염 방지 위해
# staging 버킷에 둔다.
FEED_DONE_URI = os.getenv(
    "FEED_DONE_URI", "gs://financial-pipeline-501007-staging/_feed/ALL_DONE"
)

# D단계: Dataproc Serverless 제출(spark_silver)용
GCP_REGION         = os.getenv("GCP_REGION", "asia-northeast3")
GCS_BUCKET_BRONZE  = os.getenv("GCS_BUCKET_BRONZE", "financial-pipeline-501007-bronze")
GCS_BUCKET_SILVER  = os.getenv("GCS_BUCKET_SILVER", "financial-pipeline-501007-silver")
GCS_BUCKET_STAGING = os.getenv("GCS_BUCKET_STAGING", "financial-pipeline-501007-staging")
GCP_SA_EMAIL       = os.getenv("GCP_SA_EMAIL", "financial-service@financial-pipeline-501007.iam.gserviceaccount.com")
DATAPROC_RUNTIME   = "2.2"
SPARK_CODE_URI     = f"gs://{GCS_BUCKET_STAGING}/code/batch_silver.py"

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


def _gcs_object_exists(gs_uri: str) -> bool:
    """gs://bucket/path 오브젝트 존재 확인 (FEED_DONE 마커용)."""
    from google.cloud import storage

    bucket_name, _, blob_name = gs_uri[len("gs://"):].partition("/")
    return storage.Client(project=GCP_PROJECT_ID).bucket(bucket_name).blob(blob_name).exists()


def _bronze_has_tx_date(ds: str) -> bool:
    """day-by-day 실시간 흐름: 해당 tx_date 데이터가 Bronze에 '완결 도착'했는지 센싱.

    E단계: Bronze는 Kafka Connect GCS Sink가 쓰는 BigQuery 외부테이블(fraud_bronze)로 조회.
    step을 파싱해 이벤트시간 tx_date 계산. 완결 판정(워터마크)은 기존과 동일:
    그날 데이터 존재 AND (다음날 데이터도 도착 OR 피드 완료 마커).
      - 다음날이 Bronze에 보이면 = Kafka 오프셋 순서상 그날은 이미 전부 도착(완결).
      - 마지막날은 다음날이 없으므로 producer가 남긴 FEED_DONE_URI 마커로 완결 판정.
    Kafka Connect가 동시에 쓰는 중 조회 실패(하이브 파티션 미매칭 등)는 False 반환 → 재시도.
    """
    try:
        day_cnt, after_cnt = _bq_query(
            f"""
            WITH b AS (
              SELECT DATE(TIMESTAMP_ADD(TIMESTAMP '{STEP_EPOCH}',
                     INTERVAL (CAST(step AS INT64) - 1) HOUR)) AS tx_date
              FROM {BQ_BRONZE}
            )
            SELECT
              COALESCE(SUM(CASE WHEN tx_date = DATE '{ds}' THEN 1 ELSE 0 END), 0),
              COALESCE(SUM(CASE WHEN tx_date > DATE '{ds}' THEN 1 ELSE 0 END), 0)
            FROM b
            """
        )[0]
    except Exception as e:  # 하이브 파티션 미매칭(빈 버킷) 등 일시적 조회 실패 → 다음 poke 재시도
        print(f"[bronze_sensor] ds={ds} Bronze 조회 일시 실패: {e} → 다음 poke 재시도")
        return False

    done = _gcs_object_exists(FEED_DONE_URI)
    ok = day_cnt > 0 and (after_cnt > 0 or done)
    print(f"[bronze_sensor] ds={ds} day_cnt={day_cnt} after_cnt={after_cnt} feed_done={done} → {ok}")
    return ok


def _bq_query(sql: str):
    """BigQuery 조회 → 행 리스트(튜플). 인증은 GOOGLE_APPLICATION_CREDENTIALS(SA 키)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    return [tuple(row.values()) for row in client.query(sql).result()]


def _reconcile() -> None:
    """레이어 간 무손실·무중복 정합성 검증(누적 불변식): Gold undetected_fraud 행수 ==
    Silver is_suspicious 행수. 다르면 Silver→Gold 이동 중 행이 새거나 겹친 것이므로 실패시킨다."""
    gold = _bq_query(f"SELECT count(*) FROM {BQ_UNDETECTED}")[0][0]
    silver = _bq_query(
        f"SELECT count(*) FROM {BQ_SILVER} WHERE is_suspicious"
    )[0][0]

    print(f"[reconcile] undetected_fraud={gold}  silver_is_suspicious={silver}")
    if gold != silver:
        raise ValueError(
            f"정합성 불일치: undetected_fraud({gold}) != silver is_suspicious({silver})"
        )
    print("[reconcile] OK — 레이어 간 정합성(무손실·무중복) 통과")


def _push_metrics(ds: str) -> None:
    """⑥ 모니터링: 배치 records + 사기 KPI를 Pushgateway로 push(Prometheus가 스크랩).

    전부 전역 그룹(grouping_key 없음) + 풀테이블 집계 → 단일 실행으로 전체 일자/시각을 커버.
    Prometheus는 값을 스크레이프 시각으로 타임스탬프하므로(2016 데이터를 진짜 시간축에 못 그림),
    일자/시각 흐름은 tx_date·tx_hour(epoch millis)를 라벨로 박고 Grafana 순서축(라인/막대)으로 표현.
    """
    import time

    from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

    # ── 1) 일자별(라벨 d_ms = 날짜 자정 epoch millis) 층별 정합성 ──
    # Silver: 처리 행수 + 사기(isFraud=1) + is_suspicious.
    by_date = _bq_query(
        f"SELECT UNIX_MILLIS(TIMESTAMP(tx_date)) AS d_ms, count(*), "
        f"       COALESCE(SUM(CASE WHEN isFraud=1 THEN 1 ELSE 0 END),0), "
        f"       COALESCE(SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END),0) "
        f"FROM {BQ_SILVER} GROUP BY d_ms ORDER BY d_ms"
    )
    # Gold 미탐지(정합성 등식의 Gold 쪽) — 날짜별.
    gold_by_ms = dict(_bq_query(
        f"SELECT UNIX_MILLIS(TIMESTAMP(tx_date)), count(*) FROM {BQ_UNDETECTED} GROUP BY 1"
    ))
    # Bronze "정상·유니크"(품질통과 + row_id DISTINCT) — batch_silver `_reject_reason`/row_id 를 SQL로
    # 미러링(원천: spark/batch_silver.py). 이걸 Silver와 대조하면 Bronze→Silver 무손실·무중복 독립 교차검증.
    # tx_date = 2016-01-01 00:00 + (step-1)h. valid = 아래 reject 조건 전부 미해당.
    bronze_by_ms = dict(_bq_query(
        f"""
        SELECT UNIX_MILLIS(TIMESTAMP(DATE(TIMESTAMP_ADD(TIMESTAMP '{STEP_EPOCH}',
                 INTERVAL (CAST(step AS INT64) - 1) HOUR)))) AS d_ms,
               COUNT(DISTINCT TO_HEX(SHA256(CONCAT(nameOrig,'|',step,'|',type,'|',amount,'|',nameDest)))) AS n
        FROM {BQ_BRONZE}
        WHERE step IS NOT NULL AND amount IS NOT NULL AND nameOrig IS NOT NULL
          AND type IS NOT NULL AND nameDest IS NOT NULL
          AND SAFE_CAST(amount AS FLOAT64) IS NOT NULL AND SAFE_CAST(amount AS FLOAT64) > 0
          AND SAFE_CAST(step AS INT64) BETWEEN 1 AND 743
          AND type IN ('PAYMENT','TRANSFER','CASH_OUT','CASH_IN','DEBIT')
          AND isFraud IN ('0','1') AND isFlaggedFraud IN ('0','1')
          AND NOT COALESCE(SAFE_CAST(oldbalanceOrg AS FLOAT64) < 0, FALSE)
          AND NOT COALESCE(SAFE_CAST(newbalanceOrig AS FLOAT64) < 0, FALSE)
        GROUP BY d_ms
        """
    ))

    # ── 2) 시간순(tx_hour) 집계 — 라벨은 epoch millis(순서/시각 라벨용) ──
    by_hour = _bq_query(
        f"SELECT CAST(UNIX_MILLIS(TIMESTAMP(tx_hour)) AS STRING) AS ms, "
        f"       SUM(tx_count), SUM(fraud_count) "
        f"FROM {BQ_HOURLY} GROUP BY ms ORDER BY ms"
    )

    # ── 3) 거래 유형별(type) 건수 + 위험 계좌 Top-N(사기 건수 랭킹) ──
    by_type = _bq_query(
        f"SELECT type, SUM(tx_count), SUM(fraud_count) FROM {BQ_HOURLY} GROUP BY type ORDER BY type"
    )
    # 사기 "수취" 계좌(nameDest) Top-N — mule 후보. 출발계좌(nameOrig)는 PaySim 특성상 전부 1건이라
    # 무의미 → 목적지 기준 사기 수신 건수로. Silver 직접 조회.
    mule_accounts = _bq_query(
        f"SELECT nameDest, COUNT(*) FROM {BQ_SILVER} WHERE isFraud=1 "
        f"GROUP BY nameDest ORDER BY 2 DESC LIMIT {TOP_N_ACCOUNTS}"
    )

    # ── 4) 전역 누적 KPI + 기존 룰 혼동행렬 ──
    undetected_total = _bq_query(f"SELECT count(*) FROM {BQ_UNDETECTED}")[0][0]
    #   actual=실제사기, flagged=기존룰 탐지, tp=맞춘것, fp=오탐, fn=놓침(=is_suspicious=미탐지)
    #   Silver 1회 스캔으로 누적 집계(전역). PaySim은 flagged가 극히 드묾 → recall≈0(스토리).
    cm = _bq_query(
        f"SELECT "
        f"  COALESCE(SUM(CASE WHEN isFraud=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN isFlaggedFraud=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN isFraud=1 AND isFlaggedFraud=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN isFraud=0 AND isFlaggedFraud=1 THEN 1 ELSE 0 END),0), "
        f"  COALESCE(SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END),0) "
        f"FROM {BQ_SILVER}"
    )[0]
    actual, flagged, tp, fp, fn = (cm[0], cm[1], cm[2], cm[3], cm[4])
    precision = (tp / flagged) if flagged else 0.0   # 탐지한 것 중 진짜 사기 비율
    recall    = (tp / actual) if actual else 0.0      # 실제 사기 중 잡은 비율
    # 레이어 정합성: Gold 미탐지 건수 == Silver is_suspicious 건수여야 무손실·무중복(reconcile 등식).
    reconcile_match = 1.0 if undetected_total == fn else 0.0

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
    Gauge(
        "fraud_reconcile_match",
        "레이어 정합성(Gold 미탐지==Silver is_suspicious): 1=정합/0=불일치", registry=g_reg,
    ).set(reconcile_match)

    # 일자별(d_ms) — 처리 행수(추세) + 사기 건수 + 층별 정합성 차이(무결성 시 0).
    d_rows  = Gauge("fraud_by_date_rows", "일자별 Silver 처리 행수", ["d_ms"], registry=g_reg)
    d_fraud = Gauge("fraud_by_date_fraud", "일자별 사기(isFraud=1) 건수", ["d_ms"], registry=g_reg)
    d_bs    = Gauge("fraud_by_date_bs_diff",
                    "Bronze(정상·유니크) − Silver (Bronze→Silver 무손실·무중복, 0이면 정합)",
                    ["d_ms"], registry=g_reg)
    d_sg    = Gauge("fraud_by_date_sg_diff",
                    "Silver is_suspicious − Gold 미탐지 (Silver→Gold 정합, 0이면 정합)",
                    ["d_ms"], registry=g_reg)
    bs_max = sg_max = 0
    for d_ms, rows, fr, susp in by_date:
        key = str(d_ms)
        bs = int(bronze_by_ms.get(d_ms, 0)) - int(rows or 0)
        sg = int(susp or 0) - int(gold_by_ms.get(d_ms, 0))
        d_rows.labels(d_ms=key).set(float(rows or 0))
        d_fraud.labels(d_ms=key).set(float(fr or 0))
        d_bs.labels(d_ms=key).set(float(bs))
        d_sg.labels(d_ms=key).set(float(sg))
        bs_max = max(bs_max, abs(bs)); sg_max = max(sg_max, abs(sg))

    # 시간순(tx_hour, epoch millis 라벨) — 거래/사기 별도 추세.
    h_tx    = Gauge("fraud_by_hour_tx", "시간(tx_hour)별 거래 건수", ["ts_ms"], registry=g_reg)
    h_fraud = Gauge("fraud_by_hour_fraud", "시간(tx_hour)별 사기 건수", ["ts_ms"], registry=g_reg)
    for ms, tx_count, fraud_count in by_hour:
        h_tx.labels(ts_ms=str(ms)).set(float(tx_count or 0))
        h_fraud.labels(ts_ms=str(ms)).set(float(fraud_count or 0))

    # 거래 유형별 건수.
    type_tx    = Gauge("fraud_type_tx", "거래 유형별 거래 건수", ["type"], registry=g_reg)
    type_fraud = Gauge("fraud_type_fraud", "거래 유형별 사기 건수", ["type"], registry=g_reg)
    for tx_type, t_tx, t_fraud in by_type:
        type_tx.labels(type=str(tx_type)).set(float(t_tx or 0))
        type_fraud.labels(type=str(tx_type)).set(float(t_fraud or 0))

    # 사기 수취 계좌(nameDest) Top-N — mule 후보.
    mule = Gauge("fraud_mule_recv_count", "사기 수취 계좌 건수(nameDest, Top-N mule 후보)", ["account"], registry=g_reg)
    for dest, cnt in mule_accounts:
        mule.labels(account=str(dest)).set(float(cnt or 0))

    # DAG run 성공/실패 — 트리거 날짜(logical date=tx_date)별. Prometheus airflow_* 엔 날짜 라벨이 없어
    # Airflow 메타DB(DagRun)를 ORM으로 조회. 라벨 d_ms(날짜 자정 epoch ms)로 다른 일자별 패널과 통일.
    from collections import defaultdict
    from datetime import datetime as _dt, timezone as _tz

    from airflow.models import DagRun
    from airflow.utils.session import create_session

    dr_succ, dr_fail, dr_seen = defaultdict(int), defaultdict(int), set()
    with create_session() as _s:
        for ex, st in _s.query(DagRun.execution_date, DagRun.state).filter(
            DagRun.dag_id == "fraud_pipeline"
        ).all():
            ms = int(_dt(ex.year, ex.month, ex.day, tzinfo=_tz.utc).timestamp() * 1000)
            dr_seen.add(ms)
            if st == "success":
                dr_succ[ms] += 1
            elif st == "failed":
                dr_fail[ms] += 1
    g_dr_ok = Gauge("fraud_dagrun_success", "트리거 날짜별 DAG run 성공 수", ["d_ms"], registry=g_reg)
    g_dr_ng = Gauge("fraud_dagrun_failed", "트리거 날짜별 DAG run 실패 수", ["d_ms"], registry=g_reg)
    for ms in sorted(dr_seen):
        g_dr_ok.labels(d_ms=str(ms)).set(float(dr_succ.get(ms, 0)))
        g_dr_ng.labels(d_ms=str(ms)).set(float(dr_fail.get(ms, 0)))

    pushadd_to_gateway(PUSHGATEWAY, job="fraud_pipeline", registry=g_reg)

    print(
        f"[push_metrics] ds={ds} | "
        f"actual={actual} flagged={flagged} tp={tp} fp={fp} fn={fn} "
        f"precision={precision:.3f} recall={recall:.3f} reconcile_match={reconcile_match:.0f} | "
        f"bs_diff_max={bs_max} sg_diff_max={sg_max} (0=정합) | "
        f"undetected_total={undetected_total} dates={len(by_date)} hours={len(by_hour)} "
        f"types={len(by_type)} mule_accounts={len(mule_accounts)} dagruns={len(dr_seen)} → Pushgateway"
    )


with DAG(
    dag_id="fraud_pipeline",
    description="Bronze→Silver→Gold 이벤트시간 일별 증분 배치 (Medallion)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 1, 6),   # 5일치 백필용(ds 01-01~01-05) — 전량 검증 시 2016-02-01로 복원
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

    # 최신 batch_silver.py 를 GCS에 동기화 — Dataproc이 gs:// 코드를 실행하므로 스테일 방지.
    upload_spark_code = LocalFilesystemToGCSOperator(
        task_id="upload_spark_code",
        src=f"{PROJECT_DIR}/spark/batch_silver.py",
        dst="code/batch_silver.py",
        bucket=GCS_BUCKET_STAGING,
    )

    # {{ ds }} = 처리할 tx_date. Dataproc Serverless 배치로 Bronze(gs://) → Silver(gs://).
    # batch_id는 제출마다 uuid8 suffix로 유니크(과거 배치 재부착/no-op 방지). 연산자는 동일 ID가
    # 이미 있으면 새로 돌리지 않고 기존(완료된) 배치에 attach 후 SUCCESS 처리해버려서, DAG 이력
    # 삭제 후 재실행(try_number가 1로 리셋)하면 예전 배치와 조용히 충돌하는 문제가 실측됨.
    # 서브넷 미지정=기본(PGA on).
    spark_silver = DataprocCreateBatchOperator(
        task_id="spark_silver",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch_id="silver-{{ ds_nodash }}-{{ macros.uuid.uuid4().hex[:8] }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SPARK_CODE_URI,
                "args": [
                    # topics/transactions = Kafka Connect GCS Sink 실제 적재 경로(topics.dir=topics
                    # 기본값). 버킷 루트를 그대로 읽으면 과거 스모크테스트 잔여물과 파티션 구조가
                    # 충돌해 Spark가 "Conflicting directory structures" 로 실패한다(실측 확인).
                    f"--bronze-path=gs://{GCS_BUCKET_BRONZE}/topics/transactions",
                    f"--silver-path=gs://{GCS_BUCKET_SILVER}",
                    "--target-tx-date={{ ds }}",
                ],
            },
            "runtime_config": {"version": DATAPROC_RUNTIME},
            "environment_config": {
                "execution_config": {"service_account": GCP_SA_EMAIL}
            },
        },
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

    bronze_sensor >> spark_silver
    upload_spark_code >> spark_silver
    spark_silver >> dbt_run >> dbt_test >> reconcile >> push_metrics
