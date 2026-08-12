"""⑤ Fraud Pipeline DAG — Bronze→Silver→Gold 이벤트시간 일별 증분 배치.

설계(사용자 확정):
  - DAG 스코프 = 배치만. Kafka→Bronze 적재(Kafka Connect GCS Sink, E단계)는 DAG 밖.
    → DAG는 Bronze 스토리지(GCS/BQ)에서 출발하므로 적재 방식 교체와 무관(불변).
  - 처리 모델 = Model 2(이벤트시간 일별 증분): run 1개 = tx_date 하루치({{ ds }}).
    start_date=2016-01-01 + catchup=True 로 데이터셋 30일 구간을 하루씩 백필.
    (end_date로 한정 — 안 그러면 현재까지 수천 run 생성됨. PaySim=step 1~743 → 정확히 31일.)
  - 실행: BashOperator + `docker compose run --rm` (docker.sock). 잡 컨테이너 마운트는
    절대 호스트경로(HOST_PROJECT_DIR)로 해석되어 docker-out-of-docker 경로 문제 없음.
  - STEP_EPOCH(step→이벤트일 기준시각)는 이 DAG가 단일 출처로 소유한다. **Spark에는 넘기지
    않는다** — batch_silver 는 tx_date 를 event_time 에서 직접 파생한다. 이 값이 쓰이는 곳은
    reconcile 의 Bronze 집계 SQL(step→이벤트일 재계산)뿐이다.

태스크: bronze_sensor      ─┐
       upload_spark_code  ─┴→ spark_silver({{ds}}) → dbt_run → dbt_test → reconcile → push_metrics
       (앞 두 개는 병렬 — 센서가 대기하는 동안 코드 업로드가 끝난다)
정합성: reconcile 이 Bronze→Silver 무손실·무중복을 검증 — Bronze(품질통과·row_id 유니크)
  행수 == Silver 행수를 이벤트일별로 대조(tx_date <= ds), 불일치 시 DAG 실패.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.sensors.python import PythonSensor

from airflow import DAG


def _req(name: str) -> str:
    """필수 환경변수. compose가 이미 :? 로 강제하므로 정상 경로에선 항상 설정돼 있다 —
    누락 시 DAG import 단계에서 즉시 실패시켜 조용한 개인값 폴백을 막는다."""
    v = os.getenv(name)
    if not v:
        raise ValueError(f"필수 환경변수 미설정: {name} — .env 확인")
    return v


# 단일 웨어하우스 = BigQuery. reconcile/push_metrics/bronze_sensor 가 fraud_bronze/fraud_silver/fraud_gold 조회.
GCP_PROJECT_ID    = _req("GCP_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "fraud_bronze")
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "fraud_silver")
BQ_DATASET_GOLD   = os.getenv("BQ_DATASET_GOLD", "fraud_gold")
BQ_BRONZE     = f"`{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.bronze_transactions`"
BQ_SILVER     = f"`{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.silver_transactions`"
BQ_UNDETECTED = f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.undetected_fraud`"
BQ_ACCOUNT    = f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.account_risk`"
BQ_HOURLY     = f"`{GCP_PROJECT_ID}.{BQ_DATASET_GOLD}.hourly_summary`"

# step=1 의 절대 시각. **producer가 event_time을 만들 때 쓰는 값과 같아야 한다**
# (리터럴 기본값은 docker-compose.yml 의 `${STEP_EPOCH:-...}` 한 곳).
# 여기서는 reconcile 의 **교차검증**에만 쓴다 — Bronze 행수를 셀 때 date= 파티션(=producer의
# event_time에서 나옴) 대신 step 에서 날짜를 계산해서 센다. 두 값이 어긋나면 그 자체로
# reconcile 이 불일치를 잡아낸다(같은 출처끼리 비교하면 교차검증이 성립하지 않는다).
# batch_silver 는 더 이상 이 값을 받지 않는다 — tx_timestamp 를 event_time 에서 직접 만든다.
STEP_EPOCH = _req("STEP_EPOCH")

# D단계: Dataproc Serverless 제출(spark_silver)용
GCP_REGION         = os.getenv("GCP_REGION", "asia-northeast3")
GCS_BUCKET_BRONZE  = _req("GCS_BUCKET_BRONZE")
GCS_BUCKET_SILVER  = _req("GCS_BUCKET_SILVER")
GCS_BUCKET_STAGING = _req("GCS_BUCKET_STAGING")
GCP_SA_EMAIL       = _req("GCP_SA_EMAIL")
DATAPROC_RUNTIME   = "2.2"
SPARK_CODE_URI     = f"gs://{GCS_BUCKET_STAGING}/code/batch_silver.py"

PROJECT_DIR  = "/opt/airflow/project"
# compose 파일이 프로젝트 루트에 있으므로 project directory = /opt/airflow/project → 그 안의
# .env(=호스트 루트 .env, 전체 프로젝트 바인드 마운트로 접근 가능)가 자동 로드된다.
# -f를 절대경로로 줘 BashOperator의 임시 cwd와 무관하게 항상 정확히 해석되게 한다.
COMPOSE      = f"docker compose -f {PROJECT_DIR}/docker-compose.yml"
PUSHGATEWAY  = "pushgateway:9091"   # ⑥ 모니터링: 배치 records + 사기 KPI push 대상
TOP_N_ACCOUNTS = 10                 # account_risk Top-N 게이지

default_args = {
    "owner": "fraud-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 전량(6.36M) 기준 상향: batch_silver 가 매 run Bronze 전량을 스캔하므로(파티션 키가
    # 이벤트시간이 아니라 인제스트 시각이라 프루닝 불가) 30분은 마진이 부족하다 — 31 run
    # 백필 도중 타임아웃으로 깨지는 것을 막는다. (bronze_sensor는 아래에서 2h로 별도 지정)
    "execution_timeout": timedelta(minutes=60),
}


def _bronze_has_tx_date(ds: str) -> bool:
    """해당 tx_date 데이터가 Bronze에 '완결 도착'했는지 센싱 — EOD 마커 기준.

    **판정: 그날 EOD 마커 수 == 토픽 파티션 수.**

    producer가 하루치를 다 보낸 뒤 모든 파티션에 마커를 하나씩 끼워넣는다. Connect는 오프셋
    순서로 파일을 쓰고 **GCS 업로드 후에** 오프셋을 커밋하므로, 파티션 P의 마커가 Bronze에
    보이면 P의 그날 데이터는 전부 GCS에 있다(파티션 내 순서 보장). 모든 파티션에서 참이면
    그날 완결.

    이전 판정 `day_cnt > 0 AND (after_cnt > 0 OR feed_done)` 은 **틀린 전제**에 기댔다:
      - "다음날이 보이면 어제는 완결"은 파티션이 1개일 때만 참이다. Connect가 tasks.max=3 으로
        파티션마다 독립 flush 하므로 파티션 1이 4일차를 쓰는 동안 파티션 0은 3일차 중간일 수
        있다(실측: run 1이 Bronze 약 72% 상태에서 통과).
      - FEED_DONE 마커는 producer가 GCS에 **직접** 썼다 — Kafka와 Connect를 건너뛴 신호라
        "Kafka에 다 넣었다"만 증명할 뿐 Bronze 도착과 무관했다.
    완결 신호는 데이터와 같은 경로로 흘러야 그 경로의 완료를 증명한다.

    파티션 수는 하드코딩하지 않는다 — 마커 payload의 total_partitions 를 그대로 쓴다.
    Kafka Connect가 동시에 쓰는 중 조회 실패(하이브 파티션 미매칭 등)는 False 반환 → 재시도.

    **`date` 술어는 하이브 파티션 프루닝용이다.** 없으면 마커 3행을 확인하려고 Bronze 전량을
    읽는다 — 실측으로 poke 1회가 1.91GB를 스캔했고, 31일 백필에서 BigQuery 슬롯의 95%가
    Bronze를 읽는 쿼리에 쓰였다. `record_type`·`tx_date` 는 JSON 페이로드 필드라 프루닝이
    걸리지 않는다(파티션 컬럼은 폴더명에서 온 `date` 뿐).

    `tx_date` 조건은 프루닝을 넣은 뒤에도 남긴다 — `date` 는 폴더(producer의 `event_time`
    유래)이고 `tx_date` 는 마커가 스스로 선언한 날짜라 **출처가 다르다**. 둘 다 요구하면
    서로를 교차검증하게 되고, `event_time` 주입이 어긋나면 완결 판정이 서지 않아 드러난다.
    (실측 근거: 마커 93건 전부 `date == tx_date`, 프루닝 전후 판정 동일(3/3),
     스캔 1.91GB → 81KB~172MB(그날 거래량에 비례).)
    """
    try:
        rows = _bq_query(
            f"""
            SELECT COUNT(DISTINCT kafka_partition) AS seen,
                   MAX(total_partitions)           AS expected
            FROM {BQ_BRONZE}
            WHERE date = DATE '{ds}'
              AND record_type = 'eod_marker' AND tx_date = '{ds}'
            """
        )
    except Exception as e:  # noqa: BLE001 — 하이브 파티션 미매칭(빈 버킷) 등 일시적 조회 실패 전부를
        # 재시도 대상으로 잡기 위한 의도적 광범위 except(센서 포크 실패시키지 않음)
        print(f"[bronze_sensor] ds={ds} Bronze 조회 일시 실패: {e} → 다음 poke 재시도")
        return False

    seen, expected = (rows[0] if rows else (0, None))
    seen = int(seen or 0)
    ok = expected is not None and seen == int(expected)
    print(f"[bronze_sensor] ds={ds} eod_markers={seen}/{expected} → {ok}")
    return ok


def _bq_query(sql: str):
    """BigQuery 조회 → 행 리스트(튜플). 인증은 GOOGLE_APPLICATION_CREDENTIALS(SA 키)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    return [tuple(row.values()) for row in client.query(sql).result()]


def _bronze_valid_by_date(max_ds: str | None = None) -> dict:
    """Bronze의 "품질통과 + row_id 유니크" 행수를 이벤트일(tx_date)별로 집계 → {d_ms: n}.

    batch_silver 의 `_reject_reason`/`row_id` 정의를 SQL로 미러링한 **독립 교차검증**이다
    — Spark 코드와 다른 경로로 같은 답을 구해 대조하므로, 한쪽 버그가 다른 쪽에 가려지지
    않는다. (원천: spark/batch_silver.py. 그쪽 품질조건/row_id 를 바꾸면 여기도 함께 고칠 것.)

    **날짜를 `date=` 파티션이 아니라 `step` 에서 계산하는 것이 핵심이다.** `date=` 는
    producer의 `event_time` 에서 나오고 Silver `tx_date` 도 같은 값에서 나오므로, 그대로
    비교하면 같은 출처끼리 대조하는 셈이라 교차검증이 성립하지 않는다. step 기준으로 세면
    `event_time` 주입이 잘못된 경우까지 불일치로 드러난다.
    `date` 는 **프루닝에만** 쓴다(`max_ds` 지정 시).

    EOD 마커는 `record_type IS NOT NULL` 로 명시 제외한다 — 품질 조건에도 어차피 걸리지만,
    "거래만 센다"는 의도를 코드에 남긴다.

    `_reconcile`(검증)과 `_push_metrics`(관측)가 공유한다 — SQL을 복사하면 row_id 정의가
    두 벌로 갈라져 조용히 어긋난다.
    """
    prune = f"AND date <= DATE '{max_ds}'" if max_ds else ""
    return dict(_bq_query(
        f"""
        SELECT UNIX_MILLIS(TIMESTAMP(DATE(TIMESTAMP_ADD(TIMESTAMP '{STEP_EPOCH}',
                 INTERVAL (CAST(step AS INT64) - 1) HOUR)))) AS d_ms,
               COUNT(DISTINCT TO_HEX(SHA256(CONCAT(nameOrig,'|',step,'|',type,'|',amount,'|',nameDest)))) AS n
        FROM {BQ_BRONZE}
        WHERE record_type IS NULL                        -- EOD 마커 제외(거래만)
          {prune}
          AND step IS NOT NULL AND amount IS NOT NULL AND nameOrig IS NOT NULL
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


def _reconcile(ds: str) -> None:
    """레이어 간 무손실·무중복 검증: Bronze(품질통과·유니크) == Silver, 이벤트일(tx_date)별.

    **왜 Bronze↔Silver인가** — 행이 실제로 샐 수 있는 경계가 여기다: 상시 적재로 움직이는
    Bronze를 읽고, tx_date로 거르고, dedup으로 지우고, quarantine으로 분기한다.

    반면 Silver→Gold는 BigQuery 내부 CTAS라 행이 새는 구간이 아니다. 게다가 이전 등식
    (`count(undetected_fraud) == count(silver WHERE is_suspicious)`)은 **항상 참이었다** —
    undetected_fraud 가 바로 그 Silver를 그 술어로 걸러 만든 테이블이라(dbt gold 모델 →
    stg_silver_transactions → silver), 하루가 통째로 유실돼도 양쪽이 똑같이 줄어 통과했다.
    검증처럼 보였을 뿐 아무것도 잡지 못했으므로 제거했다.

    **검사 범위 = tx_date <= ds.** ds 하루만 보면 이전 일자가 나중에 깨지는 경우(dynamic
    overwrite가 다른 파티션을 덮는 등)를 놓치고, 전 일자를 보면 백필 도중 아직 처리하지
    않은 미래 일자가 Bronze에만 있어 유실로 오판된다(실측으로 확인). ds 이하 = "지금까지
    처리했어야 할 전부"가 정확한 경계다.
    """
    ds_ms = int(datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)

    # max_ds 로 `date=` 파티션 프루닝(스캔량 감소). 건수 자체는 step 기준으로 세므로
    # event_time↔step 불일치도 그대로 드러난다.
    bronze = {k: v for k, v in _bronze_valid_by_date(max_ds=ds).items() if k <= ds_ms}
    silver = {
        k: v for k, v in _bq_query(
            f"SELECT UNIX_MILLIS(TIMESTAMP(tx_date)), count(*) FROM {BQ_SILVER} GROUP BY 1"
        ) if k <= ds_ms
    }

    # 빈 결과로 "검사할 게 없어 통과"하는 것이 최악이다(이전 등식이 그랬다) → 명시적으로 막는다.
    if not bronze:
        raise ValueError(
            f"정합성 검증 불가 — ds={ds} 이하 Bronze 집계가 비어 있다"
            " (적재 실패 또는 외부테이블 조회 오류)"
        )

    # 한쪽에만 존재하는 일자야말로 잡아야 할 유실이므로 합집합으로 순회한다(결측=0).
    mismatches = []
    for d_ms in sorted(set(bronze) | set(silver)):
        b, s = int(bronze.get(d_ms, 0)), int(silver.get(d_ms, 0))
        if b != s:
            day = datetime.fromtimestamp(d_ms / 1000, tz=timezone.utc).date()
            mismatches.append((day, b, s))

    if mismatches:
        detail = "\n".join(
            f"  {day}  bronze={b:,}  silver={s:,}  diff={b - s:+,}"
            for day, b, s in mismatches
        )
        raise ValueError(
            f"정합성 불일치 {len(mismatches)}일 "
            f"(Bronze 품질통과·유니크 != Silver 행수, tx_date <= {ds}):\n{detail}"
        )

    total = sum(int(v) for v in bronze.values())
    print(
        f"[reconcile] OK — Bronze→Silver 무손실·무중복 통과 | "
        f"tx_date <= {ds}, 검사 일자 {len(bronze)}일, 총 {total:,}행"
    )


def _push_metrics(ds: str) -> None:
    """⑥ 모니터링: 배치 records + 사기 KPI를 Pushgateway로 push(Prometheus가 스크랩).

    전부 전역 그룹(grouping_key 없음) + 풀테이블 집계 → 단일 실행으로 전체 일자/시각을 커버.
    Prometheus는 값을 스크레이프 시각으로 타임스탬프하므로(2016 데이터를 진짜 시간축에 못 그림),
    일자/시각 흐름은 tx_date·tx_hour(epoch millis)를 라벨로 박고 Grafana 순서축(라인/막대)으로 표현.
    """
    import time

    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    # ── 1) 일자별(라벨 d_ms = 날짜 자정 epoch millis) 층별 정합성 ──
    # Silver: 처리 행수 + 사기(isFraud=1) + is_suspicious.
    by_date = _bq_query(
        f"SELECT UNIX_MILLIS(TIMESTAMP(tx_date)) AS d_ms, count(*), "
        f"       COALESCE(SUM(CASE WHEN isFraud=1 THEN 1 ELSE 0 END),0), "
        f"       COALESCE(SUM(CASE WHEN is_suspicious THEN 1 ELSE 0 END),0) "
        f"FROM {BQ_SILVER} GROUP BY d_ms ORDER BY d_ms"
    )
    # Bronze "정상·유니크"(품질통과 + row_id DISTINCT) — `_reconcile`과 동일 헬퍼를 공유한다
    # (SQL을 복사하면 row_id/품질조건 정의가 두 벌로 갈라져 조용히 어긋난다).
    # 여기서는 관측용 Gauge(fraud_by_date_bs_diff)를 위한 값이고, 실패 판정은 `_reconcile` 담당.
    bronze_by_ms = _bronze_valid_by_date()

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
    # `fraud_reconcile_match`(Gold 미탐지 == Silver is_suspicious)는 제거했다 —
    # undetected_fraud 가 Silver 를 is_suspicious 로 걸러 만든 테이블이라 두 값이 구조적으로
    # 항상 같아(항상 1) 어떤 사고도 잡지 못했다. 실제 정합성은 아래 fraud_run_bs_diff
    # (Bronze 정상·유니크 − Silver, 독립 SQL 교차검증)가 담당한다.

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

    # 일자별(d_ms) — 처리 행수(추세) + 사기 건수 + 층별 정합성 차이(무결성 시 0).
    d_rows  = Gauge("fraud_by_date_rows", "일자별 Silver 처리 행수", ["d_ms"], registry=g_reg)
    d_fraud = Gauge("fraud_by_date_fraud", "일자별 사기(isFraud=1) 건수", ["d_ms"], registry=g_reg)
    d_bs    = Gauge("fraud_by_date_bs_diff",
                    "Bronze(정상·유니크) − Silver (Bronze→Silver 무손실·무중복, 0이면 정합)",
                    ["d_ms"], registry=g_reg)
    # Silver→Gold 차이(sg_diff)는 내보내지 않는다 — undetected_fraud 가 Silver 를
    # is_suspicious 로 걸러 만든 테이블이라 두 값이 구조적으로 항상 같다(항상 0).
    # reconcile 에서 같은 이유로 제거한 등식이므로 지표·패널도 함께 제거했다.
    bs_max = 0
    run_rows = run_fraud = run_susp = run_bs = 0
    ds_ms = int(datetime.strptime(ds, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc).timestamp() * 1000)
    for d_ms, rows, fr, susp in by_date:
        key = str(d_ms)
        bs = int(bronze_by_ms.get(d_ms, 0)) - int(rows or 0)
        d_rows.labels(d_ms=key).set(float(rows or 0))
        d_fraud.labels(d_ms=key).set(float(fr or 0))
        d_bs.labels(d_ms=key).set(float(bs))
        bs_max = max(bs_max, abs(bs))
        if int(d_ms) == ds_ms:          # 이번 run 이 처리한 tx_date
            run_rows, run_fraud, run_susp, run_bs = int(rows or 0), int(fr or 0), int(susp or 0), bs

    # ── 이번 run(ds) 한 건에 대한 요약 — "어젯밤 배치가 잘 돌았나" 대시보드용 ──
    # 라벨 없는 단일 게이지라 카디널리티 부담이 없고, 위 루프에서 이미 구한 값을 쓰므로
    # BigQuery 쿼리도 추가되지 않는다.
    Gauge("fraud_run_date", "이번 run 이 처리한 tx_date (epoch millis)",
          registry=g_reg).set(float(ds_ms))
    Gauge("fraud_run_rows", "이번 run 처리 행수(Silver)", registry=g_reg).set(float(run_rows))
    Gauge("fraud_run_fraud", "이번 run 사기 건수(isFraud=1)", registry=g_reg).set(float(run_fraud))
    Gauge("fraud_run_undetected", "이번 run 미탐지 사기(is_suspicious)",
          registry=g_reg).set(float(run_susp))
    Gauge("fraud_run_bs_diff",
          "이번 run 정합성: Bronze(정상·유니크) − Silver (0이면 정합)",
          registry=g_reg).set(float(run_bs))

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
    from datetime import datetime as _dt
    from datetime import timezone as _tz

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

    # 집계 결과를 push **전에** 찍는다 — 전송이 실패해 skip 되더라도 수치는 로그에 남아야 한다.
    print(
        f"[push_metrics] ds={ds} | "
        f"actual={actual} flagged={flagged} tp={tp} fp={fp} fn={fn} "
        f"precision={precision:.3f} recall={recall:.3f} | "
        f"run[{ds}] rows={run_rows:,} fraud={run_fraud} undetected={run_susp} bs_diff={run_bs} | "
        f"bs_diff_max={bs_max} (0=정합) | "
        f"undetected_total={undetected_total} dates={len(by_date)} hours={len(by_hour)} "
        f"types={len(by_type)} mule_accounts={len(mule_accounts)} dagruns={len(dr_seen)}"
    )

    # ⚠ 관측 실패가 데이터 파이프라인을 죽이면 안 된다.
    # 실측 이력: Pushgateway 미기동(모니터링 프로필 누락)으로 이 호출이 DNS 해석에 실패 →
    # 재시도 소진 → 태스크 실패 → BackfillUnfinished → **31일 백필 전체 중단**.
    # reconcile까지 통과한 run이 메트릭 전송 하나로 실패 처리되는 건 잘못이다.
    #
    # 조용히 삼키지는 않는다 — AirflowSkipException 으로 태스크를 SKIPPED 로 남겨 "메트릭이
    # 안 나갔다"가 UI에서 보이게 한다(성공으로 위장하면 알 수가 없다).
    # 재시도도 걸지 않는다: 이 함수는 매번 전체 테이블을 다시 집계해 push하므로(전역 게이지),
    # 한 번 걸러도 다음 run이 전부 다시 올린다 — 자가 치유된다.
    # 감싸는 범위는 push 호출 한 줄로 좁힌다. 위쪽 BigQuery 집계 실패는 데이터 문제이므로
    # 그대로 실패시켜야 한다.
    # push_to_gateway(PUT) — pushadd(POST)가 아니다. POST는 **같은 이름의 메트릭만** 덮어써서,
    # 코드에서 제거한 지표가 Pushgateway에 영원히 남는다(실측: sg_diff 31개·reconcile_match가
    # 유령으로 남아 대시보드에 계속 노출됨). PUT은 job 그룹 전체를 이 레지스트리로 교체하므로
    # 제거한 지표가 다음 run에서 자동으로 사라진다. 이 job에 push하는 주체가 여기 하나뿐이라
    # 그룹 통째 교체가 안전하다.
    try:
        push_to_gateway(PUSHGATEWAY, job="fraud_pipeline", registry=g_reg)
    # 광범위 except은 의도적 — 전송 계층 실패 전부(DNS/연결거부/타임아웃/HTTP)를 흡수한다.
    except Exception as e:
        raise AirflowSkipException(
            f"Pushgateway({PUSHGATEWAY}) 전송 실패 — 파이프라인은 계속 진행: {e}"
        ) from e
    print(f"[push_metrics] ds={ds} → Pushgateway 전송 완료")


with DAG(
    dag_id="fraud_pipeline",
    description="Bronze→Silver→Gold 이벤트시간 일별 증분 배치 (Medallion)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2016, 1, 1, tzinfo=timezone.utc),
    # 전량 백필: ds 2016-01-01~01-31 (31 run). end_date는 logical_date에 **inclusive** —
    # 2016-02-01로 두면 데이터 없는 ds=02-01 run이 하나 더 생겨 bronze_sensor가 2h 대기 후 실패한다.
    # 원본 CSV 실측: 6,362,620행 / step 1~743 → tx_date 01-01~01-31 (743h = 30일 23시간).
    end_date=datetime(2016, 1, 31, tzinfo=timezone.utc),
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
        # 배치 완료 대기의 상한(초). 기본값 None이면 LRO 폴링이 **무기한** 매달린다 —
        # 실측으로 10시간 21분 걸린 적이 있다(그때 원인은 호스트 절전이었지만 상한이 없는
        # 구조는 그대로였다). default_args 의 execution_timeout 은 SIGALRM 기반이라 VM이
        # 멈추면 타이머도 같이 멈춰 그때 발동하지 않았다.
        #
        # 300초 = 실측 배치 전체 120초(프로비저닝 48s + Spark 72s, 가장 큰 날 01-01 기준)의
        # 2.5배. 타임아웃은 "걸렸을 때 빠져나오는 값"이지 "절대 안 걸리는 값"이 아니다.
        # 오탐이 나도 안전하다 — 재시도가 새 batch_id로 제출하고 partitionOverwrite=dynamic
        # 이라 같은 파티션을 덮어써 결과가 같다(배치 하나 값만 낭비).
        timeout=300,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SPARK_CODE_URI,
                "args": [
                    # topics/transactions = Kafka Connect GCS Sink 실제 적재 경로(topics.dir=topics
                    # 기본값). 버킷 루트를 그대로 읽으면 과거 스모크테스트 잔여물과 파티션 구조가
                    # 충돌해 Spark가 "Conflicting directory structures" 로 실패한다(실측 확인).
                    #
                    # date= 는 **이벤트일**이다(producer의 event_time 기반). 처리 대상 하루치
                    # 폴더만 넘겨 매 배치 Bronze 전량(6.36M행) 스캔을 없앤다 — 예전엔 인제스트
                    # 날짜로 파티셔닝돼 있어 프루닝이 불가능했다.
                    f"--bronze-path=gs://{GCS_BUCKET_BRONZE}/topics/transactions/date={{{{ ds }}}}",
                    f"--silver-path=gs://{GCS_BUCKET_SILVER}",
                    # --step-epoch 는 넘기지 않는다 — tx_timestamp 를 event_time 에서 직접 만든다.
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
        bash_command=COMPOSE + " run --rm dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=COMPOSE + " run --rm dbt test --profiles-dir .",
    )

    # 정합성 검증. {{ ds }} = 이번 run이 처리한 tx_date이자 **검사 상한**(그 이후 일자는
    # 아직 Silver에 있을 이유가 없다 — 백필 도중 미래 일자를 유실로 오판하지 않도록).
    reconcile = PythonOperator(
        task_id="reconcile",
        python_callable=_reconcile,
        op_kwargs={"ds": "{{ ds }}"},
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
