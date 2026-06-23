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

PROJECT_DIR  = "/opt/airflow/project"
COMPOSE      = "docker compose -f docker/docker-compose.yml"
# 형제 잡(spark/dbt) 컨테이너의 바인드 마운트는 '호스트 절대경로'여야 한다(docker-out-of-docker).
# 스케줄러에 상속된 HOST_PROJECT_DIR(=.. 가능)에 가려지지 않도록 .env에서 직접 export 해 덮어쓴다.
ENV_PREFIX   = f"cd {PROJECT_DIR} && export $(grep -E '^HOST_PROJECT_DIR=' .env) && "

default_args = {
    "owner": "fraud-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _bronze_has_data() -> bool:
    """Bronze에 데이터가 존재하는지(빈 입력 허위 성공 방지)."""
    return len(glob.glob(BRONZE_GLOB, recursive=True)) > 0


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


with DAG(
    dag_id="fraud_pipeline",
    description="Bronze→Silver→Gold 이벤트시간 일별 증분 배치 (Medallion)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2016, 1, 1),
    end_date=datetime(2016, 2, 1),   # 데이터셋 30일 구간으로 백필 한정
    catchup=True,
    max_active_runs=1,               # 같은 파티션 동시 처리 방지
    tags=["fraud", "medallion", "batch"],
) as dag:

    bronze_sensor = PythonSensor(
        task_id="bronze_sensor",
        python_callable=_bronze_has_data,
        mode="poke",
        poke_interval=15,
        timeout=120,
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

    bronze_sensor >> spark_silver >> dbt_run >> dbt_test >> reconcile
