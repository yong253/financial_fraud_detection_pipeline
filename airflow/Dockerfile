# Airflow 이미지 — DAG가 형제 컨테이너(dbt)를 docker.sock으로 띄우므로
# docker CLI + compose 플러그인 필요.
# 베이스의 airflow 유저(UID 50000, GID 0=root)가 docker.sock(root:root rw-) 접근 가능.
FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
 && chmod a+r /etc/apt/keyrings/docker.asc \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
      > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# apache-airflow-providers-google: DataprocCreateBatchOperator + LocalFilesystemToGCSOperator + BQ/GCS 훅
#   (google-cloud-bigquery/dataproc/storage 포함 → reconcile/push_metrics의 BQ 조회도 커버).
#   airflow 2.9.3 constraints 로 버전 정합성 확보(단독 설치 시 의존성 충돌 방지).
RUN pip install --no-cache-dir "apache-airflow-providers-google" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
# prometheus_client: ⑥ Pushgateway push.
RUN pip install --no-cache-dir prometheus_client==0.20.0
