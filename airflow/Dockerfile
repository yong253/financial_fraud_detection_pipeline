# Airflow 이미지 — DAG가 형제 컨테이너(spark-silver/dbt)를 docker.sock으로 띄우므로
# docker CLI + compose 플러그인 필요. reconcile 태스크용 duckdb도 설치.
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
# duckdb: reconcile/push_metrics 의 Gold/Silver 조회용. prometheus_client: ⑥ Pushgateway push 용.
RUN pip install --no-cache-dir duckdb==1.1.0 prometheus_client==0.20.0
