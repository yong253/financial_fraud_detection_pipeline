# Kafka Producer 실행용 경량 이미지 — requirements.txt 와 동일 버전 고정.
# dbt/Dockerfile과 동일 패턴: 코드는 이미지에 COPY하지 않고 compose가 bind-mount
# (재빌드 없이 코드 수정 반영). google-cloud-storage는 --done-marker gs:// 업로드용.
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    confluent-kafka==2.5.3 \
    python-dotenv==1.0.1 \
    google-cloud-storage==2.18.2

# 컨테이너 로그를 버퍼링 없이 즉시 출력(docker compose logs -f 대응)
ENV PYTHONUNBUFFERED=1
