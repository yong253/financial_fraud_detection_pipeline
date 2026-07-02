# Kafka Connect + GCS Sink Connector (Confluent Community License, 무료)
# E단계: Kafka(transactions) → Bronze GCS 상시 인제스트(DAG 밖).
# 주의: 순수 cp-kafka-connect(커뮤니티 전용) 이미지는 라이선스 매니저 클래스가 없어
# Confluent Hub 커넥터(GCS Sink 포함)가 NoClassDefFoundError(io.confluent.license.*)로
# FAILED 상태가 된다. cp-server-connect-base 로 교체(라이선스 프레임워크 포함, 무료 커넥터도
# 이 프레임워크 위에서 "커뮤니티 라이선스"로 동작).
FROM confluentinc/cp-server-connect-base:7.6.1

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-gcs:latest
