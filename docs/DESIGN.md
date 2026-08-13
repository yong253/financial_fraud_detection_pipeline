# 설계 노트

[README](../README.md)에서 요약한 신뢰성 설계의 상세 근거입니다.

이 프로젝트가 말하는 **신뢰성**은 세 가지입니다.

1. **무손실** — 들어온 행은 사라지지 않는다. 버릴 행도 지우지 않고 격리해 보존한다.
2. **무중복** — 같은 행이 두 번 세어지지 않는다. 재실행해도 결과가 같다(멱등).
3. **자기검증** — 위 두 가지가 지켜졌는지를 사람이 아니라 파이프라인이 매 실행 확인하고,
   어긋나면 성공으로 끝나지 않는다.

---

## 수집 완결 판정

Airflow DAG의 첫 태스크 `bronze_sensor`는 **그날 데이터가 Bronze에 완결 도착했을 때만**
통과합니다. 판정 기준은 이것입니다.

```
그날의 EOD 마커 수 == Kafka 토픽 파티션 수
```

Producer는 이벤트일 하루치 발행이 끝날 때마다 **모든 파티션에** EOD(End-of-Day) 마커
메시지를 하나씩 찍습니다. 31일 × 3파티션 = 마커 93개가 정상값입니다.

**왜 파일 개수나 시간 대기가 아니라 마커인가.** 완결 신호를 데이터와 **같은 경로**
(Kafka → Kafka Connect → GCS)로 흘려보내야, 그 경로 전체가 끝났다는 것이 증명되기 때문입니다.

- *"N분 기다린다"* — 지연이 나면 조용히 부분 처리됩니다. 실패보다 나쁩니다.
- *"파일 개수를 센다"* — Connect의 `flush.size` 롤오버 타이밍에 따라 개수가 달라져 기준이 되지 못합니다.
- *"마커가 도착했다"* — 그 마커가 GCS에 보인다는 것은 **그 앞의 모든 데이터가 같은 경로를
  이미 통과했다**는 뜻입니다. Kafka는 파티션 내 순서를 보장하므로, 마커는 자기보다 앞선
  메시지의 도착을 증명합니다.

파티션 수만큼 요구하는 이유도 같습니다. 마커 1개만 보면 나머지 2개 파티션이 아직 흐르는
중일 수 있습니다.

---

## 품질검증과 Quarantine

Spark(Dataproc Serverless)가 Bronze를 읽어 품질 조건을 검사하고, 위반한 행에
`reject_reason`을 부여합니다.

| 사유 | 조건 |
|------|------|
| `parse_error` | 필수 필드(`step`/`amount`/`nameOrig`/`type`/`nameDest`) 누락 |
| `missing_event_time` | `event_time` 누락 — `tx_date` 파생 불가 |
| `invalid_amount` | 금액이 숫자가 아니거나 0 이하 |
| `invalid_step` | `step`이 1~743 범위 밖 |
| `invalid_type` | 정의된 5종 거래 유형이 아님 |
| `invalid_flag` | `isFraud`/`isFlaggedFraud`가 0/1이 아님 |
| `negative_balance` | 출금 계좌 잔액이 음수 |

불량 행은 **삭제하지 않고** `silver/quarantine/`에 사유와 함께 격리 보존합니다. 금융
데이터에서 "이상해서 버렸다"는 추적 불가능한 손실이고, 나중에 판정 기준이 틀렸던 것으로
드러나도 복구할 수 없기 때문입니다.

격리 데이터가 집계에 섞이지 않게 하는 것은 **경로 스코프**로 처리합니다. Silver External
Table의 `uris`를 `tx_date=*`로 한정하면, 형제 경로인 `quarantine/`은 애초에 테이블의
스캔 대상에 들어오지 않습니다. 필터 조건(`WHERE reject_reason IS NULL` 같은)으로 거르면
쿼리를 쓸 때마다 빠뜨릴 위험이 있지만, 경로로 자르면 실수할 여지가 없습니다.

---

## 무손실 무중복 설계

전 컴포넌트를 **at-least-once(무손실) + 멱등/dedup(무중복)** 조합으로 설정했습니다.
exactly-once를 한 컴포넌트 안에서 달성하려 하는 대신, 앞단은 "절대 잃지 않되 중복은 허용",
뒷단은 "중복을 제거"로 나눈 구조입니다.

| 컴포넌트 | 설정 |
|---|---|
| Kafka | `RF=3` · `min.insync.replicas=2` · `unclean.leader.election=false` · `retention.ms=-1` |
| Producer | `enable.idempotence=true` · `acks=all` · key=`nameOrig`(계좌별 순서 보장) · delivery 콜백 + 종료 시 `flush()` |
| Kafka Connect | GCS 업로드 성공 후 오프셋 커밋(at-least-once) |
| Spark | `partitionOverwriteMode=dynamic`(멱등 재처리) · `dropDuplicates(row_id)` |
| Airflow | 멱등 태스크 · `retries=2` · `max_active_runs=1` |
| 공통 | 금액은 float 드리프트 방지를 위해 문자열 / `DECIMAL(18,2)`로 직렬화 |

### 왜 이 조합인가

- **`RF=3` + `min.insync.replicas=2`** — 브로커 1대가 죽어도 쓰기가 계속되고, 2대가 죽으면
  조용히 유실되는 대신 **쓰기가 거부**됩니다. 유실보다 정지가 낫습니다.
- **`unclean.leader.election=false`** — 뒤처진 팔로워가 리더가 되면서 이미 커밋된 메시지가
  사라지는 경로를 막습니다.
- **`partitionOverwriteMode=dynamic`** — 재실행하면 해당 `tx_date` 파티션만 통째로
  덮어씁니다. append라면 재시도 한 번에 그날치가 두 배가 되지만, 이 모드에서는 몇 번을
  돌려도 결과가 같습니다.
- **금액을 문자열/`DECIMAL`로** — float64는 `0.1 + 0.2 != 0.3`입니다. 630만 건을 합산하면
  드리프트가 누적되어, 레이어별 합계가 미세하게 어긋나며 정합성 검증이 무의미해집니다.

### dedup 키

```
row_id = SHA-256(nameOrig | step | type | amount | nameDest)
```

`dropDuplicates(row_id)`는 **같은 키를 가진 서로 다른 정상 거래가 있으면 진짜 데이터를
지웁니다.** 그래서 쓰기 전에 원본 630만 건을 전수 검사해 **키 충돌 0건**을 확인했습니다.
이 확인 없이 dedup을 넣는 것은 무중복을 얻는 대신 무손실을 잃는 거래입니다.

---

## 정합성 자동 검증

### 어느 단계에서

Airflow DAG의 **`reconcile` 태스크**입니다. Gold 생성이 끝난 뒤, 모니터링 지표를 내보내기
직전에 놓입니다.

```
bronze_sensor      ─┐
                    ├─→ spark_silver → dbt_run → dbt_test → reconcile → push_metrics
upload_spark_code  ─┘                                       ▲
                                            여기서 통과 못 하면 DAG 실패
```

앞의 두 태스크는 병렬입니다 — 센서가 데이터 도착을 기다리는 동안 Spark 코드 업로드가
이미 끝나 있습니다.

DAG 1회 실행 = 이벤트일 하루 처리이므로, **하루치를 처리할 때마다 매번** 검증합니다.
사람이 따로 돌리는 절차가 아니라 파이프라인의 한 태스크입니다.

이 위치인 이유는 **실패했을 때 지표가 나가면 안 되기 때문**입니다. `push_metrics`는
대시보드에 숫자를 올리는 태스크인데, 정합성이 깨진 채로 숫자가 올라가면 잘못된 값이
정상인 것처럼 보입니다. `reconcile`이 앞에서 막으면 DAG가 실패하면서 지표도 나가지
않습니다.

### 무엇을 대조하나

이벤트일(`tx_date`)별로 아래 등식을 확인하고, 한 날짜라도 어긋나면 DAG를 실패시킵니다.

```
Bronze(품질검증 통과 · row_id 유니크) 행수  ==  Silver 행수
```

### 어떻게 진행되나

**1) Bronze 쪽 집계** — BigQuery External Table에 SQL을 던져 이벤트일별 행수를 셉니다.
품질 조건 7종을 `WHERE`로 걸고, `COUNT(DISTINCT SHA256(...))`으로 중복을 제거합니다.

**2) Silver 쪽 집계** — Silver External Table에서 `tx_date`별 `COUNT(*)`를 셉니다.

**3) 대조** — 두 결과를 날짜 키로 합집합 순회하며 비교하고, 다르면 예외를 던집니다.

```python
for d_ms in sorted(set(bronze) | set(silver)):
    b, s = int(bronze.get(d_ms, 0)), int(silver.get(d_ms, 0))
    if b != s:
        mismatches.append((day, b, s))
```

불일치가 있으면 날짜별 차이를 전부 출력하고 실패시킵니다.

```
정합성 불일치 1일 (Bronze 품질통과·유니크 != Silver 행수, tx_date <= 2016-01-05):
  2016-01-03  bronze=205,411  silver=205,398  diff=+13
```


---

## 성능 최적화

전량 630만 건 기준. 자원(executor 4×4)과 데이터를 고정하고 측정했습니다.

| 개선 | 지표 | Before | After | 효과 |
|---|---|---|---|---|
| Kafka Connect `flush.size` 1000→10000 | Kafka→Bronze 처리량 | 21,139 행/초 | **38,797 행/초** | **+83.5%** |
| Spark 중복 스캔 제거 | Silver 배치 실행 | 156.0초 | **108.5초** | **−30.5%** |

두 개선의 누적 효과입니다.

```
개선 전   234.5초   27,129 행/초
개선 후   108.5초   58,663 행/초

   시간 −53.7%      처리량 2.16배
```

### 인제스트 — flush.size

병목은 파일당 GCS 왕복이었습니다. `flush.size`를 10배로 키워 왕복 횟수를 1/10로 줄였고,
Kafka→Bronze 처리량이 83.5% 올랐습니다.

이 설정은 다음 레이어에도 영향을 미칩니다. Bronze 파일 수가 **6,464개 → 745개**로 줄면서
Spark가 열어야 할 파일도 8.7배 감소해, Silver 배치가 코드 수정 없이 **234.5초 → 155.3초
(−33.8%)** 가 됐습니다. 한 레이어의 쓰기 설정이 다음 레이어의 읽기 성능을 결정합니다.

### Silver 배치 — 중복 스캔 제거

Spark는 지연 평가라 액션마다 원본을 다시 읽습니다. `batch_silver.py`에 캐시 없는 액션이
9개(행수 검산 5건 · Silver/Quarantine write 2건 · 마커 집계 2건) 있어 Bronze를 매 배치 9번
스캔하고 있었습니다.

집계를 병합하고 `cache`를 더해 1번으로 줄였습니다. `cache`는 executor 메모리를 쓰므로,
병합만으로 충분한지 판단하기 위해 두 조치를 나눠 측정했습니다.

| 조건 | 액션 수 | 읽은 행수 | 스캔 |
|---|---|---|---|
| 기준선 | 9 | 57,263,673 | 9배 |
| 집계 병합만 | 6 | 38,175,813 | 6배 |
| **집계 병합 + cache** | 6 | **6,367,193** | **1배** |

병합만으로는 6배까지 줄어듭니다. 1배가 되려면 `cache`가 필요합니다.

### 정합성 유지 확인

29회차 측정 전 회차에서 검산 6줄이 일치했습니다.

```
bronze_tx=6,362,620 · valid_raw=6,362,604 · quarantine=16
dedup_removed=0 · silver_written=6,362,604 · is_suspicious=8,181
```

무손실·무중복을 유지한 상태에서 얻은 개선입니다.
