# fds

송금 요청을 검사하고, 최근 5분의 거래 패턴으로 이상거래를 찾아 지갑을 동결한다.

## 흐름

```text
수집기/봇 → 게이트웨이 → Mongo(transfers + outbox_events) → Publisher → Kafka(transfer.events.v1)
→ Spark → Kafka(fds.alerts.v1) → 경보 소비자 → fds_cases 저장 + Redis 동결
→ 동결된 지갑의 다음 송금 요청은 게이트웨이가 403으로 차단
```

## 파일

| 파일 | 컨테이너 | 역할 |
|---|---|---|
| `app/main.py` | `aegis-gateway` | 요청 검사, 응답 |
| `app/store.py` | `aegis-gateway` | Mongo 저장, 금액 검사, 인덱스 생성 |
| `app/blacklist.py` | `aegis-gateway` | 동결, 블랙리스트 확인 |
| `app/whitelist.py` | 게이트웨이, 소비자 | 룰별 면제 확인 |
| `app/idempotency.py` | `aegis-gateway` | 중복 요청 확인 (Redis) |
| `app/publisher.py` | `aegis-publisher` | `outbox_events`를 Kafka로 전송 |
| `app/alert_consumer.py` | `aegis-alert-consumer` | 경보를 읽어 케이스 저장, 지갑 동결 |
| `spark/stream.py` | `aegis-spark` | Kafka 읽기, 공통 정리, 경보 전송 |
| `spark/rules.py` | `aegis-spark` | 룰 R01~R04, R06 |
| `seed/` | 직접 실행 | 블랙리스트, 화이트리스트, 동결 목록 등록 |

## 게이트웨이 검사 순서

요청: `POST /api/v1/transfers`, 헤더 `X-API-Key` 필요.
형식이 틀린 요청은 FastAPI가 먼저 422로 돌려보내며 저장하지 않는다.

| 순서 | 검사 | 실패 응답 | 저장 |
|---|---|---|---|
| 1 | API 키 | 401 `UNAUTHORIZED` | 안 함 |
| 2 | 금액: 0보다 크고 소수점 6자리 이하 | 422 `INVALID_AMOUNT` | 거절 기록 |
| 3 | 시각: 서버 시각과 30초 이내 | 422 `TIMESTAMP_OUT_OF_RANGE` | 거절 기록 |
| 4 | 동결: 보내는 지갑 | 403 `FROZEN_R01` ~ `FROZEN_R06`, `FROZEN_MANUAL` | 거절 기록 |
| 5 | R07 블랙리스트: 보내는 지갑, 받는 지갑 | 403 `BLACKLISTED_SENDER`, `BLACKLISTED_RECEIVER` | 거절 기록 |
| 6 | R05: 50,000 USDT 이상 (R05 면제 지갑은 통과) | 403 `WHALE_TRANSFER` | 거절 기록 |
| 7 | 중복: `tx_hash` + `log_index` | 409 `DUPLICATE_REQUEST` | 거절 기록 |
| - | Redis, Mongo 장애 | 503 `FDS_UNAVAILABLE` | 거절 기록 |

통과하면 200 `{"status":"ACCEPTED","reason":null}`. 거절 기록은 `transfers`에 `status: REJECTED`로 남고 Kafka로는 가지 않는다.

## 룰

| 룰 | 위치 | 적발 조건 | 적발 지갑 |
|---|---|---|---|
| R01 초고빈도 | Spark | 5분 안에 5건 이상 송금 | 보내는 지갑 |
| R02 쪼개기 | Spark | 5분 안에 900~990 USDT 송금 3회 이상 | 보내는 지갑 |
| R03 집금 | Spark | 5분 안에 서로 다른 보낸 지갑 5개 이상 | 받는 지갑 |
| R04 분산 | Spark | 5분 안에 서로 다른 받는 지갑 5개 이상 | 보내는 지갑 |
| R05 대량 송금 | 게이트웨이 | 금액 50,000 USDT 이상 | 즉시 차단 |
| R06 핑퐁 | Spark | A→B 뒤 5분 안에 B→A | 먼저 보낸 지갑 |
| R07 블랙리스트 | 게이트웨이 | 보내거나 받는 지갑이 블랙리스트에 있음 | 즉시 차단 |

- Spark는 5분 칸을 15초 간격으로 계산한다. 첫 건부터 끝 건까지 4분 45초 이내면 항상 적발된다.
- 지금까지 본 가장 늦은 거래 시각보다 1분 넘게 늦은 거래는 Spark가 계산에서 제외한다.
- 경보 소비자는 같은 룰 + 같은 지갑의 적발을 케이스 하나로 합친다. 거래 시각 기준 10분 넘게 끊기면 새 케이스.
- 적발 지갑은 동결되고, 동결은 보내는 쪽만 막는다.

## 저장소

| 위치 | 이름 | 내용 |
|---|---|---|
| Mongo | `transfers` | 모든 요청 원본 (통과, 거절) |
| Mongo | `outbox_events` | Kafka 전송 대기함. 전송 후 24시간 뒤 자동 삭제 |
| Mongo | `fds_cases` | 탐지 케이스 |
| Redis | `fds:frozen_wallets` | 동결된 지갑 |
| Redis | `fds:frozen_reasons` | 지갑별 동결 사유 (지갑 → 룰 ID) |
| Redis | `fds:blacklist_wallets` | 블랙리스트 |
| Redis | `fds:whitelist:R01` ~ `R06` | 룰별 면제 지갑 |
| Redis | `idemp:*` | 중복 확인 키, 1시간 뒤 삭제 |
| Kafka | `transfer.events.v1` | 통과한 거래 |
| Kafka | `fds.alerts.v1` | Spark 경보 |

## 바꾸기 쉬운 값

| 값 | 현재 | 위치 |
|---|---|---|
| 시각 허용 범위 `MAX_CLOCK_SKEW` | 30초 | `app/main.py` |
| R05 기준 `WHALE_LIMIT` | 50,000 USDT | `app/store.py` |
| 집계 범위 `WINDOW`, `SLIDE`, 룰별 기준 | 5분, 15초 | `spark/rules.py` |
| 늦은 거래 대기 `WATERMARK`, 메시지 규격 `SCHEMA` | 1분, 필드 7개 | `spark/stream.py` |
| 동결할 룰 `FREEZE_RULES` | R01, R02, R03, R04, R06 | `app/alert_consumer.py` |
| 위험도 `RISK_LEVELS` (임시값) | 모두 `HIGH` | `app/alert_consumer.py` |
| 사건 분리 기준 `CASE_GAP` | 10분 | `app/alert_consumer.py` |

`app/` 코드를 고치면 `docker compose up -d --build <서비스>`로 다시 빌드한다.
`spark/` 코드는 `docker compose restart spark`만 하면 된다. 계산 방식을 바꾸면 체크포인트 경로(`CHECKPOINT_DIR`)도 새로 잡는다.

## 운영 명령

프로젝트 최상단(`C:\aegis-fds`)에서 실행한다.

```bash
# 블랙리스트, 화이트리스트, 동결 목록 등록 (seed 폴더의 txt 파일 기준)
.venv\Scripts\python fds\seed\load_blacklist.py
.venv\Scripts\python fds\seed\load_whitelist.py
.venv\Scripts\python fds\seed\load_frozen.py

# 지갑 동결 해제 (두 줄 모두 실행)
docker exec aegis-redis redis-cli SREM fds:frozen_wallets <지갑주소>
docker exec aegis-redis redis-cli HDEL fds:frozen_reasons <지갑주소>

# 경보 보기 (Ctrl+C로 종료)
docker exec aegis-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fds.alerts.v1 --from-beginning

# 로그
docker logs aegis-alert-consumer --tail 30
docker logs aegis-spark --tail 30
```

새 환경에서는 Kafka 토픽 자동 생성이 꺼져 있어 두 토픽을 직접 만든다.

```bash
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transfer.events.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.alerts.v1 --partitions 3 --replication-factor 1
```

## 주의

- `seed/blacklist.txt`는 한 줄에 주소 하나만 쓴다. 주석 줄도 그대로 등록된다.
- `seed/whitelist.txt` 형식은 `지갑주소 R01,R03,R04,R05`. 지금은 실험용으로 모든 지갑이 R01~R06 면제다.
- Spark를 체크포인트 없이 처음 시작하면 그 이후 들어온 거래만 읽는다 (`STARTING_OFFSETS=latest`).
- 게이트웨이는 `.env`에 `GATEWAY_API_KEY`가 없으면 시작하지 않는다.
