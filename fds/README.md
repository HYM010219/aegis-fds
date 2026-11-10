# fds

송금 요청을 두 단계로 검사한다.
1단계는 게이트웨이가 Redis로 최근 5분을 보고 즉시 3분 보류하고, 2단계는 Spark가 최근 2주 기록을 보고 동결한다.

## 흐름

```text
수집기/봇 → 게이트웨이 + Redis (1단계: 최근 5분, R01~R07)
  ├ 통과 → Mongo transfers + outbox → Publisher → ① transfer.events.v1 ─┐
  ├ 거절 → Mongo transfers + outbox → Publisher → transfer.rejected.v1 (ClickHouse용)
  └ 보류·차단 → Mongo fds_holds + outbox → Publisher → ② fds.holds.v1 ──┤
                                                                       ▼
                               Spark (2단계: 10초마다, Mongo 최근 2주 조회, L1~L8)
                                                                       │ ③ fds.alerts.v1
                                                                       ▼
                                                                  경보 소비자
          ├ L1·L2·L3·L8 → fds_cases 저장 + Redis 동결 → ④ fds.freezes.v1 → Spark
          └ L4·L5·L6·L7 → fds_cases 저장 (검토 대기) → 관제 직원이 동결 → ④ → Spark
```

## 파일

| 파일 | 컨테이너 | 역할 |
|---|---|---|
| `app/main.py` | `aegis-gateway` | 요청 검사 순서, 응답 |
| `app/holds.py` | `aegis-gateway` | 1단계 R01~R04·R06 계산 (Redis Lua), 3분 보류 |
| `app/store.py` | `aegis-gateway` | Mongo 저장 (거래, 보류 기록, outbox), 인덱스 |
| `app/blacklist.py` | 게이트웨이, 소비자 | 동결, 블랙리스트, 보류 중 확인 |
| `app/whitelist.py` | 게이트웨이, 소비자 | 룰별 면제 확인 |
| `app/idempotency.py` | `aegis-gateway` | 중복 요청 확인 (Redis) |
| `app/publisher.py` | `aegis-publisher` | `outbox_events`를 문서의 `topic`대로 Kafka에 전송 |
| `app/alert_consumer.py` | `aegis-alert-consumer` | L1~L8 경보 → 케이스 저장, 자동 동결, 동결 이벤트 전송 |
| `spark/stream.py` | `aegis-spark` | Kafka 토픽 3개 읽기, 경보 전송 |
| `spark/detector.py` | `aegis-spark` | 분석할 지갑 고르기, Mongo 조회, L1~L8 계산 |
| `seed/` | 직접 실행 | 블랙리스트, 화이트리스트, 동결 목록 등록 |

컨테이너 종류마다 폴더 하나에 `Dockerfile`과 `requirements.txt`를 함께 둔다.
`app/`은 게이트웨이, Publisher, 경보 소비자가 같은 이미지를 쓰고, 실행 명령만 다르다.

## 1단계: 게이트웨이 검사 순서

요청: `POST /api/v1/transfers`, 헤더 `X-API-Key` 필요.
형식이 틀린 요청은 FastAPI가 먼저 422로 돌려보내며 저장하지 않는다.

| 순서 | 검사 | 실패 응답 | Spark에 알림 |
|---|---|---|---|
| 1 | API 키 | 401 `UNAUTHORIZED` (저장 안 함) | - |
| 2 | 금액: 0보다 크고 소수점 6자리 이하 | 422 `INVALID_AMOUNT` | - |
| 3 | 시각: 서버 시각과 30초 이내 | 422 `TIMESTAMP_OUT_OF_RANGE` | - |
| 4 | 보내는 지갑 동결 | 403 `FROZEN_L1` ~ `FROZEN_L8`, `FROZEN_MANUAL` | - |
| 5 | R07 블랙리스트 (보내는 지갑 / 받는 지갑) | 403 `BLACKLISTED_SENDER` / `BLACKLISTED_RECEIVER` | 받는 지갑일 때만 ② |
| 6 | 보내는 지갑이 보류 중 | 429 `TEMP_HOLD_Rxx`, `Retry-After: 남은 초` | - |
| 7 | 중복: `tx_hash` + `log_index` | 409 `DUPLICATE_REQUEST` | - |
| 8 | R05: 50,000 USDT 이상 | 429 `TEMP_HOLD_R05` + 보내는 지갑 3분 보류 | ② |
| 9 | R01, R02, R04 | 429 `TEMP_HOLD_Rxx` + 보내는 지갑 3분 보류 | ② |
| 10 | R03, R06 | **200 통과** + 받는 지갑 3분 보류 | ① + ② |
| - | Redis, Mongo 장애 | 503 `FDS_UNAVAILABLE` | - |

통과하면 200 `{"status":"ACCEPTED","reason":null}`. 거절 기록은 `transfers`에 `status: REJECTED`로 남고, 같은 내용이 `transfer.rejected.v1`로도 간다 (401은 저장도 전송도 안 함).

- 보류와 동결은 **송금만** 막는다. 보류·동결 지갑도 입금은 받는다.
- 중복 검사를 R룰 계산 앞에 둬서, 재전송한 거래가 개수에 두 번 들어가지 않는다.
- 429로 거절된 거래는 중복 키를 풀어 두므로, 보류가 끝난 뒤 같은 `tx_hash`로 다시 보낼 수 있다.
- 거절된 거래는 5분 개수에 넣지 않는다. 걸린 그 거래도 포함하지 않는다.
- 같은 거래가 여러 룰에 걸리면 첫 룰(R01 → R02 → R04 순)이 사유가 되고, 전체 목록은 `fds_holds.rules`에 남는다.

### 1단계 룰 (거래 시각 기준 최근 5분, 정확히 계산)

| 룰 | 이름 | 기준 | 보류 대상 | 이 거래 |
|---|---|---|---|---|
| R01 | 초고빈도 송금 | 송금 5건 이상 | 보내는 지갑 | 거절 |
| R02 | 쪼개기 송금 | 900~990 USDT 송금 3회 이상 | 보내는 지갑 | 거절 |
| R03 | 자금 집금 | 서로 다른 보낸 지갑 5개 이상 | 받는 지갑 | 통과 |
| R04 | 자금 분산 | 서로 다른 받는 지갑 5개 이상 | 보내는 지갑 | 거절 |
| R05 | 대량 송금 | 1건 50,000 USDT 이상 | 보내는 지갑 | 거절 |
| R06 | 핑퐁 | A→B 뒤 5분 안에 B→A | 먼저 보낸 지갑(A) | 통과 |
| R07 | 블랙리스트 | 보내거나 받는 지갑이 블랙리스트 | 보류 없이 항상 차단 | 거절 |

- 화이트리스트 지갑(`fds:whitelist:R01` ~ `R06`)은 해당 룰로 보류하지 않는다. R07은 면제가 없다.
- 이미 보류 중이거나 동결된 받는 지갑은 다시 보류하지 않는다.

## 2단계: Spark 장기 분석

### 분석 시점

| 계기 | 토픽 | 분석 대상 | 시점 |
|---|---|---|---|
| ① 보류 | `fds.holds.v1` (HOLD) | 보류된 지갑 | 다음 배치 (10초 안) |
| ② 거래 | `transfer.events.v1`, `fds.holds.v1` (R07 BLOCK) | 보낸 지갑, 받은 지갑 | 다음 배치. 단, 재분석 간격 안이면 대기 목록으로 미룸 |
| ③ 동결, 블랙리스트 | `fds.freezes.v1`, Redis 블랙리스트 비교 | 그 지갑과 최근 2주 안에 거래한 상대 지갑 (L5만) | 배치마다 500개씩 |

- 토픽 3개를 쿼리 하나로 10초마다 읽는다. 보류는 어차피 3분이라 10초면 충분하다.
- 거래가 없는 동안에도 대기 목록을 처리하도록 1초에 1줄씩 시계 신호(`rate`)를 같이 읽는다.
- 블랙리스트는 스크립트로 Redis에 직접 넣으므로, Spark가 배치마다 `fds:blacklist_seen`과 비교해 새 주소를 찾는다.
- 화이트리스트 `fds:whitelist:L`, 동결, 블랙리스트 지갑은 분석하지 않는다.
- 한 배치에서 Mongo 조회는 룰 묶음마다 1번씩이다. L1·L2·L3·L5·L6·L7은 Mongo가 지갑별 숫자만 돌려주고, L4·L8은 거래 원본을 받아 Spark가 순서대로 짝짓는다.

### 2단계 룰

| 룰 | 이름 | 기준 (prod 기간) | 처리 |
|---|---|---|---|
| L1 | 반복 보류 | 2주 안에 1단계 보류 3회 이상 (`fds_holds`) | 자동 동결 |
| L2 | 쪼개서 대량 송금 | 24시간 누적 송금 50,000 이상, 모든 거래 50,000 미만 | 자동 동결 |
| L3 | 장기 쪼개기 | 2주 안에 900~990 송금 10회 이상 | 자동 동결 |
| L4 | 경유 지갑 | 1,000 이상 받고 1시간 안에 80% 이상 다시 송금, 2주 안에 3회 이상 | 검토 대기 |
| L5 | 위험 지갑 접촉 | 아래 등급표 | 검토 대기 |
| L6 | 급격한 활동 증가 | 최근 24시간 송금 수가 그 전 하루 평균의 10배 이상, 최소 20건 | 검토 대기 |
| L7 | 새 상대 집중 | 24시간 안에 처음 보내는 상대 20개 이상 (2주 기록 기준) | 검토 대기 |
| L8 | 순환 거래 | A→B→A 또는 A→B→C→A, 24시간 안에 시간 순서대로, 각 1,000 이상, 처음 금액의 80% 이상 귀환 | 자동 동결 |

L5 등급 (통과된 거래만 셈, R07로 막힌 시도는 제외):

| 등급 | 조건 |
|---|---|
| CRITICAL | 블랙리스트 지갑에게서 직접 받음 |
| HIGH | 블랙리스트 지갑에게 보냄 |
| MEDIUM | 동결 지갑과 2주 누적 10,000 USDT 이상 주고받음 |

- 블랙리스트에 오른 뒤의 거래는 R07이 막으므로, CRITICAL·HIGH는 **블랙리스트 등록 전 2주 안의 거래**에서 찾는다 (계기 ③).
- L8은 화이트리스트(`L`) 지갑과 상대가 200개 넘는 지갑을 거치는 경로는 따라가지 않는다.
- 자동 동결 사유는 `FROZEN_L1`처럼 룰 ID를 쓴다. 관제 직원이 검토 후 동결할 때도 케이스 룰을 사유로 쓴다.
- 같은 룰 + 같은 지갑의 경보는 케이스 하나로 합친다. 거래 시각 기준 10분 넘게 끊기면 새 케이스.

### 기간 설정 (`PERIOD_PROFILE`)

| 설정 | 장기 | 하루 | 경유 | 재분석 간격 |
|---|---|---|---|---|
| `prod` (운영) | 2주 | 24시간 | 1시간 | 60초 |
| `demo` (시연) | 20분 | 5분 | 1분 | 15초 |

내 PC는 `docker-compose.yml`, AWS는 k3s ConfigMap에서 바꾼다. 코드는 같다.

## 저장소

| 위치 | 이름 | 내용 |
|---|---|---|
| Mongo | `transfers` | 모든 요청 원본 (통과, 거절). 자동 삭제는 `TRANSFERS_TTL_DAYS`로 켬 (지금 0 = 끔) |
| Mongo | `fds_holds` | 새로 걸린 보류 (지갑, 룰, 보낸/받은 쪽, 근거). 14일 뒤 자동 삭제 |
| Mongo | `outbox_events` | Kafka 전송 대기함. 전송 후 24시간 뒤 자동 삭제 |
| Mongo | `fds_cases` | L1~L8 케이스. 자동 동결(`WALLET_FROZEN`) / 검토 대기(`REVIEW_PENDING`, `review_status: PENDING`) |
| Redis | `fds:w:send / split / out / in:{지갑}` | 1단계 5분 기록 (6분 뒤 자동 삭제) |
| Redis | `fds:hold:{지갑}` | 보류 중 (값 = 룰, 3분 뒤 자동 삭제) |
| Redis | `fds:frozen_wallets`, `fds:frozen_reasons` | 동결 지갑, 사유 (지갑 → L1~L8) |
| Redis | `fds:blacklist_wallets` | 블랙리스트 |
| Redis | `fds:whitelist:R01` ~ `R06`, `fds:whitelist:L` | 1단계 룰별 면제, 2단계 분석 면제 |
| Redis | `fds:analyzed:{지갑}`, `fds:analyze_pending` | 재분석 간격, 미룬 지갑 (점수 = 분석할 시각) |
| Redis | `fds:recheck_l5`, `fds:blacklist_seen` | 계기 ③ 대기 목록, 처리한 블랙리스트 |
| Redis | `idemp:*` | 중복 확인 키, 1시간 뒤 삭제 |
| Kafka | `transfer.events.v1` | 통과한 거래 |
| Kafka | `transfer.rejected.v1` | 거절된 요청 (401 제외). ClickHouse 장기 보관용, Spark는 읽지 않음 |
| Kafka | `fds.holds.v1` | 1단계 보류(HOLD), R07 차단(BLOCK) |
| Kafka | `fds.alerts.v1` | Spark L1~L8 경보 |
| Kafka | `fds.freezes.v1` | 새로 동결된 지갑 |

## 바꾸기 쉬운 값

| 값 | 현재 | 위치 |
|---|---|---|
| 시각 허용 범위 `MAX_CLOCK_SKEW` | 30초 | `app/main.py` |
| 1단계 기준, 5분, 보류 시간 | R01 5건, R02 3회, R03·R04 5개, 3분 | `app/holds.py` |
| R05 기준 `WHALE_LIMIT`, 보류 기록 보관 `HOLDS_KEEP_DAYS` | 50,000 USDT, 14일 | `app/store.py` |
| `transfers` 자동 삭제 `TRANSFERS_TTL_DAYS` | 0 (끔). ClickHouse 연결 후 14 | `docker-compose.yml` (gateway) |
| 2단계 기준, 기간, 처리량 제한 | L1~L8 숫자, `PROFILES` | `spark/detector.py` |
| 자동 동결 룰 `AUTO_FREEZE_RULES` | L1, L2, L3, L8 | `app/alert_consumer.py` |
| 위험도 `RISK_LEVELS` (L5는 등급 사용) | 자동 동결 HIGH, 나머지 MEDIUM | `app/alert_consumer.py` |
| 사건 분리 기준 `CASE_GAP` | 10분 | `app/alert_consumer.py` |

`app/` 코드를 고치면 `docker compose up -d --build <서비스>`로 다시 빌드한다.
`spark/` 코드는 `docker compose restart spark`만 하면 된다. `spark/requirements.txt`를 바꾸면 `docker compose up -d --build spark`.

## 운영 명령

프로젝트 최상단(`C:\aegis-fds`)에서 실행한다.

```bash
# 내 PC에서 시드 스크립트를 처음 쓸 때 패키지 설치
.venv\Scripts\pip install -r fds\app\requirements.txt

# 블랙리스트, 화이트리스트, 동결 목록 등록 (seed 폴더의 txt 파일 기준)
.venv\Scripts\python fds\seed\load_blacklist.py
.venv\Scripts\python fds\seed\load_whitelist.py
.venv\Scripts\python fds\seed\load_frozen.py

# 지갑 동결 해제 (두 줄 모두 실행)
docker exec aegis-redis redis-cli SREM fds:frozen_wallets <지갑주소>
docker exec aegis-redis redis-cli HDEL fds:frozen_reasons <지갑주소>

# 보류 해제 (3분을 기다리지 않을 때)
docker exec aegis-redis redis-cli DEL fds:hold:<지갑주소>

# 경보 보기 (Ctrl+C로 종료)
docker exec aegis-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fds.alerts.v1 --from-beginning

# 로그. Spark 배치 로그의 seconds가 10초보다 충분히 짧은지, pending_size가 계속 늘지 않는지 본다.
docker logs aegis-alert-consumer --tail 30
docker logs aegis-spark --tail 30
```

새 환경에서는 Kafka 토픽 자동 생성이 꺼져 있어 다섯 토픽을 직접 만든다. **Spark보다 먼저** 만들어야 한다 (없으면 Spark가 시작하다 멈춘다).

```bash
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transfer.events.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transfer.rejected.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.holds.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.alerts.v1 --partitions 3 --replication-factor 1
docker exec aegis-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fds.freezes.v1 --partitions 3 --replication-factor 1
```

## 주의

- `seed/blacklist.txt`는 한 줄에 주소 하나만 쓴다. 주석 줄도 그대로 등록된다.
- `seed/whitelist.txt` 형식은 `지갑주소 R01,R03,R05,L`. 거래소 16개는 `L`(2단계 면제)까지, 시뮬레이터 지갑은 지금 실험용으로 R01~R06 면제다.
- 관제 웹에서 동결할 때는 Redis 동결(사유 = 케이스 룰)과 함께 `fds.freezes.v1`에 `{"event":"FREEZE","wallet":...,"rule":...,"source":"MANUAL"}`을 보내야 계기 ③이 돈다.
- Spark를 체크포인트 없이 처음 시작하면 그 이후 들어온 메시지만 읽는다 (`STARTING_OFFSETS=latest`).
- 게이트웨이는 `.env`에 `GATEWAY_API_KEY`가 없으면 시작하지 않는다.
- `TRANSFERS_TTL_DAYS`를 0에서 14로 바꾸면 게이트웨이가 시작할 때 14일 지난 `transfers`를 지우기 시작한다. **ClickHouse에 거래·거절 기록이 쌓이는 걸 확인한 뒤에** 켠다. 다시 0으로 바꾸면 자동 삭제를 끈다.
