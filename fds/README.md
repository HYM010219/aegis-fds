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
                                                                       │ Mongo fds_alert_outbox에 먼저 저장
                                                                       │ ③ fds.alerts.v1
                                                                       ▼
                                                                  경보 소비자
          ├ L1·L2·L3·L8 → fds_cases 저장 + Redis 동결 → ④ fds.freezes.v1 → Spark
          └ L4·L5·L6·L7 → fds_cases 저장 (검토 대기)
                              ↓
관제 웹 백엔드 → 직원 API (/api/v1/admin) → 수동 동결 → ④ → Spark / 이상 없음 / 동결 해제
```

## 파일

| 파일 | 컨테이너 | 역할 |
|---|---|---|
| `app/main.py` | `aegis-gateway` | 요청 검사 순서, 응답 |
| `app/admin.py` | `aegis-gateway` | 직원 API: 동결·해제, 케이스 처리, 블랙리스트·화이트리스트 관리, 지갑 상태 조회 |
| `app/holds.py` | `aegis-gateway` | 1단계 R01~R06 판단 (R05 + Redis Lua), 3분 보류 |
| `app/store.py` | `aegis-gateway` | Mongo 저장 (거래, 보류 기록, outbox), 인덱스 |
| `app/wallet_status.py` | 게이트웨이, 소비자 | 동결, R07 블랙리스트, 보류 중 확인 |
| `app/whitelist.py` | `aegis-gateway` | 룰별 면제 확인 |
| `common/keys.py` | 전부 | 여러 서비스가 같이 쓰는 토픽 이름, Redis 키, R05 기준 |
| `app/idempotency.py` | `aegis-gateway` | 중복 요청 확인 (Redis) |
| `app/publisher.py` | `aegis-publisher` | `outbox_events`를 문서의 `topic`대로 Kafka에 전송 |
| `app/alert_consumer.py` | `aegis-alert-consumer` | L1~L8 경보 → 케이스 저장, 자동 동결, 동결 이벤트 전송 |
| `spark/stream.py` | `aegis-spark` | Kafka 토픽 3개 읽기, 경보 저장 → 전송 순서 |
| `spark/alert_outbox.py` | `aegis-spark` | 경보 전송 대기함 `fds_alert_outbox` (저장, 못 보낸 경보 다시 보내기) |
| `spark/detector.py` | `aegis-spark` | 배치 흐름: 계기 ①②③, 분석할 지갑 고르기, 제외. 계산(`analyze`)과 처리 표시(`mark_done`)를 나눔 |
| `spark/crosscheck.py` | `aegis-spark` | L2·L6·L7 경보의 원본 거래를 Spark로 다시 계산 (1분 내역, 5분 이동 윈도, 판정 값 대조) |
| `spark/config.py` | `aegis-spark` | 기간 프로필(prod/demo), L1~L8 기준값, 처리량 제한 |
| `spark/queries.py` | `aegis-spark` | Mongo 조회 (여러 지갑을 한 번에, L2·L3·L6과 L4·L8은 조회를 같이 씀) |
| `spark/rules.py` | `aegis-spark` | L1~L8 판단 (조회 결과 → 경보, Mongo 없이 테스트 가능) |

컨테이너 종류마다 폴더 하나에 `Dockerfile`과 `requirements.txt`를 함께 둔다.
`app/`은 게이트웨이, Publisher, 경보 소비자가 같은 이미지를 쓰고, 실행 명령만 다르다.
`common/`은 게이트웨이 이미지와 Spark 이미지에 복사된다 (둘 다 빌드 위치 `fds/`).
Spark 이미지에는 `spark/`·`common/` 코드가 들어 있어 k3s는 이미지 그대로 실행하고, 로컬 compose는 같은 폴더를 볼륨으로 덮어써서 재시작만으로 코드가 반영된다.
Spark 이미지에 넣을 파일은 `spark/Dockerfile.dockerignore`가 정한다 (`fds/.dockerignore`는 게이트웨이용).

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
| 8 | R05: 100,000 USDT 이상 | 429 `TEMP_HOLD_R05` + 보내는 지갑 3분 보류 | ② |
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
| R05 | 대량 송금 | 1건 100,000 USDT 이상 | 보내는 지갑 | 거절 |
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
| L2 | 쪼개서 대량 송금 | 24시간 누적 송금 100,000 이상, 모든 거래 100,000 미만 | 자동 동결 |
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
- 같은 룰 + 같은 지갑의 경보는 케이스 하나로 합친다. 거래 시각 기준 10분 넘게 끊기면 새 케이스. 동결 해제된 케이스에는 합치지 않는다.
- 직원이 동결 해제한 지갑은 **해제 시각 이후 기록만** 다시 분석한다 (`fds:unfrozen_at`). 해제 전 거래로 바로 다시 동결되지 않게 하기 위해서다. L7의 '처음 보낸 상대'는 해제 전 기록까지 보고 판단하고, L6은 해제 후 이틀이 지나야 본다.

### 경보 전송 (빠짐·중복 방지)

게이트웨이 outbox와 같은 원리로, 경보를 Kafka로 보내기 전에 Mongo `fds_alert_outbox`에 먼저 적어 둔다.

```text
배치마다: 계산(analyze) → Spark 재계산 → fds_alert_outbox 저장(PENDING) → Redis 처리 표시(mark_done)
          → 못 보낸 경보 전부(이번 배치 + 지난 실패분) fds.alerts.v1 전송 → PUBLISHED
```

| 상황 | 결과 |
|---|---|
| Kafka 전송 실패, Spark 재시작 | 저장해 둔 PENDING 경보를 다음 배치에서 다시 보낸다. 재분석 간격 때문에 같은 경보가 다시 계산되지 않아도 빠지지 않는다 |
| 같은 배치를 다시 돌림 | 경보 번호 `alert_id` = `배치번호:룰:지갑`이 같아서 두 줄로 저장되지 않는다 |
| 보낸 뒤 PUBLISHED 표시 전에 죽음 | 같은 경보가 한 번 더 나간다. 경보 소비자가 케이스의 `alert_ids`로 이미 반영한 번호를 알아보고 케이스는 건드리지 않는다 (동결 여부만 이어서 확인) |

- 처리 표시(재분석 간격, 대기 목록 정리)는 경보를 저장한 **뒤에** 한다. 먼저 하면 전송 실패 뒤 다시 돌릴 때 재분석 간격에 막혀 경보가 사라진다.
- 계기 ③의 '이미 본 동결·블랙리스트 주소' 표시도 L5 대기 목록에 넣은 뒤에 남긴다.
- 보낸 경보는 7일 뒤 자동 삭제. PENDING은 지워지지 않는다.
- 11/26 로컬 확인: 정상 흐름, 같은 경보 재발송, Spark 중지 중 쌓인 경보, Kafka 약 50초 중지 중 쌓인 경보 모두 케이스·감사 원장·동결이 하나씩만 남음.

### Spark 재계산 (L2·L6·L7, `recompute`)

판정은 바꾸지 않고, 경보 지갑의 원본 거래(`transfers` 승인 송금)를 Spark DataFrame으로 다시 계산해 근거로 붙인다. 케이스 `recompute`에 저장되고 케이스 상세·STR 초안 4-1이 보여 준다.

| 항목 | 내용 |
|---|---|
| 판정 값 대조 | L2 누적·최대·건수, L6 오늘·평소, L7 처음 보낸 상대 → `MATCH` / `MISMATCH` |
| 범위 요약 | 송금 횟수·합계·서로 다른 상대 수 |
| 1분 내역 | 1분마다 건수·합계·최대·상대 수 (`minutes`) |
| 5분 이동 윈도 | Spark `window(시각, "5 minutes", "1 minute")`로 5분 구간(1분 간격)마다 송금 횟수·금액·서로 다른 상대 수. 가장 많이 보낸 구간(`peak_count`), 상대가 가장 많았던 구간(`peak_receivers`), 최근 60개 구간 (`windows_5m`) |

- 상시 스트리밍 집계(워터마크·상태 저장)가 아니라, 경보가 뜬 지갑만 경보 시점에 원본으로 계산한다. 모든 주소를 계속 집계하는 것보다 비용이 작고 판정 근거와 같은 범위를 본다.
- 범위 안 거래만 세므로, 범위 처음·끝에 걸친 5분 구간은 일부 시간만 담긴다.

## 직원 API (관제 웹 백엔드용)

관제 웹의 **백엔드 서버**가 부른다. 브라우저에서 바로 부르지 않는다. 헤더 `X-Admin-Key` 필요 (`.env`의 `ADMIN_API_KEY`, 송금 키와 다름). 키를 설정하지 않으면 503 `ADMIN_API_DISABLED`.

주소는 전부 `/api/v1/admin`으로 시작한다 (코드는 `app/admin.py`).

| 기능 | 요청 | 하는 일 |
|---|---|---|
| 케이스 보고 동결 | `POST /cases/{케이스 _id}/freeze` | 케이스 지갑 동결 (사유 = 케이스 룰 → `FROZEN_L5` 등), `fds.freezes.v1`에 `FREEZE`(`source: MANUAL`) |
| 이상 없음 | `POST /cases/{케이스 _id}/dismiss` | 검토 대기 케이스만 닫음 (동결 안 함) |
| 케이스 없이 동결 | `POST /wallets/{지갑}/freeze` | 신고·수사 요청 등으로 바로 동결 (사유 `FROZEN_MANUAL`). 기록용 `MANUAL` 케이스를 만든다 |
| 동결 해제 | `POST /wallets/{지갑}/unfreeze` | 동결 목록·사유에서 빼고 해제 시각 기록, 동결 케이스를 `UNFROZEN`으로, `UNFREEZE` 이벤트 |
| 지갑 상태 | `GET /wallets/{지갑}` | 동결 여부·사유, 블랙리스트, 화이트리스트 룰, 보류 중(남은 초), 해제 시각 |
| 블랙리스트 등록 | `POST /blacklist` `{"wallets": [...]}` | 바로 R07 차단. Spark가 다음 배치에서 상대 지갑 L5 재계산 (계기 ③). 최대 1,000개 |
| 블랙리스트 해제 | `POST /blacklist/{지갑}/remove` | R07 차단 해제. 이미 만들어진 L5 케이스는 그대로 |
| 블랙리스트 목록 | `GET /blacklist?q=&page=1&size=50&history=false` | 등록 중인 주소 (`q`: 주소 일부, `history=true`: 해제 기록까지) |
| 화이트리스트 등록 | `POST /whitelist` `{"wallet": "...", "rules": ["R01", "L"]}` | 고른 룰만 면제. **메모 필수** |
| 화이트리스트 해제 | `POST /whitelist/{지갑}/remove` `{"rules": [...]}` | 고른 룰 면제만 해제 |
| 화이트리스트 목록 | `GET /whitelist?q=&rule=L&page=1&size=50&history=false` | 등록 중인 (지갑, 룰) 목록 |

POST 본문에는 공통으로 `"operator": "직원ID"`, `"memo": "사유"`가 들어간다. 성공하면 200 `{"status":"OK", ...}`, 실패하면 `{"status":"ERROR","reason":...}`.
블랙리스트·화이트리스트 등록은 `added`(새로 등록)와 `already`(이미 등록돼 있던 것)를, 화이트리스트 해제는 `removed`와 `not_listed`를 돌려준다.

| 응답 | 뜻 |
|---|---|
| 200 `FROZEN` / `ALREADY_FROZEN` | 동결함 / 이미 다른 룰로 동결돼 있어 직원 판단만 기록 |
| 200 `DISMISSED` / `UNFROZEN` | 이상 없음 처리 / 동결 해제 (`cases_updated`: 해제로 바뀐 케이스 수) |
| 401 `UNAUTHORIZED` | 직원 키가 틀림 |
| 404 `CASE_NOT_FOUND` | 케이스 없음 |
| 409 `NOT_PENDING` | 검토 대기가 아닌 케이스에 이상 없음 (동결된 케이스는 동결 해제로) |
| 409 `NOT_FROZEN` / `CONFLICT` | 동결되지 않은 지갑 해제 / 다른 직원이 먼저 처리함 |
| 409 `NOT_LISTED` | 등록돼 있지 않은 블랙리스트·화이트리스트 해제 |
| 422 | 본문 형식 오류 (operator 없음, 모르는 룰, 화이트리스트 메모 없음 등) |
| 503 `FDS_UNAVAILABLE` | Redis·Mongo 장애. 같은 요청을 다시 보내면 남은 부분만 처리된다 |

케이스 `review_status`: `PENDING` 검토 대기 → `FROZEN` 직원 동결 / `DISMISSED` 이상 없음, 동결된 케이스(자동은 `null`, 직원은 `FROZEN`) → 해제하면 `UNFROZEN`.
처리한 직원과 시각은 `reviewed_by`, `reviewed_at`, `review_memo` (해제는 `unfrozen_by`, `unfrozen_at`, `unfreeze_memo`)에 남는다.

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
| Mongo | `fds_cases` | L1~L8 케이스. 자동 동결(`WALLET_FROZEN`) / 검토 대기(`REVIEW_PENDING`, `review_status: PENDING`). 직원 처리 상태는 위 직원 API 참고. 케이스 없는 직원 동결은 `rule_id: MANUAL`. 반영한 경보 번호 `alert_ids` (최근 200개) |
| Mongo | `fds_alert_outbox` | Spark 경보 전송 대기함 (`_id` = 경보 번호, `PENDING` → `PUBLISHED`). 전송 후 7일 뒤 자동 삭제 |
| Mongo | `fds_blacklist`, `fds_whitelist` | 블랙리스트·화이트리스트 기록 (등록 한 번 = 문서 하나: 주소, 화이트리스트는 룰, 메모, 등록·해제한 직원과 시각). 해제해도 지우지 않고 `active: false` |
| Redis | `fds:w:send / split / out / in:{지갑}` | 1단계 5분 기록 (6분 뒤 자동 삭제) |
| Redis | `fds:hold:{지갑}` | 보류 중 (값 = 룰, 3분 뒤 자동 삭제) |
| Redis | `fds:frozen_wallets`, `fds:frozen_reasons` | 동결 지갑, 사유 (지갑 → L1~L8) |
| Redis | `fds:unfrozen_at` | 직원이 동결 해제한 시각 (지갑 → ms). Spark는 이 시각 이후 기록만 분석 |
| Redis | `fds:blacklist_wallets` | 블랙리스트 |
| Redis | `fds:whitelist:R01` ~ `R06`, `fds:whitelist:L` | 1단계 룰별 면제, 2단계 분석 면제 |
| Redis | `fds:analyzed:{지갑}`, `fds:analyze_pending` | 재분석 간격, 미룬 지갑 (점수 = 분석할 시각) |
| Redis | `fds:recheck_l5`, `fds:blacklist_seen` | 계기 ③ 대기 목록, 처리한 블랙리스트 |
| Redis | `idemp:*` | 중복 확인 키, 1시간 뒤 삭제 |
| Kafka | `transfer.events.v1` | 통과한 거래 |
| Kafka | `transfer.rejected.v1` | 거절된 요청 (401 제외). ClickHouse 장기 보관용, Spark는 읽지 않음 |
| Kafka | `fds.holds.v1` | 1단계 보류(HOLD), R07 차단(BLOCK) |
| Kafka | `fds.alerts.v1` | Spark L1~L8 경보 |
| Kafka | `fds.freezes.v1` | 동결(`FREEZE`, 자동 `AUTO` / 직원 `MANUAL`)과 해제(`UNFREEZE`). Spark는 `FREEZE`만 계기 ③으로 씀 |

## 바꾸기 쉬운 값

| 값 | 현재 | 위치 |
|---|---|---|
| 시각 허용 범위 `MAX_CLOCK_SKEW` | 30초 | `app/main.py` |
| 1단계 기준, 5분, 보류 시간 | R01 5건, R02 3회, R03·R04 5개, 3분 | `app/holds.py` |
| R05 기준 `WHALE_LIMIT_USDT` (Spark L2도 같이 씀) | 100,000 USDT | `common/keys.py` |
| 보류 기록 보관 `HOLDS_KEEP_DAYS` | 14일 | `app/store.py` |
| `transfers` 자동 삭제 `TRANSFERS_TTL_DAYS` | 0 (끔). ClickHouse 연결 후 14 | `docker-compose.yml` (gateway) |
| 2단계 기준, 기간, 처리량 제한 | L1~L8 숫자, `PROFILES` | `spark/config.py` |
| 자동 동결 룰 `AUTO_FREEZE_RULES` | L1, L2, L3, L8 | `app/alert_consumer.py` |
| 위험도 `RISK_LEVELS` (L5는 등급 사용) | 자동 동결 HIGH, 나머지 MEDIUM | `app/alert_consumer.py` |
| 사건 분리 기준 `CASE_GAP` | 10분 | `app/alert_consumer.py` |

`app/` 코드를 고치면 `docker compose up -d --build <서비스>`로 다시 빌드한다.
`spark/` 코드는 로컬에서 `docker compose restart spark`만 하면 된다. `spark/requirements.txt`를 바꾸면 `docker compose up -d --build spark`.
k3s는 이미지 안의 코드를 쓰므로 `spark/`·`common/`을 고칠 때마다 `docker compose build spark` 후 `deploy/k3s/import_images.sh`로 다시 넣는다.

## 운영 명령

프로젝트 최상단(`C:\aegis-fds`)에서 실행한다.

```bash
# 블랙리스트·화이트리스트 등록과 지갑 동결은 직원 API(위 표)로 한다.
# 지갑 동결 해제는 직원 API(POST /api/v1/admin/wallets/{지갑}/unfreeze)로 한다.
# Redis에서 직접 빼면 케이스 기록·해제 이벤트가 남지 않고, 해제 전 거래로 다시 동결될 수 있다.

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

- 지금 Redis 화이트리스트: 거래소 16개는 `L`(2단계 면제)까지, 시뮬레이터 지갑은 실험용으로 R01~R06 면제다. Redis를 비우거나 새 환경을 만들면 직원 API로 다시 등록해야 한다.
- 관제 웹은 동결·해제를 Redis에 직접 쓰지 않고 직원 API를 부른다 (Redis·이벤트·케이스를 한 번에 처리).
- Spark를 체크포인트 없이 처음 시작하면 그 이후 들어온 메시지만 읽는다 (`STARTING_OFFSETS=latest`).
- 게이트웨이는 `.env`에 `GATEWAY_API_KEY`가 없으면 시작하지 않는다. `ADMIN_API_KEY`가 없으면 직원 API만 꺼진다.
- `TRANSFERS_TTL_DAYS`를 0에서 14로 바꾸면 게이트웨이가 시작할 때 14일 지난 `transfers`를 지우기 시작한다. **ClickHouse에 거래·거절 기록이 쌓이는 걸 확인한 뒤에** 켠다. 다시 0으로 바꾸면 자동 삭제를 끈다.


## 통합 반영 (11/23, DE 브랜치 기능 합침)

| 항목 | 내용 | 위치 |
|---|---|---|
| 감사 원장 `policy_decisions` | 자동 동결(SYSTEM)·직원 조치(ANALYST)·블랙/화이트리스트 변경·STR 저장을 한 줄씩 쌓기만 함 (`decision_id` 고유) | `app/admin.py`, `app/alert_consumer.py`, `app/store.py` |
| 직원 조치 사유 필수 | 요청 본문 `rationale` (없으면 422). 관제 화면 버튼(`POST /cases/{id}/decision`)은 `note`를 넘기고, 비어 있으면 `관제 화면 조치 (사유 미입력)`으로 기록 | `app/admin.py`, `app/admin_views.py` |
| 감사 이력 화면 (`GET /audit`) | 케이스 생성 + 감사 원장을 읽음 | `app/admin_views.py` |
| 근거 막대 기준값 | 케이스에 저장된 `criteria`(Spark가 판정에 쓴 값)를 먼저 씀, 없으면 기존 상수 | `app/admin_views.py` |
| STR 초안 API | `GET`/`PATCH /api/v1/admin/cases/{id}/str-draft` — 케이스 증거·기준값·계산 출처(엔진·설정·배치·시각) 그대로. 프론트 모달은 아직 기존 방식 | `app/str_draft.py` |
| 실시간 경보 WebSocket | `WS /ws/admin` (헤더 `X-Admin-Key`), 새 케이스·변경을 Mongo change stream으로 보냄. 프론트는 아직 5초 폴링 | `app/live_alerts.py` |
| [EVAL] 평가 | 봇 정답 라벨 → 룰별 성적표. `EVAL_ENABLED=0`이면 꺼짐 | `app/eval_poc.py` |
| 429 본문 `retry_after` | 남은 보류 초를 헤더와 본문에 같이 | `app/main.py` |
| 계산 출처 | Spark 경보에 `criteria`·`alerted_at`·`batch_id`, 케이스에 `criteria`·`source` 저장 | `spark/rules.py`, `spark/detector.py`, `spark/stream.py`, `app/alert_consumer.py` |
| L8 위험도 | CRITICAL | `app/wallet_status.py` `RISK_LEVELS` |
| L6 | 평소 기록이 없는 새 지갑은 보지 않음 | `spark/rules.py` |
| **L5 근거 기본값** | `L5_EXPOSURE=all`(자동 동결 포함, 기존 방식). `confirmed`는 비교 측정용 | `spark/config.py` |
| **L1 보류 집계 기본값** | `L1_HOLD_SIDE=sender`(보낸 쪽 보류만) | `spark/config.py`, `docker-compose.yml` |
| 게이트웨이 healthcheck | compose가 `/healthz`를 10초마다 확인 | `docker-compose.yml` |
