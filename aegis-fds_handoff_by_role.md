# Aegis-FDS 담당별 인수 메모 — 사전 점검 결과 (11/18, 아키텍트)

> 기준 코드: GitHub `HYM010219/aegis-fds` — **DE 브랜치(11/17)** 게이트웨이·Spark·직원 API, **FE 브랜치(11/16)** 시뮬레이터·관제 콘솔
> 기준 문서: 룰 정의서 v2.4 PoC (이번 주 동결, 다음 주 재검 후 한 번에 반영)
> 정적 점검: Python 전 파일 문법·pyflakes 통과, Spark 코드 Python 3.8 문법 통과. 아래는 **동작·계약** 차이

---

## 0. 먼저: Git 기준선 (모두)

**현재 상태가 위험합니다.** `main`(11/09)은 옛 구조(Spark가 R01~R06 바로 동결, HOLD·L룰 없음)이고, 최신 코드는 `DE`·`FE` 브랜치에 흩어져 있습니다. 특히 게이트웨이(BE 코드, `fds/app/`)가 `DE` 브랜치에 들어 있어서, `BE` 브랜치(=옛 main)에서 작업을 시작하면 대량 충돌이 납니다.

| 순서 | 할 일 | 누가 |
| --- | --- | --- |
| 1 | 사전 점검 수정분을 `DE`+`FE` 통합 브랜치에 반영 → **`main`으로 머지해 새 기준선** 만들기 | @Baik (이번 주) |
| 2 | 각자 **새 `main`에서** 작업 브랜치 생성: `be/<작업>`, `de/<작업>`, `fe/<작업>`, `infra/<작업>` (옛 `BE`·`DE`·`FE`·`infra` 브랜치는 더 쓰지 않음) | 각 담당 |
| 3 | 작업 → push → **PR로 `main`에 머지** (최소 1명 리뷰). 직접 `main` push 금지 | 각 담당 |
| 4 | 공용 파일(`docker-compose.yml`, `fds/common/keys.py`, `.env.example`, 데이터 규격)은 바꾸기 전 팀에 공지, PR에 담당 전원 리뷰 | 전원 |
| 5 | 머지 후 로컬: `git pull` → 바뀐 서비스만 `docker compose up -d --build <서비스>` | 전원 |

- 파일 담당: `fds/app/` BE(아키텍트, 단 `publisher.py`·`alert_consumer.py`는 DE) · `fds/spark/` DE · `web/` FE · `docker-compose.yml`·`collector/`·배포 INFRA(아키텍트) · `fds/common/` 공용(아키텍트 승인).
- 커밋·PR에 `.env`(키 값)를 절대 올리지 않음. 로그·스크린샷의 API 키도 가림.

---

## 1. BE (`fds/app/`) — 아키텍트 담당 (BE 담당 FE 이동, 11/18 확정)

경계는 파이프라인 단계: 게이트웨이 안 = BE(아키텍트), outbox 이후(`publisher.py`·Kafka·Spark·`alert_consumer.py`) = DE. 담당 파일·계약 전체는 정의서 1.4와 런북 1장.

- `holds.py`는 DE가 작성한 코드로, R01 확정안 반영을 위해 아키텍트가 맡는다.
- 상태 변경(동결·해제·목록 등록·해제)은 직원 API 한 길로. Redis 직접 변경 금지 (조회와 테스트용 `DEL fds:hold:<지갑>`만 허용).

| # | 우선 | 내용 | 현재 코드 | 해야 할 것 |
| --- | --- | --- | --- | --- |
| BE-1 | **높음** | **관제 웹 조회 API 없음** | `admin.py`에 조치 API만 있음 (동결·이상 없음·해제·지갑 상태·블랙/화이트리스트) | FE가 부르는 조회 API 추가: `GET /cases`, `GET /cases/{id}`, `GET /cases/{id}/graph`, `GET /stats`, `GET /transfers`, `GET /audit`, `GET /health` (Mongo 조회). 경로·응답 모양은 FE `web/src/admin/data/api.js`·`store.js`와 맞춤 |
| BE-2 | **높음** | **조치 API 모양이 FE와 다름** | FE: `POST /cases/{id}/decision {action: FREEZE·DISMISS·RELEASE·STR_DRAFT}` / BE: `/cases/{id}/freeze`, `/cases/{id}/dismiss`, `/wallets/{w}/unfreeze` | `decision` 어댑터 추가(기존 함수 호출) 또는 FE가 BE 경로를 직접 부르게 FE와 합의. `STR_DRAFT`는 감사 기록만 |
| BE-3 | **높음** | **R01 5번째 거래** | `holds.py` Lua: 5건째 포함 시 거절(429) + 보류 | 11/09 합의: **5번째는 기록하고 `200`**, 보내는 지갑 보류. 6번째부터 보류 중 `429`. R02·R04·R05는 지금처럼 거절 |
| BE-4 | **높음** | **보류 시 5분 기록 리셋 없음** | 걸린 거래만 기록 안 하고 앞의 4건은 남음 → 3분 보류 해제 직후 1건으로 다시 보류 | Lua에서 보류 거는 순간 R01→`fds:w:send:{from}`, R02→`fds:w:split:{from}`, R03→`fds:w:in:{to}` DEL. `fds:w:out`(R04·R06 공유)은 유지 |
| BE-5 | 중간 | 상태값 매핑 | BE `review_status`: 자동 동결 `null` · `PENDING` · `FROZEN` · `DISMISSED` · `UNFROZEN` / FE: `AUTO_FROZEN` · `RELEASED` | 조회 API 응답에서 FE 값으로 변환하거나 FE가 BE 값을 쓰게 합의 (한쪽으로 통일) |
| BE-6 (DE) | 낮음 | L8 위험도 | `RISK_LEVELS` L8 = `HIGH` | 정의서는 `CRITICAL`. 한 줄 수정 또는 문서 쪽을 HIGH로 (팀 결정) |
| BE-7 | 낮음 | `log_index` 선택값 | 없으면 멱등키가 `tx:None` | 수집기가 항상 보내면 문제 없음. 시뮬레이터는 0 고정 확인 |

확인 완료(정상): 동결·블랙리스트·보류 중 판정 순서, `Retry-After` 남은 초, transactional outbox, 409 중복, R03·R06 받는 지갑 보류, R05 `429`+보류, 블랙리스트·화이트리스트 Mongo 이력(`active`), 직원 처리 기록(`reviewed_by`·`unfrozen_by`).

---

## 2. DE (`fds/spark/`)

| # | 우선 | 내용 | 해야 할 것 |
| --- | --- | --- | --- |
| DE-1 | **높음** | 로컬에서 `Current batch is falling behind` (배치 처리 > 10초) | `docker logs aegis-spark`의 `batch=N {...}` 로그에서 `seconds`·`pending_size` 확인. ClickHouse를 끈 상태(INFRA-2)에서 다시 측정 → 정의서 9.2 실측값 |
| DE-2 | 중간 | `maxOffsetsPerTrigger` 기본 50,000 | 정의서 9.2는 500으로 적혀 있음 → 코드값이 맞는지 확정 (다음 주 문서 반영) |
| DE-3 | 중간 | 시연 기간 충돌 | demo 기간 안에 L3는 R02, L6는 R01, L7은 R04·R01이 먼저 걸림 → 시연 지갑에 해당 1단계 룰 면제가 필요 (INFRA-3과 같이) |
| DE-4 | 낮음 | 계산 방식 변경 시 | 체크포인트 경로(`CHECKPOINT_DIR`)를 새로 잡아야 함 (README 그대로) |

확인 완료(정상): L1~L8 기준값이 DB 요약·정의서와 일치, 3중 트리거, 재분석 간격(prod 60초/demo 15초), L5 소급(블랙리스트 등록 전 2주), 해제 후 재분석(`fds:unfrozen_at`, L6 해제 후 2일).

---

## 3. FE (`web/`)

| # | 우선 | 내용 | 현재 코드 | 해야 할 것 |
| --- | --- | --- | --- | --- |
| FE-1 | **높음** | **관제 콘솔이 Mock 데이터로 동작** | `VITE_API_BASE` 없으면 `store.js`(브라우저 생성 데이터) | BE-1 완료 후 `VITE_API_BASE` 설정해 실연동. 상태값은 BE-5 합의대로 |
| FE-2 | **높음** | **고객 문구가 확정 문구와 다름** | `UserApp.jsx`: "보안 확인 중", "약 n분 뒤", "입금은 그대로 됩니다", "계정 보호를 위해 송금이 제한", "보낼 수 없는 주소" | 429(보류): **"처리 중입니다. 잠시 후 다시 시도해 주세요."** / 403(동결·블랙리스트): **"현재 요청을 처리할 수 없습니다. 자세한 사항은 고객센터로 문의해 주세요."** 남은 시간·사유 표시 삭제 |
| FE-3 | **높음** | 시뮬레이터 송금에 API 키 없음 → 게이트웨이 `401` | 코드 주석대로 브라우저에 키를 두지 않음 | Vite 프록시가 서버 쪽에서 `X-API-Key`를 붙임 (INFRA-4와 같이). 관제 API는 `X-Admin-Key` |
| FE-4 | 중간 | 없는 API 호출 | `GET /api/v1/wallets/{addr}`, `POST /api/v1/admin/wallets/freeze·unfreeze` | 시뮬레이터 잔액은 화면용 Mock 유지, 이 호출 삭제. 동결·해제는 관제 콘솔에서만 |
| FE-5 | 중간 | STR 초안 | 웹이 케이스로 초안 txt 생성 (BE API 없음) | PoC는 웹 생성 유지. 문구는 정의서 6.2("제4조에 따른 의심거래보고 검토가 필요한 것으로 보임")에 맞춤 |

---

## 4. INFRA (`docker-compose.yml`, `collector/`, 배포)

| # | 우선 | 내용 | 해야 할 것 |
| --- | --- | --- | --- |
| INFRA-1 | **높음** | 수집기·봇 코드가 repo에 없음 (`collector/`는 README만) | 코드 위치 확인 후 커밋. 게이트웨이는 시각 ±30초 밖이면 `422` → **실데이터 재생 시 `timestamp`를 현재 시각으로 바꿔 전송**, `log_index` 포함 |
| INFRA-2 | **높음** | ClickHouse가 로컬에서 같이 뜸 (To-Be인데 자원 사용) | compose `profiles: ["clickhouse"]`로 기본 기동에서 제외. `CLICKHOUSE_PASSWORD` 없이도 나머지가 뜨게 |
| INFRA-3 | **높음** | 화이트리스트: 시뮬레이터 지갑이 R01~R06 전부 면제 (README 주의) → 1단계·L1 시연 불가 | DE 브랜치에서 seed 스크립트가 삭제됨 → **직원 API로** 정리: `POST /api/v1/admin/whitelist/{w}/remove`, `POST /api/v1/admin/whitelist` (operator·memo 필수). 시드 A(거래소 핫월렛 `R01~R06`+`L`) / 시드 B(L3→R02, L6→R01, L7→R01·R04, 나머지 면제 없음) |
| INFRA-4 | **높음** | 키 주입 | `.env`: `GATEWAY_API_KEY`, `ADMIN_API_KEY`, `CLICKHOUSE_PASSWORD`. Vite 프록시 서버 쪽 헤더 주입 설정 (FE-3) |
| INFRA-5 | 중간 | 토픽 5개 | **Spark보다 먼저** 생성 (`/opt/kafka/bin/kafka-topics.sh ... --create --if-not-exists`, README 명령 그대로) |
| INFRA-6 | 중간 | 블랙리스트 등록 방식 | Redis `SADD` 직접 대신 `POST /api/v1/admin/blacklist` (Mongo 이력 + Spark L5 소급) |
| INFRA-7 | 중간 | 동결 해제 방식 | Redis `SREM` 직접 금지 → `POST /api/v1/admin/wallets/{w}/unfreeze` (안 그러면 해제 전 거래로 재동결 가능) |
| INFRA-8 | 낮음 | 스택 버전 | 실제: `apache/kafka:4.3.1`(명령은 `/opt/kafka/bin/*.sh`), `redis:7.4`, `mongo:7.0` rs0, ClickHouse 25.8, Spark `local[2]`·2GB → 정의서 1.4 수정 대상 |

---

## 5. 다음 주 문서 반영 대상 (정의서 v2.4 → 코드 기준)

| 정의서 위치 | 바꿀 내용 |
| --- | --- |
| 1.4 | 스택 버전 (INFRA-8) |
| 2.2 | 429 바디에 `retry_after` 없음(헤더만) · R01만 `200`+보류 정보 · `401/409/422/503`, `FROZEN_MANUAL` 추가 |
| 7.1 | 해제는 `fds.freezes.v1`의 `UNFREEZE` 이벤트 (별도 토픽 불필요) |
| 7.2 | `fds_blacklist`·`fds_whitelist` 이미 구현 (To-Be 아님) |
| 감사 | `policy_decisions` 대신 `fds_cases` 처리 필드 + 목록 이력 (PoC), `policy_decisions`는 To-Be |
| 6.2·7.5 | STR은 웹 생성(PoC), 실제 API 경로·헤더(`X-Admin-Key`) |
| 9.2 | `maxOffsetsPerTrigger` 50,000, 배치당 분석 지갑 2,000 |
| 9.3 | L6 "해제 후 2일" 코드 확인 완료 |
