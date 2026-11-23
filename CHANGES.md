# Aegis-FDS 로컬 테스트 변경 내역 (CHANGES.md)

로컬 E2E 테스트(`local-lab` 브랜치) 중 변경·보완된 코드와 설정 내역을 기록합니다.
팀원들이 본인 브랜치나 운영 환경에 바로 적용할 수 있도록 **[기존 코드(Before) ➔ 변경 코드(After)]** 형태로 상세 표기합니다.

---

## 1. Step 0: 스냅샷 커밋 & 브랜치 통합 (11/18)
- **기준 커밋**: 로컬 통합 테스트 시작 전 `infra` 브랜치 기준 스냅샷
- **브랜치 생성**: `local-lab` (로컬 전용 통합 브랜치)
- **DE 브랜치 병합**: `origin/DE` (commit `46512b0`) 머지
  - **충돌 해결 1 (`fds/app/store.py`)**:
    - **Before**: `req.exchange_name` 직접 참조 (필드 없으면 에러 가능)
    - **After**: `getattr(req, "exchange_name", None)` 안전 조회 적용
  - **충돌 해결 2 (`fds/spark/stream.py`)**:
    - **Before**: 구버전 개별 스트림 루프
    - **After**: DE 신규 `detector.process` 기반 `foreachBatch` 일원화 파이프라인 채택
- **FE 화면 가져오기**: `origin/FE` (commit `c5c4a49`)의 `web/` 디렉터리 checkout

---

## 2. Step 1: ClickHouse 연동 및 compose 설정 (11/18)
- **대상 파일**: `docker-compose.yml`
- **변경 이유**: 로컬 메모리 절약을 위해 profile로 뺐으나, 팀 합의(ClickHouse PoC 포함)에 따라 기본 8개 컨테이너에 포함.

```yaml
# [Before] docker-compose.yml 143~146행
  clickhouse:
    profiles: ["clickhouse"]
    image: clickhouse/clickhouse-server:25.8

# [After] profiles 줄 제거 (기본 docker compose up -d로 자동 기동)
  clickhouse:
    image: clickhouse/clickhouse-server:25.8
```

- **Kafka 토픽 생성**:
  ```bash
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic fds.freezes.v1 --partitions 3 --replication-factor 1
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transfer.rejected.v1 --partitions 3 --replication-factor 1
  ```

---

## 3. Step 4: R01 5번째 송금 200 승인 + HOLD 시작 & 윈도우 키 DEL 리셋 패치 (11/18)
- **배경**: 이용자보호법 제11조(정당한 사유 없는 출금 제한 금지) 준수. 5번째 거래까지는 통과시키고 180초 보류를 시작하며, 만료 후 카운트가 1부터 다시 시작되도록 윈도우 키를 `DEL` 리셋.

### ① `fds/app/holds.py` (Lua 스크립트 수정)

```lua
-- [Before] holds.py EVAL_SCRIPT 내부
-- 5번째 거래일 때 바로 거절 판정 반환
if sends >= r01_min then
    return {"HOLD_SENDER", "R01", ...}
end

-- [After] holds.py EVAL_SCRIPT 내부
-- 5번째 거래는 통과시키면서 보류를 걸고, 윈도우 키(send)를 DEL 리셋!
if sends == r01_min then
    redis.call("SET", hold_key, "R01", "EX", hold_secs)
    redis.call("DEL", send_key)  -- 3분 뒤 무한 보류 락인 방지를 위한 리셋!
    return {"HOLD_SENDER_PASS", "R01", ...}
end
-- 6번째 거래부터는 wallet_status.py에서 fds:hold 키를 감지하여 429 TEMP_HOLD_R01로 거절
```

### ② `fds/app/store.py` (`execute_transfer` 함수)

```python
# [Before] store.py
async def execute_transfer(req, status, reason):
    # transfers 원장만 저장
    ...

# [After] store.py (sender_hold 매개변수 추가로 원자적 outbox 저장)
async def execute_transfer(req, status, reason, sender_hold=None):
    # 1) transfers 컬렉션에 200 ACCEPTED 저장
    # 2) sender_hold가 전달된 경우 fds_holds 문서 동시 생성
    # 3) fds.holds.v1 outbox 이벤트 동시 발행 (한 트랜잭션 보장)
```

### ③ `fds/app/main.py` (`create_transfer` 엔드포인트)

```python
# [Before] main.py
if decision.kind == "HOLD_SENDER":
    return await reject_with_hold(req, decision.hold)

# [After] main.py (5번째 거래 200 반환 처리)
if decision.kind == "HOLD_SENDER_PASS":
    # 200 ACCEPTED 응답 바디에 보류 안내 메타데이터 부착
    body = {
        "status": "ACCEPTED",
        "reason": None,
        "hold_applied": True,
        "hold_rule": decision.hold.rule,
        "hold_until": (datetime.now(timezone.utc) + timedelta(seconds=HOLD_SECONDS)).isoformat()
    }
    await execute_transfer(req, "ACCEPTED", None, sender_hold=decision.hold)
    return JSONResponse(status_code=200, content=body)
```

---

## 4. Step 2: 화이트리스트 정리 및 시드 A 등록 (11/18)
- **배경**: 시뮬레이터 지갑 231개 면제 해제 점검 및 주요 공개 라벨 거래소 핫월렛 9개 면제 등록.
- **적용 명령어 (직원 API 경유)**:
  ```powershell
  # Binance, Upbit, Bithumb, Coinbase, Kraken, OKX 핫월렛 9개 등록
  POST /api/v1/admin/whitelist
  {
    "wallet": "0x28c6c06298d514db089934071355e5743bf21d60",
    "rules": ["R01", "R02", "R03", "R04", "R05", "R06", "L"],
    "operator": "architect",
    "memo": "시드 A: 공개 라벨 거래소 핫월렛"
  }
  ```
- **검증**: `fds:whitelist:R01~R06,L` 각 Set의 카운트가 정확히 9개로 유지됨 확인.

---

## 5. [COLLECTOR] 온체인 수집기 v2.3 파일 녹화 (11/18 ~ 11/19)
- **대상 파일**: `collector/collector_alchemy.py`
- **변경 이유**: 게이트웨이 파이프라인에 실데이터를 쏟아붓지 않고, 로컬 `.jsonl` 파일로만 녹화하여 추후 무한 재생(Replay) 데이터셋 확보 및 API 키 하드코딩 제거.

```python
# [Before] v2.2
# Alchemy 키가 코드에 박혀 있었고, 게이트웨이로만 전송
ALCHEMY_WSS = "wss://eth-mainnet.g.alchemy.com/v2/<API_KEY>"

# [After] v2.3
# 환경 변수 필수 검사 및 --record-only 파일 저장 모드 탑재
ALCHEMY_WSS = os.getenv("ALCHEMY_WSS_URL", "").strip()
if not ALCHEMY_WSS:
    print("❌ ALCHEMY_WSS_URL 환경 변수가 없습니다.")
    sys.exit(1)

# 실행: python collector/collector_alchemy.py --record-only
# 결과: data/onchain/usdt_YYYYMMDD.jsonl 및 .meta.jsonl 에 실시간 기록
```

- **야간 12시간 수집 성과**:
  - 총 수집: **348,583건** (약 35만 건 / 216 MB)
  - 유실률: **0%** (`received == recorded`)
  - 끊김: 4회 (2초 내 자동 재연결)

---

## 6. Step 5: Vite 프록시 보안 키 주입 및 FE 실데이터 연동 (완료)
- **대상 파일**: `web/vite.config.js`, `web/.env.local`
- **목표**: 클라이언트 화면에 키를 노출하지 않고, Vite 서버 프록시에서 `X-API-Key`와 `X-Admin-Key`를 헤더로 실어 FastAPI(8000)로 전달.

```javascript
// [Before] web/vite.config.js
    proxy: {
      "/api": { target: BACKEND, changeOrigin: true },
      "/ws": { target: BACKEND, ws: true, changeOrigin: true },
    }

// [After] web/vite.config.js (경로별 인증 키 자동 주입)
    proxy: {
      "/api/v1/admin": {
        target: BACKEND,
        changeOrigin: true,
        headers: { "X-Admin-Key": process.env.ADMIN_API_KEY }
      },
      "/api/v1/transfers": {
        target: BACKEND,
        changeOrigin: true,
        headers: { "X-API-Key": process.env.GATEWAY_API_KEY }
      },
      "/api": { target: BACKEND, changeOrigin: true },
      "/ws": { target: BACKEND, ws: true, changeOrigin: true },
    }
```
- [FE/INFRA] Vite 프록시가 루트 .env 키를 서버 쪽에서 주입 (vite.config.js, web/.env.local), 키 하드코딩 없음, 백엔드 모드에서는 127.0.0.1 바인딩
- [BE] 통합 검색 API GET /api/v1/admin/search?q= 추가 (admin_views.py, 사건·지갑·거래 접두어 정규식 검색, 최대 8건)
- [BE] 자금 흐름 그래프 API GET /api/v1/admin/cases/{id}/graph 추가 (admin_views.py, 발신·수신 1차 연결망 노드·엣지 생성)
- [BE] 감사 로그 API GET /api/v1/admin/audit 추가 (admin_views.py, 관리자 조치 이력 fds_cases 조회)
- [BE] 대시보드 통계 API GET /api/v1/admin/stats 추가 (admin_views.py, 24시간 거래·동결·보류 KPI 집계)
- [BE/INFRA] K3s 파드 상태 프로브 GET /healthz (liveness), GET /ready (readiness, Redis/Mongo ping) 추가 (main.py)
- [BE] 거래 조회 API GET /api/v1/admin/transfers 추가, FE 실시간 거래 화면에 실데이터 연동 확인 (11/19)
- [BE/INFRA] GET /api/v1/admin/health: 시스템 상태 + 데이터 신뢰성 신호(원장 신선도, outbox 발행 대기, Mongo↔ClickHouse 적재 완결성 SQL 대조, Spark 하트비트 자리). gateway에 ClickHouse 접속 환경 변수 4종 추가
- [BE] GET /api/v1/admin/cases, /cases/{id} 추가: Spark 원본 근거(dict)를 FE 근거 막대 목록으로 변환, 상태 매핑, 응답 모델로 Swagger 계약화. 기준값은 spark/config.py와 중복(To-Be: common/ 통합)
- [E2E] 메인넷 실데이터로 수집기→게이트웨이→Kafka→Spark(L1~L8)→fds_cases→경보화면·상세·STR 초안까지 전 구간 확인. 발견: 수분 만에 경보 46건(L1 자동 동결 다수, 핫월렛 반복 송금) → 화이트리스트 긴급 조치 + demo 기준값 조정 필요성 확인

---

## 7. [발표용 성과] 메인넷 실데이터 E2E 파이프라인 관제 및 실시간 이상거래 탐지 확인 (2026-11-19)

### 1) 실시간 데이터 파이프라인 전 구간 동작 확인
- **전체 파이프라인 경로**:
  `Ethereum Mainnet (Alchemy WSS)` ➔ `collector_alchemy.py` ➔ `FastAPI Gateway (R01~R07 입구 검사)` ➔ `MongoDB Outbox` ➔ `Publisher` ➔ `Kafka 토픽 5종` ➔ `PySpark Streaming (L1~L8 심층 탐지)` ➔ `MongoDB fds_cases` ➔ `ClickHouse 분석 원장` ➔ `FastAPI Admin Views` ➔ `React 관제 콘솔`
- **처리량 및 지연(Latency) 실측 지표**:
  - 인입 처리량: **실측 약 6.4 TPS** (단일 실시간 수집 구간 기준. 30시간 전체 녹화 평균은 7.7건/초)
  - Kafka 토픽 발행 속도:
    - `transfer.events.v1`: **초당 4.3건 (Lag 0)**
    - `transfer.rejected.v1`: **초당 2.1건 (Lag 1)**
    - `fds.holds.v1`: **초당 0.4건 (Lag 0)**
  - 카프카 파티션 Lag: **0 ~ 1건** 유지 (메시지 밀림 없음 확인, 종단 지연은 추후 정밀 측정)
  - 데이터 신뢰성 신호: 원장 신선도 정상, ClickHouse ↔ MongoDB 적재 완결성 (1시간 누적 후 재측정 예정)

### 2) 핵심 발견: 실데이터 거절률 35% 원인 규명 및 "연쇄 동결(Cascading Freeze)" 현상 실측
- **최근 1,000건 판정 결과 통계 분석**:
  - 통과(PASS): 605건 (60.5%)
  - **L1 자동 동결 (FROZEN, L1)**: 258건 (25.8%) ➔ 거절 건수(395건)의 약 65% 점유
  - 고액 송금 보류 (HOLD, R05): 29건 (24h 전체 집계 시 216건으로 최다 원천)
  - 분산 송금 보류 (HOLD, R04): 40건
  - 속도 제한 보류 (HOLD, R01): 21건
  - 순환 거래 동결 (FROZEN, L8): 15건
  - 쪼개기 대량송금 동결 (FROZEN, L2): 6건
- **상위 거절/동결 지갑 분석**:
  - 1위: `0x559432e18b281731c054cd703d4b49872be4ed53` (49건) ➔ **OKX: Hot Wallet 5** (Etherscan 공식 라벨 확인)
  - 2위: `0x000000000004444c5dc75cb358380d2e3de08a90` (32건) ➔ **Uniswap V4: Pool Manager** (DEX 인프라 계약 확인)
- **인과 메커니즘 규명 (화이트리스트 + 기준값 튜닝의 필요성)**:
  - 초당 수십 건을 처리하는 거래소 핫월렛에 대해 사전 화이트리스트 면제가 누락되고, 메인넷 USDT 규모 대비 1단계 기준(R05 5만 USDT 등)이 엄격하여 보류가 누적됨.
  - ➔ Spark Streaming이 `L1(반복 보류)`을 즉각 격발하여 지갑을 `자동 동결(AUTO_FROZEN)` 상태로 등록.
  - ➔ 이후 쏟아지는 해당 핫월렛의 모든 후속 거래가 입구(게이트웨이 동결 검사)에서 `403 FROZEN`으로 차단되어 거절률이 35%까지 폭증함.
  - **교훈**: "화이트리스트 사전 등록"과 함께 메인넷 규모에 맞춘 "R05 고액 기준값 재검토"가 병행되어야 함을 데이터로 규명.

### 3) 실데이터로 발견한 구조적 약점: 오탐 동결이 L5로 번짐 ➔ 조치 및 재측정
- **L2 실데이터 발동 확인**:
  - 대상 지갑: `0xdbbe...4994` (3건 분할, 누적 71,596.17 USDT ➔ HIGH 자동 동결)
  - 정상 분할 송금(DEX/봇) 여부는 미검증 상태이며, L5 검토 대기 직후 L2로 동결된 연쇄 전이 가능성 분석.
- **L5 위험 지갑 접촉 2차 오탐 번짐 현상 (사건 46건 ➔ 122건)**:
  - L1 오탐으로 동결 목록이 오염되자, 메인넷에서 해당 지갑과 정상 거래한 상대방들이 L5(동결 지갑 노출 누적 10,000 USDT 초과)에 줄줄이 걸려 `검토 대기` 사건으로 연속 번짐.
  - **대응 조치**: 거래소/DEX 화이트리스트(`R01, R04, R05, L`) 등록 및 오탐 동결 해제 집행.

### 4) 컴플라이언스 실무 연동: STR 초안 자동 생성 (보고책임자 검토용)
- **STR 초안 생성 모달**:
  - 사건 번호, 혐의 지갑, 의심 거래 요약, 실측치/기준치 판정 근거, 관련 트랜잭션 해시 및 이체 내역 8~9건을 보고책임자 검토용 초안 포맷으로 1-Click 자동 생성 및 `.txt` 다운로드 제공 (공식 법정 서식 아님).
  - 백엔드 Spark Raw JSON(`fds_cases`)을 FE 프론트엔드 기준선 게이지(`[{label, value, threshold, unit, op}]`)로 실시간 변환 매핑.

### 5) 포트폴리오 & 발표용 검증 증거 스크린샷 아카이브 (docs/evidence/20261119/)
| 번호 | 증거 파일 | 핵심 증명 내용 |
|:---:|:---|:---|
| 09 | `09_172556.png` | **시스템 상태 정상, 신선도 0초** (ClickHouse 완결성 수치는 1시간 누적 후 재측정) |
| 10 | `10_173302.jpg` | **Kafka 토픽 처리량 약 6.4 TPS 실측** (통과 4.3 TPS, 거절 2.1 TPS, 발행 대기 0~1건) |
| 11 | `11_173302.png` | **경보 목록 46건 실시간 적재** (L1 반복 보류, L5 위험 지갑 접촉 실시간 탐지) |
| 12 | `12_173302.png` | **사건 상세 분석 화면** (Spark 실측치/기준치 게이지, 입출금 플로우 차트) |
| 13 | `13_173302.jpg` | **STR(의심거래보고) 초안 자동 생성 모달 (보고책임자 검토용)** |
| 14 | `14_173708.jpg` | **라이트모드 122건 폭증 및 실데이터 L2(71,596 USDT) 발동 포착** |
| 15 | `15_174906.png` | **집계 대시보드 실데이터 100% 반영** (KPI 10종, 17:18 실데이터 인입 피크 차트) |

- [관찰] /stats 실측(24h): 1단계 보류 최다 원천은 R05 고액(216) > R03(108) > R04(86) > R01(53). 메인넷 USDT는 5만 이상 이체가 흔함 → 화이트리스트만으로 부족, R05 기준값 재검토 필요. 자동 동결 67→66(해제 1건 반영 확인)

- [BE] POST /cases/{id}/decision 어댑터 (FE action → admin.py freeze/dismiss/unfreeze). 원인: FE·BE 경로·본문 불일치로 404. 사건 상세 보류 이력을 분석 기간으로 제한
- [관찰] Morpho Blue 컨트랙트(0xBBBB…EEFFCb)가 L8 자동 동결 → 예치 지갑들이 L5로 전이. Uniswap V4 PoolManager와 함께 '프로토콜 계약 순환 오인' 패턴. To-Be: 알려진 프로토콜 계약을 동결·L5 노출 계산에서 제외
- [조치] 면제 3곳(OKX Hot Wallet 5, Uniswap V4 PoolManager, Morpho Blue) + 자동 동결 64곳 일괄 해제(사건 66건 RELEASED) + 1호 오탐 처리(Morpho 예치 지갑 L5). 오탐 처리 1건 확인, 해제는 스크립트. 재측정은 기준값 그대로, 면제 효과만 분리
- [Spark·BE 제안→DE 인계] A안: L5 동결 노출 근거를 '직원이 확인한 동결'(fds:frozen_confirmed)+블랙리스트로 제한, 자동 동결 제외. L5_EXPOSURE=confirmed|all 로 전환 가능. 근거: 2차 측정 30분 L5 106건(검토 업무 폭증). 미통과율·자동 동결은 R05 보류 기원이라 본 변경 범위 밖
- [DE 인계] 하트비트 키 fds:heartbeat:spark 연결: Spark 10초 마이크로배치 성공 시 epoch ms 기록, 관제 화면 헬스 프로브 연동
- [3차 재측정 검증 (20:10~20:40)] A안(fds:frozen_confirmed 기반 L5) 실환경 적용 후 30분 검증:
  * 인입 트랜잭션: 12,377건 (전송 12,151건 / 수락 8,409건 / 거절 3,742건)
  * L5 검토 대기(PENDING): 0건 (L5 소멸 확인, 양성 경로는 미검증)
  * 자동 동결: 85건 (L1 69, L2 10, L8 6) / 미통과율: 34.7% (3차 표본은 R03 1위(47건) — B안 대상 재집계 필요)
  * 직원 동결(fds:frozen_confirmed): 0건 (측정 중 직원 동결 조작 없음 확인)
- [BE] 엔터티 목록 fds/common/entities.json(주소·이름·종류·출처·확인일, 사람 확인 4곳) + fds/common/entities.py(Spark 3.8 호환 로더). 관제 API v7이 거래 fromLabel/toLabel, 사건 label, 거래 상대 label을 채움. 다음 단계: L8 계약 제외·L5 노출 계산에 재사용
- [INFRA] AWS 배포 아키텍처: v2.4 §9.6 설계는 데이터 노드를 프라이빗 서브넷에 두고 NAT 게이트웨이 없이 S3 VPC 엔드포인트를 사용함. (v1 임시 단일 노드 운영 경험을 바탕으로 K3s 2노드 분리 매니페스트 준비 완료, 실제 배포 예정)
- [FE 제안] 이름 배지 3개 화면(Alerts.jsx, CaseDetail.jsx, Transfers.jsx) — 아키텍트 로컬 적용, 다음 주 FE 리뷰 후 확정
- [DE 제안] L1_ACTION=freeze|review (기본 freeze). 리플레이 B 비교용: C안(L1 자동 동결 ➔ 검토 대기 전환) 단일 변수 실험 스위치
- [DE 제안] L1_HOLD_SIDE=all|sender (기본 all). 리플레이 E 비교용: E안(L1 집계 시 side == SENDER만 카운트, 수취인 보류 제외) 단일 변수 실험 스위치
- [DE 제안] R05_LIMIT_USDT=50000|100000 (기본 50000). 리플레이 K 비교용: L2(50,000)와 분리된 게이트웨이 R05 대량 송금 전용 임계치 독립 변경 스위치

---

## 8. [실측 검증] 메인넷 골든 데이터셋 4자 비교 리플레이 실측 결과 (2026-11-20)
- **실험 데이터셋**: `data/onchain/golden_r3_30min.jsonl` (12,023건, 메인넷 녹화 구간 2026-11-18 21:46:35 ~ 2026-11-20 04:40:35 KST 중 30분, 실시간 1.0x 배속 재생)
- **녹화 중단 사유**: Alchemy RPC HTTP 429 지속 발생 (월 한도 초과로 추정, Alchemy 대시보드로 확인 예정)
- **실험 비교군**:
  * A안 (Baseline): R05 50k, L1 all, L1 freeze
  * B안 (C안): R05 50k, L1 all, L1 review
  * E안 (E안): R05 50k, L1 sender, L1 freeze
  * K안 (100k안): R05 100k, L1 all, L1 freeze

### 1) 4자 비교 실측 종합
| 지표 | A (기준안) | B (C안) | E (E안) | K (100K안) |
|:---|:---:|:---:|:---:|:---:|
| **총 처리 건수** | 12,023건 | 12,023건 | 12,023건 | 12,023건 |
| **200 통과(PASS)** | 8,323건 (69.2%) | 8,583건 (71.4%) | 8,414건 (70.0%) | 8,452건 (70.3%) |
| **429 응답(보류/재인입)** | 2,003건 (16.7%) | 3,245건 (27.0%) | 2,353건 (19.6%) | 1,887건 (15.7%) |
| **403 거절(동결 재송금)** | 1,697건 (14.1%) | 195건 (1.6%) | 1,256건 (10.4%) | 1,684건 (14.0%) |
| **영향받은 고유 지갑** | **392곳** | **392곳** | **392곳** | **323곳 (-69곳, -17.6%)** |
| **1단계 순수 보류** | **660회** | **822회** | **743회** | **583회 (-77회, -11.7%)** |
| - R05 (고액 송금) | 282회 | 358회 | 287회 | 199회 (-83회, -29.4%) |
| - R03 (자금 집금) | 153회 | 193회 | 214회 | 153회 (동일) |
| - R04 (자금 분산) | 115회 | 134회 | 124회 | 117회 |
| - R01 (고빈도 거래) | 78회 | 99회 | 82회 | 81회 |
| - R06 (핑퐁 거래) | 32회 | 38회 | 36회 | 33회 |
| **L1 동결 지갑** | **67곳** | **0곳 (69곳 검토대기)** | **34곳 (-33곳, -49.3%)** | **65곳 (-2곳)** |
| **총 자동 동결 지갑** | **83곳** | **23곳** | **56곳** | **85곳** |
| **L1 검토 대기 지갑** | **0곳** | **69곳** | **0곳** | **0곳** |

### 2) 핵심 분석 및 사실 검증
1. **R05 임계치(100K)의 효과**:
   - R05 임계치를 50k → 100k로 상향 시 R05 보류는 282회 → 199회로 83회(-29.4%) 감소했으나, L1 동결은 67곳 → 65곳으로 2곳 감소에 그침.
   - 영향받은 고유 지갑 수는 392곳 → 323곳으로 69곳(-17.6%) 감소함.
   - 빠진 69곳 중 68곳(98.6%)은 5만~10만 USDT 사이의 순수 R05 단독 보류 지갑이며, 1곳은 R01 1회 보류 지갑임.
2. **E안(발송 측 보류만 L1 집계)의 효과**:
   - L1 자동 동결 지갑이 67곳 → 34곳으로 33곳(-49.3%) 감소함. E안에서만 동결된 신규 지갑은 0곳(차집합 = 0).
   - 동결에서 제외된 33곳의 A회차 보류 구성: 18곳은 수취 측(R03) 보류만 100% 누적된 지갑이었으며, 9곳은 발송 보류 1회, 5곳은 2회, 1곳은 3회 포함.
   - 403 거절은 441건 감소했으나 429 보류가 350건 증가하여, 순 통과 건수 증가는 +91건(+0.8%p)임. 동결되지 않은 지갑이 송금을 지속하며 일시 보류(R03 153→214회)를 겪음. 영구 차단(동결)이 일시 지연(보류)으로 전환된 구조임.
3. **한계 및 향후 검증**:
   - 조건별 1회 실행이라 자연 변동 폭 미측정 (A2 및 EK 복합안 재측정: 미실시).
   - K회차 쿨다운은 약 13분으로 규칙(20분) 대비 다소 짧았음.
   - E안 및 K안 완화 지갑의 위험 여부(재현율)는 정답 라벨 연동 시험을 통해 검증 예정 (K3s 배포 후 과제).

---

## 9. [이슈 조사] Kafka CPU 간헐적 스파이크 현상 (AWS 실측 확인, 침지 종료 후 튜닝 예정)
- **현상**: 침지 중 `aegis-kafka`의 CPU 사용률이 간헐적으로 200% 이상 스파이크하는 현상 관측 (최종 실측: 최대 243.4%, p95 227.3%, 평균 51.3%).
- **AWS 침지 데이터 25시간 최종 실측 검증 (2026-11-23 종료 기준)**:
  * AWS EC2 침지 서버(Ubuntu, m5.2xlarge)의 `soak_stats.csv` 확인 결과:
    - `aegis-kafka`: 최대 **243.4%** (코어 2.43개 상당), p95 **227.3%**, 평균 **51.3%**
    - `aegis-clickhouse`: 최대 **105.2%**, p95 **48.7%**, 평균 **11.4%** (원인 추정: 병합 부하, 원인 확정 전)
  * **가설 검증 결과**: 로컬 가상화 환경(WSL2)의 수치 왜곡 가설은 AWS 리눅스 실측 데이터로 명백히 반증되어 완전 철회함. 실제 워크로드상 발생하는 부하임이 확인됨.
- **아키텍처 및 리소스 영향**:
  * 현재 m5.2xlarge(8 vCPU)에서는 순간 피크 기준 전체 8코어 대비 약 29% 수준이나,
  * 화요일 분리 예정인 데이터 노드(m5.xlarge vs m5.2xlarge 상향 결정 대기) 환경에서는 Kafka(2.4) + ClickHouse(0.7) 동시 점유 시 Mongo 및 Redis(`docker-compose.aws.yml` 기준 `--appendonly yes` 설정 확인)와의 경합 가능성(분포 확인 후 판단).
- **향후 계획 (원칙: 침지 중 AWS 설정 변경 일체 금지)**:
  1. [침지 성과] AWS Compose 침지 테스트는 11/23 00:31에 25시간 26분 가동 후 정상 중지 완료 (연결 끊김 0건 달성).
  2. [정밀 진단 계획] 로컬 환경에서 단일 변수(`kafka_poll_timeout_ms`, `fetch_wait_max_ms`는 기본 500ms 대비 1000ms 이상 설정 시에만 유의미) 격리 시험을 통해 ClickHouse 외 Spark/컨슈머 복합 영향 여부 검증.
  3. [해결 방안 계획] 침지 완료 및 실측 후 데이터 노드 vCPU 증설(m5.2xlarge 유지) 또는 ClickHouse Kafka 엔진 파라미터 튜닝 PR 적용.

---

## 10. [배포 트러블슈팅] 배포 환경 관제 콘솔 인증 헤더 충돌 현상 (원인 규명 완료, 다음 주 SIT/PR 반영)
- **현상 및 발견 경위**:
  * AWS EC2 침지 서버 Nginx 리버스 프록시 환경에서 관제 콘솔(`/admin/`) 접속 후 로그인 시 Nginx Basic Auth 팝업이 무한 반복 발생.
  * Nginx access log에서 사용자 칸 `-`, 응답 크기 `581` 바이트(Nginx 자체 401 차단 페이지) 실측 확인.
- **원인 분석**:
  1. **헤더 충돌**: FE (`web/src/admin/data/api.js:16`)에서 API 호출 시 `Authorization: Bearer <token>` 헤더를 명시적으로 첨부하여 전송함. HTTP 표준상 단일 `Authorization` 헤더만 허용되므로 브라우저의 Basic Auth 자격증명(`Authorization: Basic <base64>`)이 덮어씌워져 Nginx `auth_basic` 검증에 실패함.
  2. **프록시 키 누락**: 게이트웨이 백엔드 관리자 API(`admin_views.py`)가 요구하는 `X-Admin-Key`를 Nginx 프록시 계층에서 전달하지 않는 구조적 미연결 확인.
- **침지 테스트 운영 방침 (현행 유지)**:
  * 25시간 무인 침지 테스트에는 웹 UI 조작이 불필요하며(지표 무결성 유지를 위해 인위적 조작 금지), 수집기는 Nginx를 거치지 않고 호스트 데몬(systemd)으로 게이트웨이에 직결되어 6초 신선도로 정상 가동했음 (현재는 25시간 침지 완료 후 정상 중지 상태).
  * Nginx 임의 완화 시 공용 IP 환경에서 관리자 API 무인증 노출 및 git 추적 파일(`nginx.conf`) 비밀키 유출 위험이 있으므로 **금일 핫픽스는 일체 보류**.
  * 상태 모니터링은 SSM 터미널 관리자 헬스 신호(`ledger_freshness_s`, `outbox_pending`) 및 `record_stats.sh` 로깅으로 완전 대체.
- **다음 주 SIT 및 정식 PR 계획**:
  1. **FE 헤더 명칭 분리**: `web/src/admin/data/api.js`의 `Authorization` 헤더를 `X-Session-Token` 등으로 변경하여 HTTP Basic Auth와의 충돌 해소.
  2. **보안 잠금 유지**: `/api/v1/admin/` 엔드포인트의 Nginx Basic Auth 2중 잠금 정책 엄격 유지.
  3. **비밀값 안전 주입**: Nginx가 백엔드로 전달할 `X-Admin-Key`는 git 추적 대상 파일이 아닌 비공개 include 파일(`.env` 기반 생성, `.gitignore` 등록)에서 동적 로드하도록 구현.





---

## 11. [DE 통합] DE 브랜치 기능 합침 (11/23)
- **감사 원장 `policy_decisions`** 추가 (자동 동결·직원 조치·목록 변경·STR 저장), **직원 조치 사유 `rationale` 필수**. 화면 `/decision`은 `note` → `rationale`, 비면 `관제 화면 조치 (사유 미입력)`.
- **STR 초안 API** `/cases/{id}/str-draft`, **WebSocket** `/ws/admin`, **[EVAL]** 평가 (`EVAL_ENABLED`).
- Spark 경보·케이스에 **판정 기준값(`criteria`)·계산 출처(`source`: 엔진·설정·배치·시각)** 저장 → 사건 상세 근거 막대·STR 초안이 같은 값.
- **L8 CRITICAL**, **L6 새 지갑 제외**, **429 본문 `retry_after`**, compose 게이트웨이 healthcheck(`/healthz`).
- 기본값 변경: **`L5_EXPOSURE=all`** (자동 동결도 L5 근거 — L5는 검토 대기라 직원이 판단), **`L1_HOLD_SIDE=sender`**.
- 유지: R01 5번째 200+보류, `R05_LIMIT_USDT`, `L1_ACTION`, L8 증거 이름 `path`, ClickHouse `kafka:29092`.
