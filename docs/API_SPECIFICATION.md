# Aegis-FDS 공식 백엔드 API 명세서 (API Specification)

> **문서 목적**: 프론트엔드(FE), 데이터(DE), 인프라(INFRA) 팀원이 AWS 서버 가동 여부와 관계없이 오프라인에서도 모든 엔드포인트의 규격, 요청/응답 형식, 에러 코드를 확인하고 연동할 수 있도록 제공합니다.

---

## 1. 기본 접속 및 인증 규격

### 1.1 기본 접속 주소 (Base URL)
* **로컬 개발 환경**: `http://localhost:8000`
* **AWS 클라우드 환경**: `http://<APP_NODE_PUBLIC_IP>` (배포 후 공지)

### 1.2 인증 헤더 (HTTP Headers)
| 헤더 명칭 | 필수 여부 | 사용 대상 엔드포인트 | 설명 |
| :--- | :---: | :--- | :--- |
| `Content-Type` | 필수 | 모든 `POST` 요청 | `application/json` 고정 |
| `X-Admin-Key` | 필수 | `/api/v1/admin/*` | 관리자 전용 API 접근 비밀키 (`.env`의 `ADMIN_API_KEY`) |
| `X-API-Key` | 필수 | `/api/v1/transfers` | 트랜잭션 수집기 전송 비밀키 (`.env`의 `GATEWAY_API_KEY`) |
| `X-Session-Token` | 선택 | `/api/v1/admin/*` | 프론트엔드 로그인 세션 토큰 (`Bearer <token>`) |

---

## 2. 프론트엔드 관제 웹 연동 API (`/api/v1/admin/*`)

### 2.1 상단 통합 검색 (Search)
화면 상단 검색창에 사건 ID, 지갑 주소, 트랜잭션 해시를 입력할 때 접두어(Prefix)로 자동 완성 및 검색 결과를 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/search?q={query}`
* **인증**: `X-Admin-Key` 필수
* **쿼리 파라미터**:
  * `q` (string, 필수): 3글자 이상 입력 시 검색 수행 (미만 시 빈 배열 `[]` 반환)
* **응답**: `200 OK` (최대 8건)
```json
[
  {
    "type": "CASE",
    "id": "case_20261123_001",
    "title": "사건 #case_20261123_001 (L1)",
    "subtitle": "0x5594...ed53 (반복 보류 누적)"
  },
  {
    "type": "WALLET",
    "id": "0x559432e18b281731c054cd703d4b49872be4ed53",
    "title": "0x5594...ed53",
    "subtitle": "동결 지갑 (AUTO_FROZEN)"
  },
  {
    "type": "TX",
    "id": "0x1234abcd5678ef90...",
    "title": "트랜잭션 0x1234...ef90",
    "subtitle": "50,000.00 USDT (R05 고액)"
  }
]
```

---

### 2.2 대시보드 메인 통계 집계 (Dashboard Stats)
대시보드 상단 10종 KPI 카드 및 모니터링 수치를 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/stats`
* **인증**: `X-Admin-Key` 필수
* **응답**: `200 OK`
```json
{
  "total_transfers_24h": 860031,
  "passed_transfers_24h": 558000,
  "hold_transfers_24h": 182031,
  "frozen_transfers_24h": 120000,
  "active_cases_count": 46,
  "frozen_wallets_count": 83,
  "r01_holds": 53,
  "r03_holds": 108,
  "r04_holds": 86,
  "r05_holds": 216
}
```

---

### 2.3 경보/사건 목록 조회 (Alert Cases)
Spark 실시간 이상 탐지 엔진이 감지하여 적재한 FDS 사건 목록을 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/cases`
* **쿼리 파라미터**:
  * `status` (string, 선택): `PENDING` (검토 대기), `AUTO_FROZEN` (자동 동결), `RELEASED` (해제 완료), `DISMISSED` (기각)
* **응답**: `200 OK`
```json
[
  {
    "case_id": "case_20261123_001",
    "rule_id": "L1",
    "risk_level": "HIGH",
    "status": "AUTO_FROZEN",
    "violator_address": "0x559432e18b281731c054cd703d4b49872be4ed53",
    "label": "OKX: Hot Wallet 5",
    "amount": 125000.0,
    "created_at": "2026-11-23T10:00:00Z"
  }
]
```

---

### 2.4 사건 상세 분석 및 판정 근거 (Case Detail)
특정 사건 클릭 시 판정 게이지 바(실측치 vs 기준치) 및 거래 상대방 정보를 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/cases/{case_id}`
* **응답**: `200 OK`
```json
{
  "case_id": "case_20261123_001",
  "rule_id": "L1",
  "status": "AUTO_FROZEN",
  "violator_address": "0x559432e18b281731c054cd703d4b49872be4ed53",
  "label": "OKX: Hot Wallet 5",
  "evidence_bars": [
    {
      "label": "반복 보류 횟수",
      "value": 5,
      "threshold": 3,
      "unit": "회",
      "op": ">="
    }
  ],
  "counterparties": [
    {
      "address": "0x000000000004444c5dc75cb358380d2e3de08a90",
      "label": "Uniswap V4: Pool Manager",
      "in": 50000.0,
      "out": 0.0
    }
  ]
}
```

---

### 2.5 자금 흐름 그래프 (Fund Flow Graph)
사건 혐의 지갑과 1차 입출금 상대방 간의 자금 흐름 연결망(React Flow 노드/엣지)을 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/cases/{case_id}/graph`
* **응답**: `200 OK`
```json
{
  "nodes": [
    {
      "id": "0x5594...ed53",
      "label": "OKX: Hot Wallet 5 (혐의 지갑)",
      "type": "target",
      "risk": "HIGH"
    },
    {
      "id": "0x0000...a90",
      "label": "Uniswap V4 (거래 상대)",
      "type": "counterparty",
      "risk": "LOW"
    }
  ],
  "edges": [
    {
      "source": "0x0000...a90",
      "target": "0x5594...ed53",
      "amount": 50000.0,
      "tx_count": 1
    }
  ]
}
```

---

### 2.6 감사 로그 조회 (Audit Logs)
관리자 및 분석관이 수행한 모든 조치(동결, 오탐 해제, 기각, STR 작성) 내역을 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/audit`
* **응답**: `200 OK`
```json
[
  {
    "audit_id": "aud_001",
    "action": "RELEASE",
    "case_id": "case_20261123_001",
    "operator": "analyst_kim",
    "target_wallet": "0x5594...ed53",
    "reason": "정상 거래소 핫월렛 확인에 따른 오탐 해제",
    "timestamp": "2026-11-23T10:15:00Z"
  }
]
```

---

### 2.7 실시간 거래 원장 목록 (Transfers)
ClickHouse / MongoDB에 유입된 최근 실시간 USDT 트랜잭션 목록을 반환합니다.

* **엔드포인트**: `GET /api/v1/admin/transfers?limit=50`
* **응답**: `200 OK`
```json
[
  {
    "tx_hash": "0xabcdef...",
    "from_address": "0x1111...2222",
    "to_address": "0x3333...4444",
    "amount": 1500.0,
    "status": "ACCEPTED",
    "rule_applied": null,
    "timestamp": "2026-11-23T10:20:00Z"
  }
]
```

---

### 2.8 데이터 신뢰성 및 시스템 상태 (Health)
* **엔드포인트**: `GET /api/v1/admin/health`
* **응답**: `200 OK`
```json
{
  "status": "OK",
  "ledger_freshness_seconds": 2.1,
  "outbox_pending_count": 0,
  "clickhouse_mongo_reconciliation": "MATCHED",
  "spark_streaming_heartbeat": "ALIVE"
}
```

---

## 3. 관리자 조치 API (버튼 연동용)

### 3.1 사건 조치 어댑터 (Decision)
웹 화면의 [동결 / 오탐 해제 / 기각 / STR 초안 작성] 버튼을 클릭했을 때 호출합니다.

* **엔드포인트**: `POST /api/v1/admin/cases/{case_id}/decision`
* **요청 바디**:
```json
{
  "action": "RELEASE",
  "operator": "analyst_kim",
  "reason": "정상 핫월렛 확인"
}
```
* **사용 가능한 액션값 (`action`)**:
  * `"FREEZE"`: 지갑을 블랙리스트에 올리고 즉시 동결 조치
  * `"RELEASE"`: 오탐으로 인한 자동 동결 해제 (보류 윈도우 리셋)
  * `"DISMISS"`: 추가 조치 없이 사건 기각 (무혐의 종결)
  * `"STR_DRAFT"`: 의심거래보고(STR) 초안 다운로드 감사 로그 기록
* **응답**: `200 OK`
```json
{
  "status": "SUCCESS",
  "case_id": "case_20261123_001",
  "action": "RELEASE",
  "message": "조치가 성공적으로 반영되었습니다."
}
```

---

## 4. 인프라 및 엔진 전용 엔드포인트

| 엔드포인트 | 메서드 | 용도 | 정상 응답 |
| :--- | :---: | :--- | :--- |
| `GET /healthz` | `GET` | K3s Liveness 프로브 (서버 생존 체크) | `200 OK` `{"status":"ok"}` |
| `GET /ready` | `GET` | K3s Readiness 프로브 (Redis/Mongo 준비 체크) | `200 OK` 또는 `503 Unavailable` |
| `POST /api/v1/transfers` | `POST` | 이더리움 메인넷 실시간 USDT 유입 입구 | `200 ACCEPTED`, `429 HOLD`, `403 FROZEN` |

---

## 5. 주요 HTTP 상태 코드 규격 (Error Codes)
* `200 OK / ACCEPTED`: 정상 승인
* `401 Unauthorized`: API 키(`X-Admin-Key` 또는 `X-API-Key`) 누락 또는 불일치
* `403 Forbidden`: `FROZEN_SENDER` 또는 `BLACKLISTED_RECEIVER` (동결/블랙리스트 지갑 차단)
* `409 Conflict`: 이미 동결되었거나 다른 관리자가 먼저 처리한 사건 조치
* `422 Unprocessable Entity`: 시간 오차 초과(±30초 밖) 또는 데이터 규격 불일치
* `429 Too Many Requests`: `TEMP_HOLD_R01~R06` (이상 거래 룰 감지로 인한 일시 보류)
* `503 Service Unavailable`: 데이터베이스 또는 카프카 연결 일시 지연
