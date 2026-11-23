# web

Aegis-FDS 웹 (React + Vite). 기획안 v8.0 기준으로 **사용자 송금 화면**과 **관리자(관제) 화면**을 주소로 분리했다.

| 주소 | 화면 | 대상 |
|---|---|---|
| `http://localhost:8080/` | 사용자 송금 화면 (로그인 없음) | 고객. 결과만 쉬운 말로 안내하고 룰 코드는 보여 주지 않는다 |
| `http://localhost:8080/admin/` | 관리자 화면 (로그인 필요) | 분석관. 경보, 사건 근거, 동결·해제·오탐 처리, 지갑 흐름, 감사 기록 |

화면 사용법과 시연 순서는 [`docs/manual.html`](docs/manual.html) (PDF: `docs/Aegis_Wallet_사용자_매뉴얼.pdf`)에 있다.

## 실행

Node.js 20.19 이상 또는 22.12 이상이 필요하다.

```bash
cd web
npm install        # 처음 한 번
npm run dev        # 개발 서버 → http://localhost:8080 , /admin/
npm run build      # 배포용 빌드 → web/dist (index.html + admin/index.html)
npm run preview    # 빌드 결과 미리보기 (8080)
```

Windows PowerShell에서 `npm` 실행이 막히면 `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`를 한 번 실행하거나 `npm.cmd run dev`로 실행한다.

관리자 임시 계정 (백엔드 인증 연결 전까지):

| 아이디 | 비밀번호 | 권한 |
|---|---|---|
| `analyst` | `aegis1234` | 분석관 — 동결 · 해제 · 오탐 처리 가능 |
| `viewer` | `view1234` | 조회 전용 |

## 현재 상태

- **탐지 (Mock)**: `src/lib/engine.js`가 브라우저 안에서 기획안 v8.0을 흉내 낸다.
  - 1단계 R01~R06: 3분 보류 (429), R07 블랙리스트: 차단 (403)
  - 2단계 L1·L2·L3·L8: 자동 동결, L4~L7: 검토 대기 (관리자가 동결 또는 오탐 처리)
  - 시연 모드 기간: 장기 20분 / 하루 5분 / 경유 1분 (`VITE_PERIOD_PROFILE=prod`이면 2주 / 24시간 / 1시간)
- **관리자 데이터**: `src/admin/data/gen.js`가 만든 예시 사건 + 같은 브라우저의 사용자 화면에서 실제로 보낸 송금(localStorage)을 합쳐 보여 준다. 다른 PC·다른 브라우저와는 공유되지 않는다.
- **메인넷 스트림**: 공개 JSON-RPC로 받은 실제 USDT Transfer 데이터 (관리자 화면 › 송금 기록 › 메인넷 탭).
- 실제 자산은 이동하지 않는다.

## 구조

```
index.html                 # 사용자 화면 진입 (/)
admin/index.html           # 관리자 화면 진입 (/admin/)
src/
├─ base.css                # 공통 색상 · 다크/라이트 테마
├─ config.js               # RPC 주소, USDT 컨트랙트, FDS 기준값, API_BASE
├─ lib/
│  ├─ engine.js            # (Mock) 1단계 R01~R07 + 2단계 L1~L8 판정
│  ├─ rules.js             # 룰 이름 · 설명
│  ├─ api.js               # 송금 API 어댑터 (VITE_API_BASE가 없으면 Mock)
│  ├─ chain.js             # JSON-RPC 조회, Transfer 로그 해석
│  └─ util.js
├─ hooks/useChain.js       # 메인넷 데이터 수집
├─ user/                   # 사용자 송금 화면
└─ admin/                  # 관리자 화면 (해시 라우팅 #/alerts, #/cases/:id ...)
   ├─ AdminApp.jsx, main.jsx, router.js, auth.js, ui.jsx
   ├─ pages/               # Dashboard, Alerts, CaseDetail, Flow, Transfers, Audit, System, Login
   └─ data/                # api.js(백엔드 어댑터), hooks.js(TanStack Query), store.js, gen.js, meta.js
docs/                      # 사용자 매뉴얼 (HTML · PDF · 캡처)
```

사용 라이브러리: TanStack Query · TanStack Table v8 · Recharts · React Flow(@xyflow/react) · Sonner.

## 백엔드 연결 (예정)

`.env.example`을 `.env.local`로 복사해 값을 넣으면 Mock 대신 백엔드를 호출한다.

- `VITE_API_BASE=/`
- `VITE_PROXY_TARGET=http://<게이트웨이 주소>:8000` → 개발 서버가 `/api`, `/ws` 요청을 넘겨준다.
- 관리자 화면이 부르는 API: `/api/v1/admin/cases`, `/cases/{id}`, `/cases/{id}/graph`, `POST /cases/{id}/decision`, `/stats`, `/transfers`, `/audit`, `/health` (`src/admin/data/api.js`). 검색은 아직 Mock만 있다.

`VITE_`로 시작하는 값은 빌드된 JS에 그대로 들어가므로 API 키를 넣지 않는다.
