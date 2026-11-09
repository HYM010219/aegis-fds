# web

관제 웹 (React + Vite). 송금 화면, FDS 경보 피드, 이더리움 메인넷 USDT 스트림을 보여준다.

- 읽는 데이터 (백엔드 연결 후): MongoDB `fds_cases`, `transfers` / Redis `fds:frozen_wallets`, `fds:frozen_reasons`

## 실행

Node.js 20.19 이상 또는 22.12 이상이 필요하다.

```bash
cd web
npm install        # 처음 한 번
npm run dev        # 개발 서버 → http://localhost:8080
npm run build      # 배포용 빌드 → web/dist
npm run preview    # 빌드 결과 미리보기 (8080)
```

Windows PowerShell에서 `npm` 실행이 막히면 `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`를 한 번 실행하거나 `npm.cmd run dev`로 실행한다.

## 현재 상태

- 메인넷 스트림: 공개 JSON-RPC로 받은 실제 USDT Transfer 데이터. RPC가 모두 실패하면 시뮬레이션으로 전환한다.
- 송금 판정, 경보, 동결: 브라우저 안의 Mock (`src/lib/fds.js`, `src/lib/api.js`). 실제 자산은 이동하지 않는다.
- 룰 번호와 정의는 기획안 v7.4 기준이다. `fds/` 백엔드의 룰 번호와 다르므로 백엔드 연결 때 맞춘다.

## 구조

```
src/
├─ main.jsx, App.jsx      # 진입점, 상태 관리와 화면 조립
├─ config.js              # RPC 주소, USDT 컨트랙트, FDS 기준값, API_BASE
├─ styles.css
├─ hooks/useChain.js      # 메인넷 데이터 수집 시작/정지
├─ lib/
│  ├─ chain.js            # JSON-RPC 조회, Transfer 로그 해석, 시뮬레이션 전환
│  ├─ fds.js              # (Mock) 입구 검사, 스트림 감시, 공격 시연 데이터
│  ├─ api.js              # 백엔드 어댑터 (VITE_API_BASE가 없으면 Mock)
│  └─ util.js
└─ components/            # Topbar, ProfileCard, SendForm, RiskAssessment,
                          # AlertFeed, EvidencePanel, ChainStream, History
```

## 백엔드 연결 (예정)

`.env.example`을 `.env.local`로 복사해 값을 넣으면 Mock 대신 백엔드를 호출한다.

- `VITE_API_BASE=/`
- `VITE_PROXY_TARGET=http://<게이트웨이 주소>:8000` → 개발 서버가 `/api`, `/ws` 요청을 넘겨준다.

`VITE_`로 시작하는 값은 빌드된 JS에 그대로 들어가므로 API 키를 넣지 않는다.
