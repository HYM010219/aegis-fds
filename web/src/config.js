/* ==========================================================
   Aegis Wallet 전역 설정
   - VITE_API_BASE 가 비어 있으면 브라우저 내 Mock API 로 동작
   - 백엔드 연결 시 .env.local 에 VITE_API_BASE 지정 (예: "/" 또는 "http://localhost:8000")
   ========================================================== */
const apiBase = (import.meta.env.VITE_API_BASE || "").trim();

export const CONFIG = {
  API_BASE: apiBase === "/" ? window.location.origin : apiBase,

  // 이더리움 메인넷 공개 JSON-RPC (CORS 허용, 순서대로 시도)
  RPC_URLS: [
    "https://ethereum-rpc.publicnode.com",
    "https://1rpc.io/eth",
    "https://eth.drpc.org",
  ],
  CHAIN_ID: 1,
  EXPLORER_TX: "https://etherscan.io/tx/",

  // USDT(ERC-20) 컨트랙트와 Transfer(address,address,uint256) 이벤트 시그니처
  USDT_CONTRACT: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
  USDT_DECIMALS: 6,
  TRANSFER_TOPIC: "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",

  POLL_MS: 12000,          // 이더리움 블록 주기(약 12초)
  ERC20_GAS_LIMIT: 65000,  // USDT transfer 평균 가스

  INITIAL_USDT: 100000,
  INITIAL_ETH: 2.45,

  // FDS 기준값 — 탐지 구조 변경 기획서(2026-11-09) 기준. 숫자는 팀 초안이며 조정 예정
  FDS: {
    // 1단계: 게이트웨이 + Redis (거래 시각 기준 최근 5분, 걸리면 3분 보류 → HTTP 429)
    WINDOW_MS: 5 * 60 * 1000,
    HOLD_MS: 3 * 60 * 1000,
    R01_MIN_SENDS: 5,            // 초고빈도: 같은 지갑 송금 5건 이상
    EVASION_BAND: [900, 990],    // 쪼개기 밴드
    R02_MIN_BAND: 3,             // 쪼개기: 밴드 송금 3회 이상
    R03_MIN_SENDERS: 5,          // 집금: 서로 다른 보낸 지갑 5개 이상 → 받는 지갑 보류
    R04_MIN_RECEIVERS: 5,        // 분산: 서로 다른 받는 지갑 5개 이상
    R05_WHALE_USDT: 50000,       // 대량 송금: 1건 50,000 이상 → 거래 거절 + 보류
    IDEMPOTENCY_WINDOW_MS: 10 * 1000,

    // 2단계: Spark 장기 분석 (걸리면 동결 → HTTP 403 FROZEN_L1~L8)
    PERIOD_PROFILE: (import.meta.env.VITE_PERIOD_PROFILE || "demo").trim(),
    PERIODS: {
      prod: { LONG: 14 * 864e5, DAY: 864e5, PASS: 36e5, label: "운영: 2주 / 24시간 / 1시간" },
      demo: { LONG: 20 * 6e4, DAY: 5 * 6e4, PASS: 6e4, label: "시연: 20분 / 5분 / 1분" },
    },
    ANALYZE_MS: 10 * 1000,       // ② 일반 거래 지갑 분석 주기
    L1_MIN_HOLDS: 3,
    L2_DAY_SUM: 50000,
    L3_MIN_BAND: 10,
    L4_RATIO: 0.8, L4_MIN: 3, L4_MIN_IN: 1000,   // v8.0: 1,000 이상 받고 80% 재송금 3회
    L5_MIN: 5,                   // (구버전 시연 시나리오용)
    L5_FROZEN_SUM: 10000,        // L5 MEDIUM: 동결 지갑과 누적 거래액 (팀 확정 전 임시값)
    L6_MULT: 10, L6_MIN: 20,
    L7_MIN_NEW: 20,
    L8_MAX_WALLETS: 3, L8_MIN_USDT: 1000, L8_RETURN: 0.8,

    // R07 블랙리스트 (시연용 가상 주소) / 화이트리스트(거래소 등, 보류·L8 면제)
    BLACKLIST: [
      "0xbad0000000000000000000000000000000000001",
      "0xbad0000000000000000000000000000000000002",
    ],
    WHITELIST: [],
  },
};
