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

  INITIAL_USDT: 50000,
  INITIAL_ETH: 2.45,

  // FDS 임계치 (기획서 v7.4 기준)
  FDS: {
    TRAVEL_RULE_USDT: 1000,
    EVASION_BAND: [900, 990],
    SMURF_WINDOW_MS: 5 * 60 * 1000,
    SMURF_MIN_COUNT: 3,
    VELOCITY_WINDOW_MS: 60 * 1000,
    VELOCITY_MAX: 10,
    DRAIN_MIN_USDT: 100,
    DRAIN_RATIO: 0.95,
    HOLD_NEW_COUNTERPARTY_USDT: 10000,
    IDEMPOTENCY_WINDOW_MS: 10 * 1000,
    // R07 TEST_AND_STRIKE: 1 USDT 이하 탐색 송금 후 3분 내 동일 수취인에게 3,000 USDT 이상 & 1,000배 이상
    PROBE_MAX_USDT: 1,
    STRIKE_MIN_USDT: 3000,
    STRIKE_MULTIPLIER: 1000,
    STRIKE_WINDOW_MS: 3 * 60 * 1000,
    AUTO_FREEZE_SCORE: 90,
  },
};
