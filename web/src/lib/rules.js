/* ==========================================================
   룰 메타데이터와 응답 → 화면 상태 변환
   탐지 구조 변경 기획서(2026-11-09) 기준
   ========================================================== */
import { CONFIG } from "../config.js";

const T = CONFIG.FDS;
export const PERIOD = T.PERIODS[T.PERIOD_PROFILE] || T.PERIODS.demo;

// 1단계: 게이트웨이 + Redis (최근 5분) → 3분 보류
export const STAGE1 = {
  R01: { name: "초고빈도 송금", short: "RAPID_FIRE", desc: `같은 지갑의 송금 ${T.R01_MIN_SENDS}건 이상`, target: "보내는 지갑" },
  R02: { name: "쪼개기 송금", short: "SPLIT", desc: `${T.EVASION_BAND.join("~")} USDT 송금 ${T.R02_MIN_BAND}회 이상`, target: "보내는 지갑" },
  R03: { name: "자금 집금", short: "FAN_IN", desc: `서로 다른 지갑 ${T.R03_MIN_SENDERS}개 이상에서 입금`, target: "받는 지갑" },
  R04: { name: "자금 분산", short: "FAN_OUT", desc: `서로 다른 지갑 ${T.R04_MIN_RECEIVERS}개 이상으로 송금`, target: "보내는 지갑" },
  R05: { name: "대량 송금", short: "WHALE", desc: `1건 ${T.R05_WHALE_USDT.toLocaleString()} USDT 이상 → 거래 거절`, target: "보내는 지갑" },
  R06: { name: "핑퐁 거래", short: "PING_PONG", desc: "A→B 뒤 5분 안에 B→A", target: "먼저 보낸 지갑" },
  R07: { name: "블랙리스트", short: "BLACKLIST", desc: "보내거나 받는 지갑이 블랙리스트 (항상 차단)", target: "-" },
};

// 2단계: Spark (최근 2주, 시연 모드는 20분) → 동결
export const STAGE2 = {
  L1: { name: "반복 보류", desc: `기간 안에 1단계 보류 ${T.L1_MIN_HOLDS}회 이상`, catches: "막혀도 계속 시도" },
  L2: { name: "쪼개서 대량 송금", desc: `하루 누적 ${T.L2_DAY_SUM.toLocaleString()} 이상, 모든 거래는 그 미만`, catches: "R05 회피" },
  L3: { name: "장기 쪼개기", desc: `기간 안에 ${T.EVASION_BAND.join("~")} 송금 ${T.L3_MIN_BAND}회 이상`, catches: "R02 회피" },
  L4: { name: "경유 지갑", desc: `${T.L4_MIN_IN.toLocaleString()} 이상 받은 금액의 ${T.L4_RATIO * 100}% 이상을 빠르게 재송금, ${T.L4_MIN}회 이상`, catches: "돈세탁 중간 지갑" },
  L5: { name: "위험 지갑 접촉", desc: "제재 지갑에게서 받음(CRITICAL) · 제재 지갑에 보냄(HIGH) · 동결 지갑과 거래(MEDIUM)", catches: "범죄 지갑과 연결" },
  L6: { name: "급격한 활동 증가", desc: `하루 거래 수가 평균의 ${T.L6_MULT}배 이상 (최소 ${T.L6_MIN}건)`, catches: "탈취된 지갑" },
  L7: { name: "새 상대 집중", desc: `하루 안에 처음 거래하는 상대 ${T.L7_MIN_NEW}개 이상`, catches: "대포 지갑으로 분산" },
  L8: { name: "순환 거래", desc: `지갑 ${T.L8_MAX_WALLETS}개 이내 순환, 건당 ${T.L8_MIN_USDT.toLocaleString()} 이상, ${T.L8_RETURN * 100}% 이상 회수`, catches: "자전거래" },
};

// 화면 상태 4가지
export const STATES = {
  OK: { label: "정상", cls: "st-ok", icon: "✓" },
  HOLD: { label: "보류", cls: "st-hold", icon: "⏸" },
  BLOCK: { label: "차단", cls: "st-block", icon: "⛔" },
  FROZEN: { label: "동결", cls: "st-frozen", icon: "🔒" },
};

/** HTTP 코드 + 사유 코드 → 화면 상태 (OK | HOLD | BLOCK | FROZEN) */
export function classify(http, reason) {
  if (http === 200) return "OK";
  if (http === 429 || reason?.startsWith("TEMP_HOLD_")) return "HOLD";
  if (reason?.startsWith("FROZEN_")) return "FROZEN";
  return "BLOCK";
}

/** 사유 코드 → 한국어 설명 */
export function reasonText(reason) {
  if (!reason) return "통과";
  let m = reason.match(/^TEMP_HOLD_(R0[1-6])$/);
  if (m) return `3분 임시 보류 · ${m[1]} ${STAGE1[m[1]].name}`;
  m = reason.match(/^FROZEN_(L[1-8])$/);
  if (m) return `동결 · ${m[1]} ${STAGE2[m[1]].name} (장기 분석)`;
  const map = {
    FROZEN_MANUAL: "동결 · 관리자 수동 동결",
    BLACKLISTED_SENDER: "차단 · R07 보내는 지갑이 블랙리스트",
    BLACKLISTED_RECEIVER: "차단 · R07 받는 지갑이 블랙리스트",
    DUPLICATE_REQUEST: "차단 · 중복 요청 (같은 tx_hash)",
    INVALID_AMOUNT: "차단 · 금액 형식 오류",
    TIMESTAMP_OUT_OF_RANGE: "차단 · 거래 시각 오류",
    UNAUTHORIZED: "차단 · API 키 오류",
    FDS_UNAVAILABLE: "차단 · FDS 일시 장애 (503)",
  };
  return map[reason] || reason;
}

/** 남은 밀리초 → "2:41" */
export const mmss = (ms) => {
  const s = Math.max(0, Math.ceil(ms / 1000));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
};
