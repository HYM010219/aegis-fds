/* ==========================================================
   관제 웹 공통 메타데이터 — 기획서 v8.0 기준
   룰 이름 · 쉬운 설명 · 조치 구분 · 등급 · 상태 · 색
   ========================================================== */
import { CONFIG } from "../../config.js";

const T = CONFIG.FDS;
export const PROFILE = T.PERIOD_PROFILE === "prod" ? "prod" : "demo";
export const P = T.PERIODS[PROFILE];
const isDemo = PROFILE === "demo";

/** 기간 표시: "24시간 (demo 5분)" */
export const LONG_LABEL = isDemo ? "2주 (demo 20분)" : "2주";
export const DAY_LABEL = isDemo ? "24시간 (demo 5분)" : "24시간";
export const PASS_LABEL = isDemo ? "1시간 (demo 1분)" : "1시간";

/* 1단계 게이트웨이 (최근 5분) */
export const R_RULES = {
  R01: { name: "초고빈도 송금", plain: `5분 안에 ${T.R01_MIN_SENDS}건 이상 송금`, kind: "HOLD" },
  R02: { name: "쪼개기 송금", plain: `900~990 USDT를 5분 안에 ${T.R02_MIN_BAND}번 이상`, kind: "HOLD" },
  R03: { name: "자금 집금", plain: `5분 안에 서로 다른 ${T.R03_MIN_SENDERS}곳에서 입금`, kind: "HOLD" },
  R04: { name: "자금 분산", plain: `5분 안에 서로 다른 ${T.R04_MIN_RECEIVERS}곳으로 송금`, kind: "HOLD" },
  R05: { name: "대량 송금", plain: `1건 ${T.R05_WHALE_USDT.toLocaleString()} USDT 이상`, kind: "HOLD" },
  R06: { name: "핑퐁 거래", plain: "A→B 보낸 뒤 5분 안에 B→A", kind: "HOLD" },
  R07: { name: "블랙리스트", plain: "보내거나 받는 지갑이 제재 목록", kind: "BLOCK" },
};

/* 2단계 Spark (최근 2주) — 조치: AUTO = 자동 동결, REVIEW = 검토 대기 */
export const L_RULES = {
  L1: { name: "반복 보류", plain: `${LONG_LABEL} 안에 1단계 보류 ${T.L1_MIN_HOLDS}회 이상`, action: "AUTO", grade: "HIGH" },
  L2: { name: "쪼개서 대량 송금", plain: `${DAY_LABEL} 누적 ${T.L2_DAY_SUM.toLocaleString()} 이상, 한 건은 모두 5만 미만`, action: "AUTO", grade: "HIGH" },
  L3: { name: "장기 쪼개기", plain: `${LONG_LABEL} 안에 900~990 송금 ${T.L3_MIN_BAND}회 이상`, action: "AUTO", grade: "HIGH" },
  L4: { name: "경유 지갑", plain: `1,000 이상 받고 ${PASS_LABEL} 안에 80% 이상 다시 보냄, ${T.L4_MIN}회`, action: "REVIEW", grade: "MEDIUM" },
  L5: { name: "위험 지갑 접촉", plain: "제재 지갑과 돈을 주고받음 / 동결 지갑과 거래", action: "REVIEW", grade: "by-type" },
  L6: { name: "급격한 활동 증가", plain: `오늘 거래 수가 평소의 ${T.L6_MULT}배 이상 (최소 ${T.L6_MIN}건)`, action: "REVIEW", grade: "MEDIUM" },
  L7: { name: "새 상대 집중", plain: `${DAY_LABEL} 안에 처음 보내는 상대 ${T.L7_MIN_NEW}곳 이상`, action: "REVIEW", grade: "MEDIUM" },
  L8: { name: "순환 거래", plain: `A→B→C→A로 ${DAY_LABEL} 안에 80% 이상 돌아옴`, action: "AUTO", grade: "HIGH" },
};
export const L5_TYPES = {
  CRITICAL: "제재 지갑에게서 돈을 받음",
  HIGH: "제재 지갑에게 돈을 보냄",
  MEDIUM: `동결 지갑과 누적 ${T.L5_FROZEN_SUM.toLocaleString()} USDT 이상 거래`,
};

export const ruleName = (id) => (R_RULES[id] || L_RULES[id])?.name || id;
export const rulePlain = (id) => (R_RULES[id] || L_RULES[id])?.plain || "";

/* 등급 */
export const GRADES = {
  CRITICAL: { label: "CRITICAL", ko: "긴급", order: 0 },
  HIGH: { label: "HIGH", ko: "높음", order: 1 },
  MEDIUM: { label: "MEDIUM", ko: "보통", order: 2 },
};

/* 사건 상태 (review_status) */
export const STATUS = {
  PENDING: { label: "검토 대기", icon: "●", tone: "review", desc: "담당자 판단이 필요한 사건" },
  AUTO_FROZEN: { label: "자동 동결", icon: "🔒", tone: "frozen", desc: "기준을 넘어 시스템이 바로 동결" },
  FROZEN: { label: "직원 동결", icon: "🔒", tone: "staff", desc: "담당자가 검토 후 동결" },
  DISMISSED: { label: "오탐 처리", icon: "✓", tone: "muted", desc: "검토 결과 정상 거래로 종결" },
  RELEASED: { label: "동결 해제", icon: "↺", tone: "muted", desc: "동결 후 담당자가 해제" },
};

/* 지갑 출처 */
export const ORIGINS = {
  MAINNET: { label: "메인넷", short: "M", tone: "blue", source: "온체인" },
  SIM: { label: "시뮬레이터", short: "S", tone: "gray", source: "SIM" },
  APP: { label: "고객 앱", short: "A", tone: "teal", source: "앱" },
  SDN: { label: "OFAC SDN", short: "!", tone: "violet", source: "온체인" },
  EXCHANGE: { label: "거래소", short: "E", tone: "gray", source: "온체인" },
};

/** 시뮬레이터 지갑은 R01~R06 면제 (기획서 9장 4번) */
export const exemptFromR = (origin) => origin === "SIM";
