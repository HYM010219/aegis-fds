/* ==========================================================
 FDS (클라이언트 Mock) — 기획서 v7.4 룰을 화면 데모용으로 재현
 ※ 실제 판정은 백엔드(FastAPI·Redis·PySpark)가 담당할 예정이며,
   이 파일은 VITE_API_BASE 미설정 시에만 사용됩니다.
 ========================================================== */
import { CONFIG } from "../config.js";
import { fmt, short, randAddr, randTxHash, logNormal } from "./util.js";

const T = CONFIG.FDS;
const RISKY_VASP = ["UNVERIFIED_VASP", "UNHOSTED"];

/* ── Fast-Path: 동기 입구 검문 (REJECT / HOLD / PASS) ── */
export function evaluate(ctx) {
  const { wallet, req, history, frozen, idemCache } = ctx;
  const now = Date.now();
  const checks = [];
  const add = (c) => checks.push(c);
  const amount = req.amount;
  const pre = wallet.usdt;
  const ratio = pre > 0 ? amount / pre : 1;
  const toLc = req.to.toLowerCase();
  // 1 USDT 이하 탐색 송금은 '거래 이력'으로 인정하지 않음 (R07 우회 방지)
  const first = !history.some((h) => h.to.toLowerCase() === toLc && h.status === "SETTLED" && h.amount > T.PROBE_MAX_USDT);

  // 1) 동결/블랙리스트 — Redis SISMEMBER fds:frozen_wallets
  const toFrozen = frozen.includes(req.to.toLowerCase());
  const meFrozen = frozen.includes(wallet.address.toLowerCase());
  add({
    id: "BLACKLIST", icon: "⛔", title: "FROZEN / BLACKLIST",
    desc: meFrozen ? "송신 지갑이 동결 상태입니다" : toFrozen ? "수신 지갑이 동결(블랙리스트) 목록에 있습니다" : "동결·블랙리스트 미등록",
    sev: meFrozen || toFrozen ? "high" : "low", result: meFrozen || toFrozen ? "BLOCK" : "PASS", http: 403,
  });

  // 2) R01 중복 송금 — Idempotency-Key 재사용
  const dup = idemCache[req.idempotency_key];
  const isDup = dup && now - dup.at < T.IDEMPOTENCY_WINDOW_MS;
  add({
    id: "R01", icon: "⧉", title: "R01 DOUBLE_SPEND",
    desc: isDup ? `동일 Idempotency-Key 재전송 감지 (원본 ${short(dup.tx_hash, 10, 6)})` : "중복 요청 없음",
    sev: isDup ? "high" : "low", result: isDup ? "BLOCK" : "PASS", http: 409, original: dup,
  });

  // 3) 잔액 조건부 차감 가능 여부 — available_balance >= amount
  const insufficient = amount > pre;
  add({
    id: "BALANCE", icon: "◎", title: "BALANCE CHECK",
    desc: insufficient ? `잔액 부족 (가용 ${fmt(pre)} < 요청 ${fmt(amount)})` : `가용 잔액 충분 (${fmt(pre)} USDT)`,
    sev: insufficient ? "high" : "low", result: insufficient ? "BLOCK" : "PASS", http: 422,
  });

  // 4) R04 전액 유출
  const drain = amount >= T.DRAIN_MIN_USDT && ratio >= T.DRAIN_RATIO && (RISKY_VASP.includes(req.vasp) || first);
  add({
    id: "R04", icon: "⇲", title: "R04 ACCOUNT_DRAIN",
    desc: drain
      ? `잔액의 ${(ratio * 100).toFixed(1)}%를 ${first ? "최초 거래" : ""}${RISKY_VASP.includes(req.vasp) ? " 미신고/개인" : ""} 지갑으로 유출 시도`
      : `소진율 ${(ratio * 100).toFixed(1)}% (기준 ${T.DRAIN_RATIO * 100}%)`,
    sev: drain ? "high" : ratio >= 0.7 ? "mid" : "low", result: drain ? "BLOCK" : "PASS", http: 403, score: 96,
  });

  // 5) 트래블룰 (1,000 USDT 이상)
  let tr = { result: "PASS", sev: "low", desc: `대상 아님 (< ${T.TRAVEL_RULE_USDT.toLocaleString()} USDT)` };
  if (amount >= T.TRAVEL_RULE_USDT) {
    if (!req.beneficiary_name) tr = { result: "BLOCK", sev: "high", desc: "수취인 성명 누락 — 트래블룰 정보 필수", http: 400 };
    else if (RISKY_VASP.includes(req.vasp)) tr = { result: "HOLD", sev: "mid", desc: `${req.vasp}: 트래블룰 미연동 → 수동 검토 보류` };
    else tr = { result: "PASS", sev: "low", desc: `${req.vasp} 와 송수신인 정보 공유 완료` };
  }
  add({ id: "TRAVEL", icon: "⚖", title: "TRAVEL RULE", http: 400, ...tr });

  // 6) 신규 수취인 고액 → 사전 보류(HOLD)
  const bigNew = amount >= T.HOLD_NEW_COUNTERPARTY_USDT && first;
  add({
    id: "HOLD", icon: "⏸", title: "HIGH VALUE · NEW COUNTERPARTY",
    desc: bigNew ? `최초 거래 지갑으로 ${fmt(amount, 0)} USDT 고액 이체 → PENDING_REVIEW` : "고액·신규 조합 아님",
    sev: bigNew ? "mid" : "low", result: bigNew ? "HOLD" : "PASS",
  });

  // 7) R07 초소액 탐색 후 본공격 — 탐색 송금 자체는 면제, 3분 내 후속 고액만 보류
  const probe = history.find((h) => h.to.toLowerCase() === toLc && h.status === "SETTLED"
    && h.amount <= T.PROBE_MAX_USDT && now - h.ts < T.STRIKE_WINDOW_MS);
  const strike = !!probe && amount >= T.STRIKE_MIN_USDT && amount >= probe.amount * T.STRIKE_MULTIPLIER;
  const probeAgo = probe ? Math.round((now - probe.ts) / 1000) : 0;
  add({
    id: "R07", icon: "⚡", title: "R07 TEST_AND_STRIKE",
    desc: strike
      ? `${probeAgo}초 전 ${fmt(probe.amount)} USDT 탐색 송금 후 ${fmt(amount, 0)} USDT 본송금 (${Math.round(amount / probe.amount).toLocaleString()}배) → 보류`
      : probe ? `탐색 송금 이력 있음 (${probeAgo}초 전) — 후속 금액 기준 미달` : "탐색 송금 패턴 없음",
    sev: strike ? "high" : probe ? "mid" : "low", result: strike ? "HOLD" : "PASS",
  });

  // 8) R02 쪼개기 밴드 (Stream-Path 예고)
  const inBand = amount >= T.EVASION_BAND[0] && amount <= T.EVASION_BAND[1];
  const bandRecent = history.filter((h) => h.to.toLowerCase() === req.to.toLowerCase() && h.amount >= T.EVASION_BAND[0] && h.amount <= T.EVASION_BAND[1] && now - h.ts < T.SMURF_WINDOW_MS);
  add({
    id: "R02", icon: "⋔", title: "R02 SMURFING (band)",
    desc: inBand ? `트래블룰 직하 밴드(${T.EVASION_BAND.join("~")}) 송금 — 5분 내 동일 수취인 ${bandRecent.length + 1}회` : "회피 밴드 아님",
    sev: inBand ? (bandRecent.length + 1 >= T.SMURF_MIN_COUNT ? "high" : "mid") : "low", result: inBand ? "WARN" : "PASS",
  });

  // 9) R03 빈도 급증
  const recent1m = history.filter((h) => now - h.ts < T.VELOCITY_WINDOW_MS).length + 1;
  add({
    id: "R03", icon: "≋", title: "R03 VELOCITY_SPIKE",
    desc: `최근 1분 송금 ${recent1m}건 (기준 ${T.VELOCITY_MAX}건 초과)`,
    sev: recent1m > T.VELOCITY_MAX ? "high" : recent1m > T.VELOCITY_MAX / 2 ? "mid" : "low",
    result: recent1m > T.VELOCITY_MAX ? "WARN" : "PASS",
  });

  // 최종 판정
  const block = checks.find((c) => c.result === "BLOCK");
  const hold = checks.find((c) => c.result === "HOLD");
  let decision = "SETTLED", http = 200, reason = "모든 사전 검증 통과";
  if (block) { decision = block.id === "R01" ? "DUPLICATE" : "REJECTED"; http = block.http; reason = block.desc; }
  else if (hold) { decision = "PENDING_REVIEW"; http = 202; reason = hold.desc; }

  const sevW = { high: 35, mid: 12, low: 0, none: 0 };
  const risk = Math.min(99, 6 + checks.reduce((s, c) => s + sevW[c.sev], 0));

  // Stream-Path에서 사후 발생할 경보 (정산된 경우만)
  const alerts = [];
  if (decision === "SETTLED" && inBand && bandRecent.length + 1 >= T.SMURF_MIN_COUNT) {
    alerts.push(makeAlert("R02_SMURFING", 94.5, "high", req.to,
      `5분 내 동일 수취인에게 ${bandRecent.length + 1}회 트래블룰 직하(${T.EVASION_BAND.join("~")}) 분할 송금`,
      { window_minutes: 5, band_tx_count: bandRecent.length + 1, threshold_count: T.SMURF_MIN_COUNT,
        threshold_evasion_band: T.EVASION_BAND, exchange_vasp: req.vasp,
        total_amount: fmt([...bandRecent.map((h) => h.amount), amount].reduce((a, b) => a + b, 0)),
        related_tx_hashes: bandRecent.map((h) => h.tx_hash) }));
  }
  if (decision === "SETTLED" && recent1m > T.VELOCITY_MAX) {
    alerts.push(makeAlert("R03_VELOCITY_SPIKE", 88, "mid", wallet.address,
      `1분 내 ${recent1m}건 연속 송금 — 자동화 스크립트 의심`,
      { window_minutes: 1, tx_count_1m: recent1m, threshold: T.VELOCITY_MAX }));
  }
  if (strike && decision === "PENDING_REVIEW") {
    alerts.push(makeAlert("R07_TEST_AND_STRIKE", 93, "high", req.to,
      `${probeAgo}초 전 ${fmt(probe.amount)} USDT 탐색 송금 직후 ${fmt(amount, 0)} USDT 본송금 시도 — 수취 지갑 선제 동결`,
      { test_tx_id: probe.tx_hash, test_amount: fmt(probe.amount), strike_amount: fmt(amount),
        amplification: Math.round(amount / probe.amount), interval_seconds: probeAgo,
        window_minutes: 3, threshold_strike_usdt: T.STRIKE_MIN_USDT, exchange_vasp: req.vasp, to_wallet: req.to }));
  }
  if (drain) {
    // 송신 측(내 지갑) 경보: Fast-Path에서 이미 거절되었으므로 action = REJECT
    alerts.push(Object.assign(makeAlert("R04_ACCOUNT_DRAIN", 96, "high", wallet.address,
      `잔액 ${(ratio * 100).toFixed(1)}% 일괄 유출 시도 차단 (Fast-Path)`,
      { pre_tx_balance: fmt(pre), amount: fmt(amount), drain_ratio: +(ratio * 100).toFixed(2),
        is_first_counterparty: first, is_unverified_vasp: RISKY_VASP.includes(req.vasp), to_wallet: req.to }), { action: "REJECT" }));
  }

  return { decision, http, reason, checks, risk, alerts, is_first_counterparty: first, drain_ratio: ratio };
}

/* ── 표준 FDSAlert (fds.alerts.v1) ── */
let seq = 90;
export function makeAlert(rule, score, sev, target, reason, evidence) {
  const d = new Date();
  const ymd = d.toISOString().slice(0, 10).replace(/-/g, "");
  return {
    case_id: `case_${ymd}_${String(++seq).padStart(4, "0")}`,
    case_dedup_key: `${rule}_${target.slice(0, 10)}_${d.toISOString().slice(0, 16).replace(/[-:]/g, "")}`,
    rule_code: rule, risk_score: score, severity: sev,
    target_wallet: target,
    action: score >= T.AUTO_FREEZE_SCORE ? "AUTO_FREEZE" : "FLAG",
    reason,
    detected_at: d.toISOString(),
    evidence,
  };
}

/* ── 메인넷 스트림 감시 (Stream-Path 관찰용 경량 버전) ──
   동일 수취 주소로 5분 내 서로 다른 송신자 3명 이상이 밴드 금액을 보내면 R02 후보 경보 */
const bandMap = new Map();    // to → [{from, amount, tx, ts}]
const alertLevel = new Map(); // to → {level, ts}
export function observe(tr) {
  const [lo, hi] = T.EVASION_BAND;
  if (tr.amount < lo || tr.amount > hi) return null;
  const now = Date.now();
  const list = (bandMap.get(tr.to) || []).filter((x) => now - x.ts < T.SMURF_WINDOW_MS);
  list.push({ from: tr.from, amount: tr.amount, tx: tr.tx_hash, ts: now });
  bandMap.set(tr.to, list);
  const senders = new Set(list.map((x) => x.from));
  // 수취 주소별 경보 단계: 0 없음 → 1 관찰(3명+) → 2 확정(15명+). 단계가 오를 때만 1회 발행
  const level = senders.size >= 15 ? 2 : senders.size >= 3 ? 1 : 0;
  const prev = alertLevel.get(tr.to);
  const prevLevel = prev && now - prev.ts < T.SMURF_WINDOW_MS ? prev.level : 0;
  if (level > prevLevel) {
    alertLevel.set(tr.to, { level, ts: now });
    const total = list.reduce((s, x) => s + x.amount, 0);
    const sev = level === 2 ? "high" : "mid";
    return makeAlert("R02_SMURFING", sev === "high" ? 94.5 : 72, sev, tr.to,
      `5분 내 ${senders.size}개 지갑이 트래블룰 직하 금액으로 분할 집금`,
      { window_start: new Date(now - T.SMURF_WINDOW_MS).toISOString(), window_end: new Date(now).toISOString(),
        unique_sender_count: senders.size, threshold_sender_count: 15,
        total_inflow_amount: fmt(total), mean_tx_amount: fmt(total / list.length),
        is_registered_hub: false, source: tr.source,
        related_tx_hashes: list.map((x) => x.tx),
        subgraph: { nodes: [tr.to, ...senders], edges: list.map((x) => ({ from: x.from, to: tr.to, amt: x.amount.toFixed(2) })) } });
  }
  return null;
}

/* ── 데모 공격 시나리오 (Bot 3 서브모듈 축소판) ── */
export const Scenarios = {
  // 3B SmurfingSyndicate: 20~35개 대포 지갑 → 보스 지갑
  smurfing() {
    const boss = randAddr();
    const n = 20 + ((Math.random() * 15) | 0);
    return Array.from({ length: n }, () => ({
      tx_hash: randTxHash(), from: randAddr(), to: boss,
      amount: Math.round((950 + Math.random() * 40) * 100) / 100, demo: true,
    }));
  },
  // 3E CircularTransferRing: A→B→C→D→A
  circular() {
    const ring = Array.from({ length: 3 + ((Math.random() * 3) | 0) }, () => randAddr());
    let amt = 20000 + Math.random() * 30000;
    const start = amt;
    const hops = ring.map((from, i) => {
      const h = { tx_hash: randTxHash(), from, to: ring[(i + 1) % ring.length], amount: Math.round(amt * 100) / 100, demo: true };
      amt *= 0.985;
      return h;
    });
    const alert = makeAlert("R05_CIRCULAR", 92, "high", ring[0],
      `${ring.length}-hop 순환 송금 고리 탐지 (금액 보존율 ${((hops[hops.length - 1].amount / start) * 100).toFixed(1)}%)`,
      { cycle_path: [...ring, ring[0]], hop_tx_hashes: hops.map((h) => h.tx_hash),
        rotation_seconds: 180 + ((Math.random() * 100) | 0),
        amount_preservation_ratio: +(hops[hops.length - 1].amount / start).toFixed(3),
        subgraph: { nodes: ring, edges: hops.map((h) => ({ from: h.from, to: h.to, amt: h.amount.toFixed(2) })) } });
    return { hops, alert };
  },
  // 3C VelocitySpammer: 단일 지갑 1분 15~30건
  velocity() {
    const from = randAddr();
    const n = 15 + ((Math.random() * 15) | 0);
    const txs = Array.from({ length: n }, () => ({ tx_hash: randTxHash(), from, to: randAddr(), amount: Math.round(logNormal(3, 0.6) * 100) / 100, demo: true }));
    const alert = makeAlert("R03_VELOCITY_SPIKE", 88, "mid", from, `1분 내 ${n}건 연속 송금 (평균 간격 ${(60000 / n / 1000).toFixed(1)}초)`,
      { tx_count_1m: n, threshold: T.VELOCITY_MAX, avg_interval_ms: Math.round(60000 / n), related_tx_hashes: txs.map((t) => t.tx_hash) });
    return { txs, alert };
  },
};

