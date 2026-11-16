/* ==========================================================
   FDS Mock 엔진 — 탐지 구조 변경 기획서(2026-11-09) 재현
   1단계: 게이트웨이 + Redis  — 요청마다 최근 5분을 정확히 세어 3분 보류 (HTTP 429)
   2단계: Spark 장기 분석      — 기간(운영 2주 / 시연 20분) 기록으로 L1~L8 → 동결 (HTTP 403)
   ※ VITE_API_BASE 미설정 시에만 사용. 실제 판정은 fds/ 백엔드가 담당
   ========================================================== */
import { CONFIG } from "../config.js";
import { store, randHex } from "./util.js";
import { STAGE1, STAGE2, PERIOD } from "./rules.js";

const T = CONFIG.FDS;
const KEY = "fds_v2";
const lc = (a) => String(a || "").toLowerCase();
const BLACK = new Set(T.BLACKLIST.map(lc));
const WHITE = new Set(T.WHITELIST.map(lc));
const inBand = (x) => x >= T.EVASION_BAND[0] && x <= T.EVASION_BAND[1];
const today = () => new Date().toLocaleDateString("sv-SE");

const fresh = () => ({
  ledger: [],      // 게이트웨이를 통과한 거래 (MongoDB transfers 중 ACCEPTED)
  holds: {},       // Redis: 지갑 → {rule, at, until}
  holdLog: [],     // 429 보류 이벤트 (fds.holds.v1) — L1 계산용
  blockLog: [],    // R07 차단 이벤트 — L5 계산용
  frozen: {},      // Redis fds:frozen_wallets + fds:frozen_reasons
  released: {},    // 동결 해제 시각 (해제 이전 기록으로 재동결하지 않도록)
  idem: {},        // 중복 요청 키
  cases: [],       // MongoDB fds_cases
  flags: {},       // 검토 대기 케이스 중복 방지 (지갑|룰 → 시각)
  stats: { date: today(), holds: 0, blocks: 0 },
});

let S = { ...fresh(), ...(store.get(KEY) || {}) };
const listeners = new Set();
const save = () => store.set(KEY, S);
const emit = () => { save(); listeners.forEach((fn) => fn()); };
let seq = 0;
const caseId = () => `case_${today().replace(/-/g, "")}_${String(++seq).padStart(3, "0")}${randHex(3)}`;

function bumpStat(k) {
  if (S.stats.date !== today()) S.stats = { date: today(), holds: 0, blocks: 0 };
  S.stats[k]++;
}

function prune(now) {
  const keep = now - PERIOD.LONG - 60_000;
  S.ledger = S.ledger.filter((t) => t.ts >= keep).slice(-5000);
  S.holdLog = S.holdLog.filter((h) => h.ts >= keep);
  S.blockLog = S.blockLog.filter((b) => b.ts >= keep);
  for (const k in S.holds) if (S.holds[k].until <= now) delete S.holds[k];
  for (const k in S.idem) if (now - S.idem[k] > 3600_000) delete S.idem[k];
  S.cases = S.cases.slice(0, 120);
}

/* ── 상태 조회 ── */
export function statusOf(addr, now = Date.now()) {
  const a = lc(addr);
  if (S.frozen[a]) return { state: "FROZEN", code: S.frozen[a].code, at: S.frozen[a].at };
  if (BLACK.has(a)) return { state: "BLOCK", code: "R07" };
  const h = S.holds[a];
  if (h && h.until > now) return { state: "HOLD", rule: h.rule, until: h.until };
  return { state: "OK" };
}

/** 최근 5분 내 1단계 집계 (송금 폼 미리보기용) */
export function windowStats(addr, now = Date.now()) {
  const a = lc(addr);
  const since = now - T.WINDOW_MS;
  const sends = S.ledger.filter((t) => t.from === a && t.ts >= since);
  return {
    sends: sends.length,
    band: sends.filter((t) => inBand(t.amount)).length,
    receivers: new Set(sends.map((t) => t.to)).size,
  };
}

export function snapshot(now = Date.now()) {
  const holds = Object.entries(S.holds).filter(([, h]) => h.until > now);
  if (S.stats.date !== today()) S.stats = { date: today(), holds: 0, blocks: 0 };
  return {
    cases: S.cases,
    holdCount: holds.length,
    frozenCount: Object.keys(S.frozen).length,
    stats: S.stats,
    blacklist: [...BLACK],
  };
}

export function subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); }

/** 저장본 다시 읽기 — 관제 웹(다른 탭)에서 동결·해제한 결과를 덮어쓰지 않도록 */
export function reload() { S = { ...fresh(), ...(store.get(KEY) || {}) }; }
if (typeof window !== "undefined") {
  window.addEventListener("storage", (e) => {
    if (e.key === "aegis:" + KEY && e.newValue) { reload(); listeners.forEach((fn) => fn()); }
  });
}

/* ── 케이스 기록 ── */
function holdCase(wallet, rule, txHashes, evidence, now) {
  S.holds[wallet] = { rule, at: now, until: now + T.HOLD_MS };
  S.holdLog.push({ wallet, rule, ts: now });
  bumpStat("holds");
  S.cases.unshift({
    case_id: caseId(), topic: "fds.holds.v1", stage: 1,
    rule_id: rule, rule_name: STAGE1[rule].name, reason_code: `TEMP_HOLD_${rule}`, action: "TEMP_HOLD",
    violator_address: wallet, detected_at: new Date(now).toISOString(), hold_until: now + T.HOLD_MS,
    attempts: 1, tx_hashes: txHashes, evidence: { window: "최근 5분", ...evidence },
  });
  scheduleAnalyze(800); // 계기 ①: 보류 발생 → 즉시 2단계 분석
}

function holdAttempt(wallet, h, now) {
  // 보류 중 재시도도 429 이벤트로 기록 → L1(반복 보류) 근거
  // v8.0: 보류 중 재시도는 429만 돌려주고 L1 계산에는 넣지 않음 (fds_holds 는 새 보류만)
  S.holdLog.push({ wallet, rule: h.rule, ts: now, retry: true });
  const c = S.cases.find((x) => x.action === "TEMP_HOLD" && x.violator_address === wallet && x.hold_until === h.until);
  if (c) c.attempts++;
  scheduleAnalyze(800);
}

function blockCase(wallet, counterparty, reason, now) {
  S.blockLog.push({ wallet, counterparty, reason, ts: now });
  bumpStat("blocks");
  const c = S.cases.find((x) => x.rule_id === "R07" && x.violator_address === wallet && now - Date.parse(x.detected_at) < 60_000);
  if (c) { c.attempts++; return; }
  S.cases.unshift({
    case_id: caseId(), topic: "fds.holds.v1", stage: 1,
    rule_id: "R07", rule_name: STAGE1.R07.name, reason_code: reason, action: "BLOCK",
    violator_address: wallet, detected_at: new Date(now).toISOString(), attempts: 1,
    evidence: { blacklisted_wallet: counterparty },
  });
}

/* ── 1단계: 게이트웨이 요청 처리 ── */
/** req: { tx_hash, from, to, amount }  →  { http, body:{status, reason}, retry_after, checks, receiverHold } */
export function submit(req, now = Date.now()) {
  prune(now);
  const from = lc(req.from), to = lc(req.to), amount = Number(req.amount);
  const exempt = WHITE.has(from);
  const checks = [];
  let out = null;
  const add = (id, title, metric, hit, near = false) => {
    const result = out ? "SKIP" : hit ? hit : near ? "NEAR" : exempt && id.startsWith("R0") && id !== "R07" ? "EXEMPT" : "PASS";
    checks.push({ id, title, desc: STAGE1[id]?.desc ?? "", metric, result });
  };
  const reject = (http, reason, extra = {}) => { out = { http, reason, ...extra }; };

  if (!(amount > 0) || Math.round(amount * 1e6) !== amount * 1e6) reject(422, "INVALID_AMOUNT");

  // 동결 (2단계 결과)
  const fz = S.frozen[from];
  checks.push({ id: "FROZEN", title: "동결 지갑 (2단계 결과)", desc: "Redis fds:frozen_wallets 조회", metric: fz ? `FROZEN_${fz.code}` : "미등록", result: out ? "SKIP" : fz ? "BLOCK" : "PASS" });
  if (!out && fz) reject(403, `FROZEN_${fz.code}`);

  // R07 블랙리스트 (면제 없음)
  const bl = BLACK.has(from) ? "BLACKLISTED_SENDER" : BLACK.has(to) ? "BLACKLISTED_RECEIVER" : null;
  checks.push({ id: "R07", title: "R07 블랙리스트", desc: STAGE1.R07.desc, metric: bl ?? "해당 없음", result: out ? "SKIP" : bl ? "BLOCK" : "PASS" });
  if (!out && bl) { reject(403, bl); blockCase(from, bl === "BLACKLISTED_SENDER" ? from : to, bl, now); }

  // 이미 3분 보류 중
  const h = S.holds[from];
  const held = h && h.until > now;
  checks.push({ id: "HOLD", title: "3분 보류 상태", desc: "보류 중이면 송금만 차단 (입금은 허용)", metric: held ? `TEMP_HOLD_${h.rule}` : "없음", result: out ? "SKIP" : held ? "HOLD" : "PASS" });
  if (!out && held) { reject(429, `TEMP_HOLD_${h.rule}`, { until: h.until }); holdAttempt(from, h, now); }

  // R05 대량 송금 → 거래 거절 + 3분 보류
  const whale = amount >= T.R05_WHALE_USDT && !exempt;
  add("R05", "R05 대량 송금", `${amount.toLocaleString()} / ${T.R05_WHALE_USDT.toLocaleString()}`, whale ? "HOLD" : null);
  if (!out && whale) { reject(429, "TEMP_HOLD_R05", { until: now + T.HOLD_MS }); holdCase(from, "R05", [req.tx_hash], { amount, threshold: T.R05_WHALE_USDT }, now); }

  // 중복 요청
  const dup = !!S.idem[req.tx_hash];
  checks.push({ id: "DUP", title: "중복 요청", desc: "같은 tx_hash + log_index", metric: dup ? "재전송" : "최초", result: out ? "SKIP" : dup ? "BLOCK" : "PASS" });
  if (!out && dup) reject(409, "DUPLICATE_REQUEST");

  // R01, R02, R04 — 보내는 지갑 (이번 요청 포함 최근 5분)
  const since = now - T.WINDOW_MS;
  const sends = S.ledger.filter((t) => t.from === from && t.ts >= since);
  const n1 = sends.length + 1;
  const hit1 = !exempt && n1 >= T.R01_MIN_SENDS;
  add("R01", "R01 초고빈도 송금", `${n1} / ${T.R01_MIN_SENDS}건`, hit1 ? "HOLD" : null, n1 === T.R01_MIN_SENDS - 1);
  if (!out && hit1) { reject(429, "TEMP_HOLD_R01", { until: now + T.HOLD_MS }); holdCase(from, "R01", [...sends.map((t) => t.tx_hash), req.tx_hash], { tx_count_5m: n1, threshold: T.R01_MIN_SENDS }, now); }

  const bandTx = sends.filter((t) => inBand(t.amount));
  const n2 = bandTx.length + (inBand(amount) ? 1 : 0);
  const hit2 = !exempt && inBand(amount) && n2 >= T.R02_MIN_BAND;
  add("R02", "R02 쪼개기 송금", `${n2} / ${T.R02_MIN_BAND}회`, hit2 ? "HOLD" : null, inBand(amount) && n2 === T.R02_MIN_BAND - 1);
  if (!out && hit2) { reject(429, "TEMP_HOLD_R02", { until: now + T.HOLD_MS }); holdCase(from, "R02", [...bandTx.map((t) => t.tx_hash), req.tx_hash], { band_tx_count: n2, band: T.EVASION_BAND, threshold: T.R02_MIN_BAND }, now); }

  const receivers = new Set([...sends.map((t) => t.to), to]);
  const hit4 = !exempt && receivers.size >= T.R04_MIN_RECEIVERS;
  add("R04", "R04 자금 분산", `${receivers.size} / ${T.R04_MIN_RECEIVERS}개`, hit4 ? "HOLD" : null, receivers.size === T.R04_MIN_RECEIVERS - 1);
  if (!out && hit4) { reject(429, "TEMP_HOLD_R04", { until: now + T.HOLD_MS }); holdCase(from, "R04", [...sends.map((t) => t.tx_hash), req.tx_hash], { unique_receivers: receivers.size, threshold: T.R04_MIN_RECEIVERS }, now); }

  // R03, R06 — 상대 지갑을 보류 (이번 거래는 통과, 보류 지갑도 입금은 허용)
  const inbound = S.ledger.filter((t) => t.to === to && t.ts >= since);
  const senders = new Set([...inbound.map((t) => t.from), from]);
  const toHeld = S.holds[to]?.until > now;
  const hit3 = !WHITE.has(to) && senders.size >= T.R03_MIN_SENDERS && !toHeld;
  add("R03", "R03 자금 집금 (받는 지갑)", `${senders.size} / ${T.R03_MIN_SENDERS}개`, hit3 ? "HOLD" : null, senders.size === T.R03_MIN_SENDERS - 1);

  const pong = S.ledger.find((t) => t.from === to && t.to === from && t.ts >= since);
  const hit6 = !!pong && !WHITE.has(to) && !toHeld;
  add("R06", "R06 핑퐁 거래", pong ? `${Math.round((now - pong.ts) / 1000)}초 전 역방향 거래` : "역방향 없음", hit6 ? "HOLD" : null);

  if (out) { emit(); return response(out, checks, now); }

  // 통과 → 원장 저장 (→ Kafka transfer.events.v1)
  S.ledger.push({ tx_hash: req.tx_hash, from, to, amount, ts: now });
  S.idem[req.tx_hash] = now;
  let receiverHold = null;
  if (hit3) { holdCase(to, "R03", [...inbound.map((t) => t.tx_hash), req.tx_hash], { unique_senders: senders.size, threshold: T.R03_MIN_SENDERS }, now); receiverHold = "R03"; }
  else if (hit6) { holdCase(to, "R06", [pong.tx_hash, req.tx_hash], { first_tx: pong.tx_hash, return_tx: req.tx_hash, interval_seconds: Math.round((now - pong.ts) / 1000) }, now); receiverHold = "R06"; }
  emit();
  return { ...response({ http: 200, reason: null }, checks, now), receiverHold };
}

function response(o, checks, now) {
  return {
    http: o.http,
    body: { status: o.http === 200 ? "ACCEPTED" : "REJECTED", reason: o.reason },
    retry_after: o.until ? Math.ceil((o.until - now) / 1000) : null,
    checks,
  };
}

/* ── 2단계: Spark 장기 분석 ── */
let pending = null;
function scheduleAnalyze(ms) {
  clearTimeout(pending);
  pending = setTimeout(() => { pending = null; analyze(); }, ms);
}

// v8.0: L1·L2·L3·L8 은 자동 동결, L4~L7 은 검토 대기 (관제 웹에서 담당자가 동결 / 오탐 처리)
export const AUTO_FREEZE = new Set(["L1", "L2", "L3", "L8"]);
export const GRADE = { L1: "HIGH", L2: "HIGH", L3: "HIGH", L4: "MEDIUM", L6: "MEDIUM", L7: "MEDIUM", L8: "HIGH" }; // L5 는 유형별

export function analyze(now = Date.now()) {
  prune(now);
  let changed = false;
  // 동결이 생기면 상대 지갑(L5 MEDIUM)이 바뀌므로 변화가 없을 때까지 반복
  for (let pass = 0; pass < 3; pass++) {
    const wallets = new Set([...S.ledger.flatMap((t) => [t.from, t.to]), ...S.holdLog.map((h) => h.wallet)]);
    let froze = false;
    for (const w of wallets) {
      if (S.frozen[w] || WHITE.has(w) || BLACK.has(w)) continue;
      const hits = evaluateLong(w, now);
      const auto = hits.find((h) => AUTO_FREEZE.has(h.rule));
      if (auto) { freeze(w, auto, now); froze = changed = true; continue; }
      for (const h of hits) {
        const k = `${w}|${h.rule}`;
        if (S.flags[k] && S.flags[k] > (S.released[w] || 0)) continue;
        S.flags[k] = now;
        addCase(w, h, "PENDING", now);
        changed = true;
      }
    }
    if (!froze) break;
  }
  if (changed) emit(); else save();
}

function evaluateLong(w, now) {
  const hits = [];
  const since = Math.max(now - PERIOD.LONG, S.released[w] || 0);
  const daySince = Math.max(now - PERIOD.DAY, S.released[w] || 0);
  const mine = S.ledger.filter((t) => t.ts >= since && (t.from === w || t.to === w));
  const sends = mine.filter((t) => t.from === w);
  const recvs = mine.filter((t) => t.to === w);
  const daySends = sends.filter((t) => t.ts >= daySince);
  const hashes = (arr) => arr.map((t) => t.tx_hash);

  // L1 반복 보류 (새 보류만)
  const holds = S.holdLog.filter((h) => h.wallet === w && h.ts >= since && !h.retry);
  if (holds.length >= T.L1_MIN_HOLDS)
    hits.push({ rule: "L1", tx_hashes: [], evidence: { hold_count: holds.length, threshold: T.L1_MIN_HOLDS, holds: holds.map((h) => ({ rule: h.rule, at: new Date(h.ts).toISOString() })) } });

  // L2 쪼개서 대량 송금
  const daySum = daySends.reduce((s, t) => s + t.amount, 0);
  if (daySends.length >= 2 && daySum >= T.L2_DAY_SUM && daySends.every((t) => t.amount < T.R05_WHALE_USDT))
    hits.push({ rule: "L2", tx_hashes: hashes(daySends), evidence: { day_total: +daySum.toFixed(2), tx_count: daySends.length, max_single: Math.max(...daySends.map((t) => t.amount)), threshold: T.L2_DAY_SUM } });

  // L3 장기 쪼개기
  const band = sends.filter((t) => inBand(t.amount));
  if (band.length >= T.L3_MIN_BAND)
    hits.push({ rule: "L3", tx_hashes: hashes(band), evidence: { band_tx_count: band.length, band: T.EVASION_BAND, threshold: T.L3_MIN_BAND } });

  // L4 경유 지갑: 1,000 이상 받고 PASS 시간 안에 80% 이상 재송금
  const used = new Set();
  const pairs = [];
  for (const s of [...sends].sort((a, b) => a.ts - b.ts)) {
    const r = recvs.find((r) => !used.has(r.tx_hash) && r.amount >= T.L4_MIN_IN && r.ts <= s.ts && s.ts - r.ts <= PERIOD.PASS && s.amount >= r.amount * T.L4_RATIO);
    if (r) { used.add(r.tx_hash); pairs.push({ in_tx: r.tx_hash, in_amount: r.amount, out_tx: s.tx_hash, out_amount: s.amount, seconds: Math.round((s.ts - r.ts) / 1000) }); }
  }
  if (pairs.length >= T.L4_MIN)
    hits.push({ rule: "L4", tx_hashes: pairs.flatMap((p) => [p.in_tx, p.out_tx]), evidence: { pass_through_count: pairs.length, ratio: T.L4_RATIO, threshold: T.L4_MIN, pairs } });

  // L5 위험 지갑 접촉 (통과 거래만, R07 차단 시도는 제외)
  //   CRITICAL: 제재 지갑에게서 받음 / HIGH: 제재 지갑에게 보냄 / MEDIUM: 동결 지갑과 누적 거래액
  const fromSdn = recvs.filter((t) => BLACK.has(t.from));
  const toSdn = sends.filter((t) => BLACK.has(t.to));
  const withFrozen = mine.filter((t) => S.frozen[t.from === w ? t.to : t.from]);
  const frozenSum = withFrozen.reduce((s, t) => s + t.amount, 0);
  const l5 = fromSdn.length ? ["CRITICAL", fromSdn] : toSdn.length ? ["HIGH", toSdn] : frozenSum >= T.L5_FROZEN_SUM ? ["MEDIUM", withFrozen] : null;
  if (l5)
    hits.push({ rule: "L5", risk_level: l5[0], tx_hashes: hashes(l5[1]), evidence: { sdn_received: +fromSdn.reduce((s, t) => s + t.amount, 0).toFixed(2), sdn_sent: +toSdn.reduce((s, t) => s + t.amount, 0).toFixed(2), frozen_total: +frozenSum.toFixed(2), frozen_threshold: T.L5_FROZEN_SUM } });

  // L6 급격한 활동 증가: 오늘 송금 수 ≥ 이전 하루 평균 × 10 (최소 20건)
  const days = Math.max(1, Math.round(PERIOD.LONG / PERIOD.DAY) - 1);
  const avg = (sends.length - daySends.length) / days;
  if (daySends.length >= T.L6_MIN && daySends.length >= T.L6_MULT * avg)
    hits.push({ rule: "L6", tx_hashes: hashes(daySends).slice(0, 30), evidence: { today_count: daySends.length, daily_avg: +avg.toFixed(2), multiplier: T.L6_MULT, min: T.L6_MIN } });

  // L7 새 상대 집중: 하루 안에 처음 보내는 상대
  const fresh = new Set();
  for (const s of daySends) {
    const before = S.ledger.some((t) => t.ts < s.ts && ((t.from === w && t.to === s.to) || (t.from === s.to && t.to === w)));
    if (!before) fresh.add(s.to);
  }
  if (fresh.size >= T.L7_MIN_NEW)
    hits.push({ rule: "L7", tx_hashes: hashes(daySends).slice(0, 30), evidence: { new_counterparties: fresh.size, threshold: T.L7_MIN_NEW } });

  // L8 순환 거래: A→B→(C)→A, 하루 안에, 건당 1,000 이상, 80% 이상 회귀
  const edges = S.ledger.filter((t) => t.ts >= daySince && t.amount >= T.L8_MIN_USDT && !WHITE.has(t.from) && !WHITE.has(t.to));
  const out = (a, after) => edges.filter((e) => e.from === a && e.ts > after);
  const close = (e1, path) => {
    const last = path[path.length - 1];
    if (last.to !== w || last.amount < e1.amount * T.L8_RETURN) return null;
    return { rule: "L8", tx_hashes: hashes(path), evidence: { cycle_path: [w, ...path.map((p) => p.to)], hop_amounts: path.map((p) => p.amount), return_ratio: +(last.amount / e1.amount).toFixed(3), rotation_seconds: Math.round((last.ts - e1.ts) / 1000) } };
  };
  let l8 = null;
  for (const e1 of out(w, -1)) {
    for (const e2 of out(e1.to, e1.ts)) {
      if (e2.to === w) { l8 = l8 || close(e1, [e1, e2]); continue; }
      if (T.L8_MAX_WALLETS < 3 || e2.to === e1.from) continue;
      for (const e3 of out(e2.to, e2.ts)) if (e3.to === w) l8 = l8 || close(e1, [e1, e2, e3]);
    }
  }
  if (l8) hits.push(l8);
  return hits;
}

function addCase(w, hit, review, now) {
  S.cases.unshift({
    case_id: caseId(), topic: review === "AUTO_FROZEN" ? "fds.freezes.v1" : "fds.alerts.v1", stage: 2,
    rule_id: hit.rule, rule_name: STAGE2[hit.rule].name, risk_level: hit.risk_level || GRADE[hit.rule],
    reason_code: `FROZEN_${hit.rule}`, action: review === "AUTO_FROZEN" ? "FREEZE" : "REVIEW", review_status: review,
    violator_address: w, detected_at: new Date(now).toISOString(), tx_hashes: hit.tx_hashes,
    evidence: { period_profile: T.PERIOD_PROFILE, period: PERIOD.label, ...hit.evidence },
  });
}

function freeze(w, hit, now) {
  S.frozen[w] = { code: hit.rule, at: now };
  addCase(w, hit, "AUTO_FROZEN", now);
}

/* ── 관리자 동결 / 해제 ── */
export function manualFreeze(addr, code = "MANUAL") {
  const a = lc(addr);
  if (!S.frozen[a]) S.frozen[a] = { code, at: Date.now() };
  emit();
}

/** 관제 웹 연동용: 통과 거래 원장 · 보류/차단 기록 (읽기 전용 사본) */
export const ledger = () => S.ledger.slice();
export const holdLog = () => S.holdLog.slice();
export const blockLog = () => S.blockLog.slice();
export function unfreeze(addr) {
  const a = lc(addr);
  delete S.frozen[a];
  delete S.holds[a];
  S.released[a] = Date.now();
  S.cases.forEach((c) => { if (c.violator_address === a && !c.released_at && (c.action === "FREEZE" || c.action === "TEMP_HOLD" || c.action === "REVIEW")) c.released_at = new Date().toISOString(); });
  emit();
}

/* ── 시연용: 과거 기록 주입 (게이트웨이를 거치지 않은 이전 데이터) ── */
export function seed(txs) {
  S.ledger.push(...txs.map((t) => ({ ...t, from: lc(t.from), to: lc(t.to) })));
  S.ledger.sort((a, b) => a.ts - b.ts);
  emit();
  scheduleAnalyze(800);
}

/** 다음 분석을 앞당김 (시연용: 10초 주기를 기다리지 않음) */
export const analyzeSoon = (ms = 1200) => scheduleAnalyze(ms);

/* ── 주기 분석 (계기 ②: 10초마다) ── */
let timer = null;
export function start() { if (!timer) timer = setInterval(() => analyze(), T.ANALYZE_MS); }
export function stop() { clearInterval(timer); timer = null; }

export function reset() { S = fresh(); emit(); }
