/* ==========================================================
   관제 웹 Mock 저장소
   = 백엔드(관제 API · MongoDB · Kafka)가 하는 일을 브라우저에서 흉내
   - 초기 데이터: gen.js (기획서 v8.0 규칙)
   - 실시간: 2초마다 유입·보류·차단·새 사건 (WebSocket 흉내)
   - 고객 앱 연동: 같은 브라우저의 고객 앱(Mock 엔진) 보류·차단·사건을 합쳐서 보여 줌
   - 조치(동결·오탐·해제·STR)는 감사 기록으로 남기고 localStorage에 보관
   ========================================================== */
import { CONFIG } from "../../config.js";
import { store as ls, hash } from "../../lib/util.js";
import * as Engine from "../../lib/engine.js";
import { generate } from "./gen.js";
import { P, L_RULES, ORIGINS, L5_TYPES } from "./meta.js";

const T = CONFIG.FDS;
const MIN = 60_000;
const lc = (a) => String(a || "").toLowerCase();

const { db, g } = generate(Date.now());
const listeners = new Set();
const caseListeners = new Set();
let version = 0;
let paused = false;

/* ── 담당자 조치 · 감사 기록 (브라우저 보관, 추가만 가능) ── */
const decisions = ls.get("admin_decisions", {});
const myAudit = ls.get("admin_audit", []);
const saveDecisions = () => { try { ls.set("admin_decisions", decisions); ls.set("admin_audit", myAudit.slice(-500)); } catch { /* 저장 불가 환경 */ } };

let cache = null; // 버전별 합친 데이터 캐시
function emit() {
  version++;
  cache = null;
  listeners.forEach((fn) => fn(version));
}
export const subscribe = (fn) => { listeners.add(fn); return () => listeners.delete(fn); };
export const onNewCase = (fn) => { caseListeners.add(fn); return () => caseListeners.delete(fn); };
export const getVersion = () => version;

/* ── 실시간 흉내 (WebSocket 대신) ── */
const LIVE_SPECS = ["L1", "L2", "L3", "L4", "L4", "L5:MEDIUM", "L6", "L7", "L7", "L8"];
function tick() {
  if (paused) return;
  const now = Date.now();
  // 분당 유입 카운터
  const m = Math.floor(now / MIN) * MIN;
  let last = db.inflow[db.inflow.length - 1];
  if (!last || last.t < m) { last = { t: m, n: 0 }; db.inflow.push(last); }
  last.n += Math.round(1000 / 30 * g.rand(0.85, 1.15));
  if (db.inflow.length > 24 * 60 + 2) db.inflow.shift();

  for (let i = 0; i < 2; i++) g.makeSimTransfer(now - g.rand(0, 1500));
  if (g.R() < 0.12) g.makeHold(now);
  if (g.R() < 0.025) g.makeBlock(now);
  if (g.R() < 0.05) g.makeFrozenTry(now);
  if (g.R() < 0.045) {
    const c = g.makeCase(g.pick(LIVE_SPECS), now);
    db.audit.push({ ts: now, actor: "SYSTEM (Spark)", action: "CASE_CREATED", case_id: c.case_id, wallet: c.violator_address, note: `${c.rule_id} ${L_RULES[c.rule_id].name} · ${c.risk_level}` });
    if (c.status === "AUTO_FROZEN") db.audit.push({ ts: now + 1, actor: "SYSTEM (Spark)", action: "AUTO_FREEZE", case_id: c.case_id, wallet: c.violator_address, note: `FROZEN_${c.rule_id} → 게이트웨이 동기화` });
    caseListeners.forEach((fn) => fn(view(c)));
  }
  // 표본 거래는 최근 400건만 유지
  const samples = db.ledger.filter((x) => x.sample);
  if (samples.length > 400) {
    const drop = new Set(samples.slice(0, samples.length - 400).map((x) => x.tx_hash));
    db.ledger = db.ledger.filter((x) => !drop.has(x.tx_hash));
  }
  emit();
}
let timer = null;
export function startLive() { if (!timer) timer = setInterval(tick, 2000); }
export function stopLive() { clearInterval(timer); timer = null; }
export const setPaused = (v) => { paused = v; emit(); };
export const isPaused = () => paused;

/* ── 고객 앱(Mock 엔진) 데이터 합치기 ── */
const appWallet = () => lc(ls.get("wallet_v2")?.address);
Engine.subscribe(() => emit());
window.addEventListener("storage", (e) => { if (e.key?.startsWith("aegis:")) emit(); }); // 다른 탭의 고객 앱

function engineState() {
  // 다른 탭에서 바뀐 엔진 상태도 읽기 위해 저장본을 직접 읽음
  const s = ls.get("fds_v2", null);
  return s || { ledger: Engine.ledger(), holdLog: Engine.holdLog(), blockLog: Engine.blockLog(), cases: Engine.snapshot().cases, frozen: {} };
}

function appData() {
  const s = engineState();
  const me = appWallet();
  const origin = (a) => (a === me ? "APP" : T.BLACKLIST.map(lc).includes(a) ? "SDN" : "MAINNET");
  const ledger = (s.ledger || []).map((t) => ({ ...t, status: "ACCEPTED", http: 200, app: true }));
  // 거절된 요청은 엔진 원장에 없으므로 고객 앱 송금 내역(요청 원본)에서 가져옴 (= transfer.rejected.v1)
  const accepted = new Set(ledger.map((t) => t.tx_hash));
  for (const h of ls.get("wallet_v2")?.history || []) {
    if (h.state === "OK" || accepted.has(h.tx_hash)) continue;
    ledger.push({ tx_hash: h.tx_hash, from: me, to: lc(h.to), amount: h.amount, ts: h.ts, status: "REJECTED", http: h.http, reason: h.reason, app: true });
  }
  const holds = (s.cases || []).filter((c) => c.action === "TEMP_HOLD").map((c) => ({
    id: c.case_id, wallet: c.violator_address, rule: c.rule_id, ts: Date.parse(c.detected_at), until: c.hold_until,
    tx_hash: c.tx_hashes?.[c.tx_hashes.length - 1], origin: origin(c.violator_address), app: true, attempts: c.attempts,
  }));
  const blocks = (s.cases || []).filter((c) => c.rule_id === "R07").map((c) => ({
    id: c.case_id, wallet: c.violator_address, counterparty: c.evidence?.blacklisted_wallet, reason: c.reason_code, ts: Date.parse(c.detected_at), app: true, attempts: c.attempts,
  }));
  const cases = (s.cases || []).filter((c) => c.stage === 2).map((c) => fromEngineCase(c, origin, s));
  return { ledger, holds, blocks, cases, origin };
}

/** 엔진 사건 → 관제 웹 사건 형식 */
function fromEngineCase(c, origin, s) {
  const e = c.evidence || {};
  const t = Date.parse(c.detected_at);
  const items = {
    L1: [{ label: "1단계 보류 횟수 (새 보류만)", value: e.hold_count, threshold: T.L1_MIN_HOLDS, unit: "회", op: ">=" }],
    L2: [{ label: "누적 송금액", value: e.day_total, threshold: T.L2_DAY_SUM, unit: "USDT", op: ">=" }, { label: `가장 큰 1건 (${T.R05_WHALE_USDT.toLocaleString()} USDT 미만이어야 해당)`, value: e.max_single, threshold: T.R05_WHALE_USDT, unit: "USDT", op: "<" }],
    L3: [{ label: "900~990 USDT 송금 횟수", value: e.band_tx_count, threshold: T.L3_MIN_BAND, unit: "회", op: ">=" }],
    L4: [{ label: "받고 바로 다시 보낸 횟수", value: e.pass_through_count, threshold: T.L4_MIN, unit: "회", op: ">=" }],
    L5: [
      { label: "제재 지갑에게서 받은 금액 (CRITICAL)", value: e.sdn_received || 0, threshold: 0, unit: "USDT", op: ">" },
      { label: "제재 지갑에게 보낸 금액 (HIGH)", value: e.sdn_sent || 0, threshold: 0, unit: "USDT", op: ">" },
      { label: "동결 지갑과 누적 거래 (MEDIUM)", value: e.frozen_total || 0, threshold: T.L5_FROZEN_SUM, unit: "USDT", op: ">=" },
    ],
    L6: [{ label: "오늘 송금 수", value: e.today_count, threshold: T.L6_MIN, unit: "건", op: ">=" }],
    L7: [{ label: "처음 보내는 상대 수", value: e.new_counterparties, threshold: T.L7_MIN_NEW, unit: "곳", op: ">=" }],
    L8: [{ label: "돌아온 비율", value: Math.round((e.return_ratio || 0) * 1000) / 10, threshold: 80, unit: "%", op: ">=" }],
  }[c.rule_id] || [];
  const headline = {
    L1: `고객 앱 지갑이 1단계 보류에 ${e.hold_count}번 걸렸습니다.`,
    L5: L5_TYPES[c.risk_level] ? `${L5_TYPES[c.risk_level]}.` : "위험 지갑과 거래했습니다.",
    L8: `돈이 ${(e.cycle_path || []).length - 1}개 지갑을 돌아 ${Math.round((e.return_ratio || 0) * 100)}% 돌아왔습니다.`,
  }[c.rule_id] || `${L_RULES[c.rule_id].name} 기준을 넘었습니다.`;
  const related = (s.ledger || []).filter((x) => (c.tx_hashes || []).includes(x.tx_hash));
  return {
    case_id: c.case_id, rule_id: c.rule_id, risk_level: c.risk_level || L_RULES[c.rule_id].grade,
    violator_address: c.violator_address, origin: origin(c.violator_address), detected_at: t,
    status: c.released_at ? "RELEASED" : c.review_status || (c.action === "FREEZE" ? "AUTO_FROZEN" : "PENDING"),
    tx_hashes: c.tx_hashes || [], counterpart: "고객 앱 거래", amount: related.reduce((a, x) => a + x.amount, 0),
    headline, evidence: items, cycle: c.rule_id === "L8" ? (e.cycle_path || []).slice(0, -1) : undefined, app: true, raw: c,
  };
}

/* ── 합쳐진 데이터 ── */
function merged() {
  if (cache) return cache;
  const a = appData();
  return cache = {
    app: a,
    ledger: a.ledger.length ? db.ledger.concat(a.ledger) : db.ledger,
    holds: db.holds.concat(a.holds),
    blocks: db.blocks.concat(a.blocks),
    cases: db.cases.concat(a.cases),
    origin: (addr) => db.wallets[addr]?.origin || a.origin(addr),
  };
}

const view = (c) => {
  const d = decisions[c.case_id];
  return d ? { ...c, status: d.status, decided: d } : c;
};
export const walletInfo = (addr) => {
  const w = db.wallets[addr];
  if (w) return w;
  const me = appWallet();
  return { address: addr, origin: addr === me ? "APP" : T.BLACKLIST.map(lc).includes(addr) ? "SDN" : "MAINNET", firstSeen: null };
};
const isNew = (addr, now) => {
  const f = db.wallets[addr]?.firstSeen;
  return f != null && now - f < 864e5;
};

/* ── 조회 ── */
export function listCases() {
  const now = Date.now();
  return merged().cases.map(view).map((c) => ({ ...c, isNew: isNew(c.violator_address, now) }))
    .sort((a, b) => b.detected_at - a.detected_at);
}

export function getCase(id) {
  const m = merged();
  const raw = m.cases.find((c) => c.case_id === id);
  if (!raw) return null;
  const c = view(raw);
  const w = c.violator_address;
  const since = c.detected_at - P.LONG;
  const mine = m.ledger.filter((t) => (t.from === w || t.to === w) && t.status === "ACCEPTED" && t.ts >= since);
  const received = mine.filter((t) => t.to === w).reduce((s, t) => s + t.amount, 0);
  const sent = mine.filter((t) => t.from === w).reduce((s, t) => s + t.amount, 0);
  const cps = {};
  for (const t of mine) {
    const o = t.from === w ? t.to : t.from;
    cps[o] = cps[o] || { address: o, in: 0, out: 0, n: 0, origin: m.origin(o), label: db.wallets[o]?.label };
    if (t.to === w) cps[o].in += t.amount; else cps[o].out += t.amount;
    cps[o].n++;
  }
  const holds = m.holds.filter((h) => h.wallet === w);
  const related = m.ledger.filter((t) => c.tx_hashes.includes(t.tx_hash)).sort((a, b) => b.ts - a.ts);
  const others = listCases().filter((x) => x.violator_address === w && x.case_id !== id);
  return {
    ...c,
    related,
    flows: mine.map((t) => ({ ts: t.ts, in: t.to === w ? t.amount : 0, out: t.from === w ? t.amount : 0 })),
    summary: { received, sent, counterparties: Object.keys(cps).length, holds: holds.map((h) => h.rule), firstSeen: walletInfo(w).firstSeen },
    counterparties: Object.values(cps).sort((a, b) => b.in + b.out - (a.in + a.out)).slice(0, 5),
    otherCases: others,
    audit: getAudit().filter((x) => x.case_id === id),
    sdn: db.sdn,
  };
}

/** 기간별 통계 (대시보드) — range: "1h" | "24h" | "14d" */
export function getStats(range = "24h") {
  const now = Date.now();
  const span = { "1h": 60 * MIN, "24h": 24 * 60 * MIN, "14d": 14 * 864e5 }[range];
  const since = now - span;
  const m = merged();
  const cases = listCases();
  const inRange = (x) => (x.ts ?? x.detected_at) >= since;
  const holds = m.holds.filter(inRange);
  const blocks = m.blocks.filter(inRange);
  const tries = db.frozenTries.filter(inRange);
  const rc = cases.filter((c) => c.detected_at >= since);
  const older = range === "14d" ? db.daily : [];
  const sumOld = (k) => older.reduce((s, d) => s + d[k], 0);

  const byStatus = (st) => rc.filter((c) => c.status === st).length;
  const ruleCounts = {};
  for (const h of holds) ruleCounts[h.rule] = (ruleCounts[h.rule] || 0) + 1;
  ruleCounts.R07 = blocks.length;
  for (const c of rc) ruleCounts[c.rule_id] = (ruleCounts[c.rule_id] || 0) + 1;

  // 거래 흐름 그래프: 1h=1분, 24h=30분, 14d=하루 단위
  const bucket = { "1h": MIN, "24h": 30 * MIN, "14d": 864e5 }[range];
  const series = [];
  if (range === "14d") {
    for (const d of db.daily) series.push({ t: d.day, inflow: d.inflow, hold: d.holds, block: d.blocks, freeze: d.auto });
  }
  const start = range === "14d" ? now - 864e5 : since;
  for (let t = Math.floor(start / bucket) * bucket; t <= now; t += bucket) {
    const end = t + bucket;
    const inf = db.inflow.filter((x) => x.t >= t && x.t < end).reduce((s, x) => s + x.n, 0);
    series.push({
      t, inflow: inf,
      hold: m.holds.filter((x) => x.ts >= t && x.ts < end).length,
      block: m.blocks.filter((x) => x.ts >= t && x.ts < end).length,
      freeze: cases.filter((c) => c.detected_at >= t && c.detected_at < end && c.status === "AUTO_FROZEN").length,
    });
  }
  const lastMin = db.inflow[db.inflow.length - 2]?.n ?? 0;

  // 위험 지갑 상위: 미처리 → 등급 → 금액 순
  const order = { CRITICAL: 0, HIGH: 1, MEDIUM: 2 };
  const top = [...rc].sort((a, b) => (a.status === "PENDING" ? 0 : 1) - (b.status === "PENDING" ? 0 : 1) || order[a.risk_level] - order[b.risk_level] || b.amount - a.amount).slice(0, 6)
    .map((c) => ({ ...c, rules: [...new Set([c.rule_id, ...m.holds.filter((h) => h.wallet === c.violator_address).map((h) => h.rule)])], spark: activity(m, c.violator_address, now) }));

  return {
    range,
    kpi: {
      pending: cases.filter((c) => c.status === "PENDING").length, // 검토 대기는 기간과 무관하게 "지금" 남은 수
      pendingCritical: cases.filter((c) => c.status === "PENDING" && c.risk_level === "CRITICAL").length,
      autoFrozen: byStatus("AUTO_FROZEN") + sumOld("auto"),
      staffFrozen: byStatus("FROZEN"),
      dismissed: byStatus("DISMISSED") + sumOld("dismissed"),
      holds: holds.length + sumOld("holds"),
      blocks: blocks.length + sumOld("blocks"),
      frozenTries: tries.length,
      inflowPerMin: lastMin,
      inflowTotal: series.reduce((s, x) => s + x.inflow, 0),
    },
    grades: ["CRITICAL", "HIGH", "MEDIUM"].map((gr) => ({ grade: gr, n: cases.filter((c) => c.status === "PENDING" && c.risk_level === gr).length })),
    series, ruleCounts, top,
    oldNote: range === "14d" ? "13일 전까지는 일별 요약, 최근 24시간은 실제 기록" : null,
    sparks: kpiSparks(m, cases, now),
  };
}

function activity(m, w, now) {
  const out = [];
  for (let i = 11; i >= 0; i--) {
    const a = now - (i + 1) * 2 * 60 * MIN, b = now - i * 2 * 60 * MIN;
    out.push({ v: m.ledger.filter((t) => (t.from === w || t.to === w) && t.ts >= a && t.ts < b).length });
  }
  return out;
}
function kpiSparks(m, cases, now) {
  const pts = (fn) => Array.from({ length: 12 }, (_, i) => { const a = now - (12 - i) * 2 * 60 * MIN, b = a + 2 * 60 * MIN; return { v: fn(a, b) }; });
  const cnt = (arr, key = "ts") => (a, b) => arr.filter((x) => x[key] >= a && x[key] < b).length;
  return {
    pending: pts(cnt(cases.filter((c) => c.status === "PENDING"), "detected_at")),
    auto: pts(cnt(cases.filter((c) => c.status === "AUTO_FROZEN"), "detected_at")),
    holds: pts(cnt(m.holds)),
    blocks: pts(cnt(m.blocks)),
    dismissed: pts(cnt(cases.filter((c) => c.status === "DISMISSED"), "detected_at")),
  };
}

/** 검증 성적표 — 봇 정답 라벨(fds.ground_truth.v1) 연결 전이라 예시 값 */
export const SCORECARD = [
  { rule: "R02", name: "쪼개기", recall: 0.94, fpr: 0.03 },
  { rule: "L3", name: "장기 쪼개기", recall: 0.88, fpr: 0.02 },
  { rule: "L8", name: "순환 거래", recall: 0.91, fpr: 0.01 },
  { rule: "L7", name: "새 상대 집중", recall: 0.76, fpr: 0.12 },
  { rule: "R07", name: "블랙리스트", recall: 1.0, fpr: 0.0 },
];

/** 실시간 거래: 최근 들어온 거래 (표본) + 보류·차단 결과 */
export function listTransfers(limit = 300) {
  const m = merged();
  const holdByTx = new Map(m.holds.filter((h) => h.tx_hash).map((h) => [h.tx_hash, h]));
  return m.ledger.slice().sort((a, b) => b.ts - a.ts).slice(0, limit).map((t) => {
    const h = holdByTx.get(t.tx_hash);
    const result = t.status === "ACCEPTED" ? (h ? "PASS_HOLD_OTHER" : "PASS") : t.http === 429 ? "HOLD" : t.reason?.startsWith("FROZEN_") ? "FROZEN" : "BLOCK";
    return { ...t, result, rule: h?.rule || (t.reason?.startsWith("BLACKLISTED") ? "R07" : t.reason?.match(/R0\d|L\d/)?.[0] ?? null), fromOrigin: m.origin(t.from), toOrigin: m.origin(t.to), source: t.app ? "앱" : ORIGINS[m.origin(t.from)]?.source || "온체인" };
  });
}

/** 감사 기록 (policy_decisions) — 시간순 해시 체인으로 "수정되지 않았음"을 확인 */
export function getAudit() {
  const appCases = merged().app.cases;
  const appAudit = appCases.flatMap((c) => [
    { ts: c.detected_at, actor: "SYSTEM (Spark)", action: "CASE_CREATED", case_id: c.case_id, wallet: c.violator_address, note: `${c.rule_id} ${L_RULES[c.rule_id].name} · 고객 앱` },
    ...(c.raw.review_status === "AUTO_FROZEN" ? [{ ts: c.detected_at + 1, actor: "SYSTEM (Spark)", action: "AUTO_FREEZE", case_id: c.case_id, wallet: c.violator_address, note: `FROZEN_${c.rule_id}` }] : []),
  ]);
  const all = db.audit.concat(appAudit, myAudit).sort((a, b) => a.ts - b.ts);
  let prev = "00000000";
  const chained = all.map((r, i) => {
    const h = hash(prev + JSON.stringify([r.ts, r.actor, r.action, r.case_id, r.note]));
    const row = { ...r, seq: i + 1, prev, hash: h };
    prev = h;
    return row;
  });
  return chained.reverse();
}

/** 백엔드 시스템 상태 (Mock) — 연결 후 GET /api/v1/admin/health */
export function getHealth() {
  const now = Date.now();
  const j = (a, b) => Math.round(g.rand(a, b));
  const lastSample = db.ledger.filter((x) => x.sample).at(-1)?.ts ?? now;
  return {
    mock: !CONFIG.API_BASE,
    checkedAt: now,
    services: [
      { id: "collector", name: "수집기 (Alchemy WSS)", role: "메인넷 USDT 거래 수신", status: "UP", metric: `${(db.inflow.at(-2)?.n ?? 0).toLocaleString()}건/분`, latency: j(80, 160), last: lastSample },
      { id: "gateway", name: "게이트웨이 (FastAPI)", role: "R01~R07 입구 검사", status: "UP", metric: `p95 ${g.rand(3.2, 5.8).toFixed(1)} ms`, latency: j(3, 6), last: now - j(0, 2000) },
      { id: "redis", name: "Redis", role: "5분 카운터 · 보류 · 동결 목록", status: "UP", metric: `키 ${j(41000, 46000).toLocaleString()}개`, latency: 1, last: now - j(0, 1000) },
      { id: "kafka", name: "Kafka", role: "거래 · 보류 · 동결 이벤트 전달", status: "UP", metric: `지연(lag) ${j(0, 40)}건`, latency: j(4, 12), last: now - j(0, 1500) },
      { id: "spark", name: "Spark (L1~L8)", role: "2주 심층 분석", status: paused ? "WARN" : "UP", metric: `배치 ${j(6, 11)}초 전 완료`, latency: j(900, 2400), last: now - j(6000, 11000) },
      { id: "mongo", name: "MongoDB", role: "원장 2주 · 사건 · 감사 기록", status: "UP", metric: `디스크 ${j(38, 44)}%`, latency: j(2, 8), last: now - j(0, 2000) },
      { id: "clickhouse", name: "ClickHouse", role: "장기 보관 · 성적표", status: "WARN", metric: "연결 준비 중 (고도화)", latency: null, last: null },
      { id: "api", name: "관제 API · WebSocket", role: "이 화면에 데이터 제공", status: CONFIG.API_BASE ? "UP" : "MOCK", metric: CONFIG.API_BASE ? "연결됨" : "브라우저 Mock 사용 중", latency: null, last: now },
      { id: "discord", name: "Discord 웹훅", role: "사건 알림 방송", status: "WARN", metric: "미설정", latency: null, last: null },
    ],
    topics: [
      { topic: "transfer.events.v1", desc: "통과 거래", lag: j(0, 30), rate: j(900, 1050) },
      { topic: "transfer.rejected.v1", desc: "거절 거래", lag: j(0, 3), rate: j(5, 15) },
      { topic: "fds.holds.v1", desc: "1단계 보류 → Spark", lag: 0, rate: j(0, 3) },
      { topic: "fds.alerts.v1", desc: "2단계 탐지 사건", lag: 0, rate: j(0, 2) },
      { topic: "fds.freezes.v1", desc: "동결 → 게이트웨이", lag: 0, rate: j(0, 1) },
      { topic: "fds.ground_truth.v1", desc: "봇 정답 라벨 (격리)", lag: 0, rate: j(20, 60) },
    ],
  };
}

/** 자금 흐름 그래프 데이터 */
export function getGraph(caseId, { depth = 1, minAmount = 0, from = null, to = null, expand = [] } = {}) {
  const c = getCase(caseId);
  if (!c) return null;
  const m = merged();
  const center = c.violator_address;
  const winFrom = from ?? c.detected_at - P.LONG - 30 * MIN;
  const winTo = to ?? Date.now();
  const okTx = (t) => t.status === "ACCEPTED" && t.ts >= winFrom && t.ts <= winTo && (t.amount >= minAmount || c.tx_hashes.includes(t.tx_hash));
  const nodes = new Map([[center, 0]]);
  const edges = new Map();
  let frontier = [center, ...expand];
  expand.forEach((a) => nodes.set(a, nodes.get(a) ?? 1));
  for (let d = 0; d < depth + (expand.length ? 1 : 0); d++) {
    const next = [];
    for (const a of frontier) {
      for (const t of m.ledger) {
        if (!okTx(t) || (t.from !== a && t.to !== a)) continue;
        const o = t.from === a ? t.to : t.from;
        const key = `${t.from}>${t.to}`;
        const e = edges.get(key) || { from: t.from, to: t.to, amount: 0, n: 0, txs: [], hit: false, ts: t.ts };
        e.amount += t.amount; e.n++; e.txs.push(t.tx_hash); e.ts = Math.min(e.ts, t.ts);
        if (c.tx_hashes.includes(t.tx_hash)) e.hit = true;
        edges.set(key, e);
        if (!nodes.has(o)) { nodes.set(o, nodes.get(a) + 1); next.push(o); }
      }
    }
    frontier = next;
    if (nodes.size > 30) break;
  }
  // 화면에 있는 지갑끼리의 거래도 선으로 (예: 순환 거래의 B → C)
  const shown = new Set([...nodes.keys()].slice(0, 30));
  for (const t of m.ledger) {
    if (!okTx(t) || !shown.has(t.from) || !shown.has(t.to)) continue;
    const key = `${t.from}>${t.to}`;
    const e = edges.get(key) || { from: t.from, to: t.to, amount: 0, n: 0, txs: [], hit: false, ts: t.ts };
    if (e.txs.includes(t.tx_hash)) continue;
    e.amount += t.amount; e.n++; e.txs.push(t.tx_hash); e.ts = Math.min(e.ts, t.ts);
    if (c.tx_hashes.includes(t.tx_hash)) e.hit = true;
    edges.set(key, e);
  }
  const hitTimes = m.ledger.filter((t) => c.tx_hashes.includes(t.tx_hash)).map((t) => t.ts);
  const frozenSet = new Set(listCases().filter((x) => ["AUTO_FROZEN", "FROZEN"].includes(x.status)).map((x) => x.violator_address));
  return {
    case: c,
    nodes: [...nodes.entries()].slice(0, 30).map(([address, level]) => ({
      address, level, origin: m.origin(address), label: db.wallets[address]?.label,
      frozen: frozenSet.has(address), sdn: m.origin(address) === "SDN", center: address === center,
      onPath: [...edges.values()].some((e) => e.hit && (e.from === address || e.to === address)),
    })),
    edges: [...edges.values()].filter((e) => nodes.has(e.from) && nodes.has(e.to)),
    window: { from: winFrom, to: winTo }, hitTimes,
  };
}

/** 검색: 지갑 주소 · 사건 ID · tx_hash */
export function search(q) {
  const s = lc(q).trim();
  if (s.length < 3) return [];
  const out = [];
  for (const c of listCases()) {
    if (c.case_id.toLowerCase().includes(s)) out.push({ kind: "사건", id: c.case_id, text: `${c.case_id} · ${c.rule_id} ${L_RULES[c.rule_id].name}`, to: `/cases/${c.case_id}` });
    else if (c.violator_address.includes(s)) out.push({ kind: "지갑", id: c.case_id, text: `${c.violator_address} · ${c.rule_id} 사건`, to: `/cases/${c.case_id}` });
    if (out.length > 7) break;
  }
  if (s.startsWith("0x") && s.length > 20) {
    const m = merged();
    const t = m.ledger.find((x) => x.tx_hash.startsWith(s));
    if (t) {
      const c = listCases().find((x) => x.tx_hashes.includes(t.tx_hash));
      out.push({ kind: "거래", id: t.tx_hash, text: `${t.tx_hash.slice(0, 18)}… · ${t.amount.toLocaleString()} USDT${c ? ` · ${c.case_id}` : ""}`, to: c ? `/cases/${c.case_id}` : `/transfers?q=${t.tx_hash}` });
    }
  }
  return out.slice(0, 8);
}

/* ── 조치: 동결 · 오탐 처리 · 해제 · 열람 · STR 초안 ── */
export function decide(caseId, action, { actor, note }) {
  const c = merged().cases.find((x) => x.case_id === caseId);
  if (!c) throw new Error("사건을 찾을 수 없습니다");
  const now = Date.now();
  const next = { FREEZE: "FROZEN", DISMISS: "DISMISSED", RELEASE: "RELEASED" }[action];
  if (next) {
    decisions[caseId] = { status: next, ts: now, actor, note };
    // 고객 앱 지갑이면 실제 엔진(게이트웨이 흉내)에도 반영 → 고객 앱 송금이 403 / 정상으로 바뀜
    if (c.app || c.origin === "APP") {
      Engine.reload();
      if (action === "FREEZE") Engine.manualFreeze(c.violator_address, c.rule_id);
      if (action === "RELEASE") Engine.unfreeze(c.violator_address);
    }
  }
  myAudit.push({ ts: now, actor, action, case_id: caseId, wallet: c.violator_address, note });
  saveDecisions();
  emit();
  return { ok: true, status: next };
}
export function logView(caseId, actor) {
  const recent = myAudit.find((a) => a.case_id === caseId && a.action === "VIEWED" && Date.now() - a.ts < 10 * MIN);
  if (recent) return;
  const c = merged().cases.find((x) => x.case_id === caseId);
  if (!c) return;
  myAudit.push({ ts: Date.now(), actor, action: "VIEWED", case_id: caseId, wallet: c.violator_address, note: "사건 열람" });
  saveDecisions();
  emit();
}

export const getSdn = () => db.sdn;
