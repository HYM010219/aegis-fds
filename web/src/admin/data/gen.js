/* ==========================================================
   관제 웹 Mock 데이터 생성기 (백엔드 연결 전 시연용)
   - 기획서 v8.0 규칙에 맞는 "말이 되는" 데이터를 만든다
     · 시뮬레이터 지갑은 R01~R06 면제 → 1단계 보류는 메인넷·고객 앱 지갑에만
     · 사건마다 실제 거래(원장)를 먼저 만들고, 근거 수치는 그 거래에서 계산
     · demo 기간(20분 / 5분 / 1분) 안에 패턴이 일어나도록 시각 배치
   - 같은 시드면 같은 데이터 (새로 고침해도 사건 ID 유지)
   ========================================================== */
import { CONFIG } from "../../config.js";
import { P, L_RULES } from "./meta.js";

const T = CONFIG.FDS;
const MIN = 60_000;

export function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** 생성기 인스턴스 — 초기 데이터와 실시간 추가 데이터를 같은 규칙으로 만든다 */
export function createGenerator(db, seed = 20261116) {
  const R = rng(seed);
  const rand = (a, b) => a + R() * (b - a);
  const int = (a, b) => Math.floor(rand(a, b + 1));
  const pick = (arr) => arr[Math.floor(R() * arr.length)];
  const hex = (n) => Array.from({ length: n }, () => "0123456789abcdef"[Math.floor(R() * 16)]).join("");
  const money = (x) => Math.round(x * 100) / 100;
  let caseSeq = db.cases.length;

  const wallet = (origin, firstSeen) => {
    const address = "0x" + hex(40);
    db.wallets[address] = { address, origin, firstSeen: firstSeen ?? db.now - int(3, 400) * 864e5 / 24 };
    return address;
  };
  const exchange = () => pick(db.exchanges);

  const tx = (from, to, amount, ts, extra = {}) => {
    const t = { tx_hash: "0x" + hex(64), from, to, amount: money(amount), ts: Math.round(ts), status: "ACCEPTED", http: 200, reason: null, ...extra };
    db.ledger.push(t);
    return t;
  };
  const rejected = (from, to, amount, ts, http, reason) => tx(from, to, amount, ts, { status: "REJECTED", http, reason });
  const hold = (w, rule, ts, t) => {
    const h = { id: "hold_" + hex(10), wallet: w, rule, ts: Math.round(ts), until: Math.round(ts) + T.HOLD_MS, tx_hash: t?.tx_hash, origin: db.wallets[w]?.origin || "MAINNET" };
    db.holds.push(h);
    return h;
  };

  /** 평범한 과거 거래 몇 건 (지갑 요약 · 자금 흐름 그래프를 자연스럽게) */
  const background = (w, t0, n = int(1, 3)) => {
    for (let i = 0; i < n; i++) {
      const other = R() < 0.6 ? exchange() : wallet(R() < 0.5 ? "SIM" : "MAINNET");
      const ts = t0 - rand(0.3, 0.95) * P.LONG;
      if (R() < 0.7) tx(other, w, rand(800, 30000), ts); else tx(w, other, rand(200, 6000), ts);
      if (R() < 0.5) tx(other, wallet("MAINNET"), rand(50, 900), ts - rand(1, 5) * MIN); // 2단계 펼치기용
    }
  };

  const newCase = (rule, w, t, extra) => ({
    case_id: `case_${new Date(t).toISOString().slice(0, 10).replace(/-/g, "")}_${String(++caseSeq).padStart(4, "0")}`,
    rule_id: rule,
    risk_level: extra.risk_level || L_RULES[rule].grade,
    violator_address: w,
    origin: db.wallets[w].origin,
    detected_at: Math.round(t),
    status: L_RULES[rule].action === "AUTO" ? "AUTO_FROZEN" : "PENDING",
    period_profile: T.PERIOD_PROFILE,
    ...extra,
  });

  /* ── 룰별 사건 만들기: 거래를 먼저 만들고 근거는 그 거래에서 계산 ── */
  const builders = {
    L1(t) {
      const w = wallet(R() < 0.75 ? "MAINNET" : "APP");
      background(w, t);
      const rules = [pick(["R01", "R02", "R04"]), pick(["R01", "R02", "R04", "R05"]), pick(["R01", "R02"])];
      const at = [t - rand(15, 18) * MIN, t - rand(8, 11) * MIN, t - rand(0.4, 1.5) * MIN];
      const hashes = [];
      rules.forEach((rule, i) => {
        const amt = rule === "R02" ? rand(900, 990) : rule === "R05" ? rand(50000, 62000) : rand(300, 2500);
        const r = rejected(w, wallet("MAINNET"), amt, at[i], 429, `TEMP_HOLD_${rule}`);
        hold(w, rule, at[i], r);
        hashes.push(r.tx_hash);
      });
      return newCase("L1", w, t, {
        tx_hashes: hashes,
        counterpart: `${rules[0]} 등 보류 3회`,
        amount: db.ledger.filter((x) => hashes.includes(x.tx_hash)).reduce((s, x) => s + x.amount, 0),
        headline: `${P === T.PERIODS.demo ? "20분" : "2주"} 안에 1단계 보류가 3번 걸렸습니다 (${rules.join(" → ")}).`,
        evidence: [{ label: "1단계 보류 횟수 (새 보류만)", value: 3, threshold: T.L1_MIN_HOLDS, unit: "회", op: ">=" }],
      });
    },
    L2(t) {
      const w = wallet(R() < 0.6 ? "MAINNET" : "SIM");
      background(w, t);
      const amts = [rand(18000, 21000), rand(17000, 20000), rand(12000, 16000)];
      const tos = [wallet("MAINNET"), wallet("MAINNET"), wallet("SIM")];
      const ts = [t - 4.2 * MIN, t - 2.6 * MIN, t - 0.3 * MIN];
      const txs = amts.map((a, i) => tx(w, tos[i], a, ts[i]));
      const sum = txs.reduce((s, x) => s + x.amount, 0);
      return newCase("L2", w, t, {
        tx_hashes: txs.map((x) => x.tx_hash), counterpart: "3곳", amount: sum,
        headline: `5만 미만으로 나눠 ${txs.length}번 보내 누적 ${Math.round(sum).toLocaleString()} USDT가 되었습니다.`,
        evidence: [
          { label: `${P === T.PERIODS.demo ? "5분" : "24시간"} 누적 송금액`, value: sum, threshold: T.L2_DAY_SUM, unit: "USDT", op: ">=" },
          { label: "가장 큰 1건 (5만 미만이어야 해당)", value: Math.max(...amts), threshold: T.R05_WHALE_USDT, unit: "USDT", op: "<" },
        ],
      });
    },
    L3(t) {
      const w = wallet("SIM"); // 5분 안 3회 이상이면 R02에 먼저 걸리므로, 면제 지갑만 가능 (demo 기준)
      background(w, t);
      const n = int(10, 13);
      const tos = [wallet("MAINNET"), wallet("MAINNET")];
      const txs = Array.from({ length: n }, (_, i) => tx(w, pick(tos), rand(905, 989), t - (18 - (17.5 * i) / (n - 1)) * MIN));
      return newCase("L3", w, t, {
        tx_hashes: txs.map((x) => x.tx_hash), counterpart: "2곳", amount: txs.reduce((s, x) => s + x.amount, 0),
        headline: `900~990 USDT 송금을 ${n}번 반복했습니다.`,
        evidence: [{ label: "900~990 USDT 송금 횟수", value: n, threshold: T.L3_MIN_BAND, unit: "회", op: ">=" }],
      });
    },
    L4(t) {
      const w = wallet(R() < 0.5 ? "MAINNET" : "SIM");
      background(w, t, 1);
      const hashes = [];
      let total = 0;
      for (let i = 0; i < 3; i++) {
        const base = t - (16 - i * 7) * MIN;
        const inAmt = rand(1000, 3200);
        const a = tx(wallet("MAINNET"), w, inAmt, base);
        const b = tx(w, wallet("MAINNET"), inAmt * rand(0.85, 0.95), base + rand(18, 50) * 1000);
        hashes.push(a.tx_hash, b.tx_hash);
        total += a.amount;
      }
      return newCase("L4", w, t, {
        tx_hashes: hashes, counterpart: "받음 3 · 보냄 3", amount: total,
        headline: `1,000 USDT 이상 받은 돈을 1분 안에 85~95% 다시 보내기를 3번 했습니다.`,
        evidence: [
          { label: "받고 바로 다시 보낸 횟수", value: 3, threshold: T.L4_MIN, unit: "회", op: ">=" },
          { label: "다시 보낸 비율 (최소)", value: 85, threshold: 80, unit: "%", op: ">=" },
        ],
      });
    },
    /** L5 CRITICAL/HIGH — 거래 당시에는 제재 목록에 없었고, 이후 목록 갱신으로 걸린 경우 (게이트웨이를 통과한 이유) */
    L5(t, type = "MEDIUM") {
      const w = wallet(type === "MEDIUM" ? pick(["MAINNET", "SIM"]) : "MAINNET", type === "CRITICAL" ? t - 2 * 864e5 : undefined);
      background(w, t);
      if (type === "CRITICAL" || type === "HIGH") {
        const sdn = type === "CRITICAL" ? db.sdn[0] : db.sdn[1];
        const amt = type === "CRITICAL" ? 12400 : rand(1500, 2400);
        const x = type === "CRITICAL" ? tx(sdn.address, w, amt, sdn.listedAt - 1.2 * MIN) : tx(w, sdn.address, amt, sdn.listedAt - 2 * MIN);
        if (type === "CRITICAL") tx(w, wallet("MAINNET"), 6500, sdn.listedAt - 0.6 * MIN); // 받은 돈 일부를 바로 이동
        return newCase("L5", w, t, {
          risk_level: type, l5_type: type, tx_hashes: [x.tx_hash],
          counterpart: type === "CRITICAL" ? "OFAC SDN에게서 받음" : "OFAC SDN에게 보냄", amount: x.amount,
          headline: type === "CRITICAL"
            ? `제재 지갑 ${sdn.address.slice(0, 8)}…에게서 ${x.amount.toLocaleString()} USDT를 받았습니다. 거래 직후 이 지갑이 제재 목록에 추가되었습니다.`
            : `제재 지갑 ${sdn.address.slice(0, 8)}…에게 ${x.amount.toLocaleString()} USDT를 보냈습니다. 거래 직후 이 지갑이 제재 목록에 추가되었습니다.`,
          evidence: [
            { label: "제재 지갑에게서 받은 금액 (CRITICAL)", value: type === "CRITICAL" ? x.amount : 0, threshold: 0, unit: "USDT", op: ">" },
            { label: "제재 지갑에게 보낸 금액 (HIGH)", value: type === "HIGH" ? x.amount : 0, threshold: 0, unit: "USDT", op: ">" },
            { label: "동결 지갑과 누적 거래 (MEDIUM)", value: 0, threshold: T.L5_FROZEN_SUM, unit: "USDT", op: ">=" },
          ],
        });
      }
      // MEDIUM: 이미 동결된 지갑으로 계속 입금 (동결 지갑도 입금은 허용)
      const frozen = db.cases.filter((c) => c.status === "AUTO_FROZEN" && c.detected_at < t - 4 * MIN);
      const target = frozen.length ? pick(frozen).violator_address : wallet("MAINNET");
      const from = db.cases.find((c) => c.violator_address === target)?.detected_at ?? t - 10 * MIN;
      const n = 3;
      const span = Math.min(t - from - MIN, P.LONG * 0.8);
      const txs = Array.from({ length: n }, (_, i) => tx(w, target, rand(3400, 4200), t - span + (span * i) / n));
      const sum = txs.reduce((s, x) => s + x.amount, 0);
      return newCase("L5", w, t, {
        risk_level: "MEDIUM", l5_type: "MEDIUM", tx_hashes: txs.map((x) => x.tx_hash), counterpart: "동결 지갑 1곳", amount: sum,
        headline: `동결된 지갑 ${target.slice(0, 8)}…에 ${n}번, 누적 ${Math.round(sum).toLocaleString()} USDT를 보냈습니다.`,
        evidence: [
          { label: "제재 지갑에게서 받은 금액 (CRITICAL)", value: 0, threshold: 0, unit: "USDT", op: ">" },
          { label: "제재 지갑에게 보낸 금액 (HIGH)", value: 0, threshold: 0, unit: "USDT", op: ">" },
          { label: "동결 지갑과 누적 거래 (MEDIUM)", value: sum, threshold: T.L5_FROZEN_SUM, unit: "USDT", op: ">=" },
        ],
      });
    },
    L6(t) {
      const w = wallet("SIM");
      const days = Math.max(1, Math.round(P.LONG / P.DAY) - 1);
      const before = days * 2; // 평소 하루 2건
      for (let i = 0; i < before; i++) tx(w, exchange(), rand(50, 400), t - P.DAY - rand(0.05, 0.95) * (P.LONG - P.DAY));
      const n = int(22, 28);
      const txs = Array.from({ length: n }, () => tx(w, R() < 0.5 ? exchange() : wallet("MAINNET"), rand(80, 700), t - rand(0.02, 0.95) * P.DAY));
      return newCase("L6", w, t, {
        tx_hashes: txs.map((x) => x.tx_hash).slice(0, 30), counterpart: `평소 2건 → 오늘 ${n}건`, amount: txs.reduce((s, x) => s + x.amount, 0),
        headline: `평소 하루 2건 정도 거래하던 지갑이 오늘 ${n}건을 보냈습니다.`,
        evidence: [
          { label: "오늘 송금 수", value: n, threshold: T.L6_MIN, unit: "건", op: ">=" },
          { label: "평소 하루 평균 대비", value: n / 2, threshold: T.L6_MULT, unit: "배", op: ">=" },
        ],
      });
    },
    L7(t) {
      const w = wallet("SIM");
      background(w, t, 1);
      const n = int(21, 31);
      const txs = Array.from({ length: n }, () => tx(w, wallet("MAINNET", t), rand(40, 160), t - rand(0.02, 0.95) * P.DAY));
      return newCase("L7", w, t, {
        tx_hashes: txs.map((x) => x.tx_hash).slice(0, 30), counterpart: `처음 본 ${n}곳`, amount: txs.reduce((s, x) => s + x.amount, 0),
        headline: `한 번도 거래한 적 없는 지갑 ${n}곳으로 짧은 시간에 송금했습니다.`,
        evidence: [{ label: "처음 보내는 상대 수", value: n, threshold: T.L7_MIN_NEW, unit: "곳", op: ">=" }],
      });
    },
    L8(t) {
      const a = wallet(R() < 0.5 ? "SIM" : "MAINNET"), b = wallet("MAINNET"), c = wallet("MAINNET");
      background(a, t);
      const first = rand(1400, 2000);
      const span = rand(2.5, 4.2) * MIN; // demo 기준 5분 안에 한 바퀴
      const e1 = tx(a, b, first, t - span - 0.3 * MIN);
      const e2 = tx(b, c, first * rand(0.92, 0.96), t - span / 2 - 0.3 * MIN);
      const e3 = tx(c, a, first * rand(0.84, 0.9), t - 0.3 * MIN);
      if (R() < 0.6) tx(b, wallet("MAINNET"), rand(200, 400), t - span - 3 * MIN);
      const ratio = e3.amount / e1.amount;
      return newCase("L8", a, t, {
        tx_hashes: [e1.tx_hash, e2.tx_hash, e3.tx_hash], counterpart: "B · C 2곳", amount: e1.amount, cycle: [a, b, c],
        headline: `A → B → C → A로 ${Math.round(span / 60000 * 10) / 10}분 만에 돈이 한 바퀴 돌아 ${Math.round(ratio * 100)}%가 돌아왔습니다.`,
        evidence: [
          { label: "돌아온 비율", value: Math.round(ratio * 1000) / 10, threshold: 80, unit: "%", op: ">=" },
          { label: "가장 작은 1건", value: Math.min(e1.amount, e2.amount, e3.amount), threshold: T.L8_MIN_USDT, unit: "USDT", op: ">=" },
          { label: "한 바퀴 걸린 시간", value: Math.round(span / 6000) / 10, threshold: P.DAY / MIN, unit: "분", op: "<=" },
        ],
      });
    },
  };

  /** 사건 하나 생성 (rule 예: "L5:CRITICAL") */
  function makeCase(spec, t) {
    const [rule, type] = spec.split(":");
    const c = builders[rule](t, type);
    db.cases.push(c);
    return c;
  }

  /* ── 1단계 보류 · 차단 · 동결 지갑 송금 시도 (메인넷 · 고객 앱 지갑만, 시뮬레이터는 면제) ── */
  function makeHold(t) {
    const rule = pick(["R01", "R01", "R01", "R02", "R02", "R03", "R04", "R04", "R05", "R06", "R06"]);
    const w = wallet(R() < 0.9 ? "MAINNET" : "APP");
    const amt = rule === "R02" ? rand(900, 990) : rule === "R05" ? rand(50000, 90000) : rand(100, 4000);
    const r = ["R03", "R06"].includes(rule)
      ? tx(wallet("MAINNET"), w, amt, t) // 거래는 통과, 상대(받은 쪽/먼저 보낸 쪽) 지갑을 보류
      : rejected(w, wallet("MAINNET"), amt, t, 429, `TEMP_HOLD_${rule}`);
    return hold(w, rule, t, r);
  }
  function makeBlock(t) {
    // 제재 목록에 오른 뒤에만 R07 차단이 생김
    let listed = db.sdn.filter((s) => s.listedAt < t);
    if (!listed.length) { listed = [db.sdn[1]]; t = db.sdn[1].listedAt + R() * (db.now - db.sdn[1].listedAt); }
    const sdn = pick(listed);
    const w = wallet("MAINNET");
    const send = R() < 0.7;
    const r = rejected(send ? w : sdn.address, send ? sdn.address : w, rand(200, 9000), t, 403, send ? "BLACKLISTED_RECEIVER" : "BLACKLISTED_SENDER");
    const b = { id: "blk_" + hex(10), wallet: send ? w : sdn.address, counterparty: send ? sdn.address : w, reason: r.reason, ts: t, tx_hash: r.tx_hash, amount: r.amount };
    db.blocks.push(b);
    return b;
  }
  function makeFrozenTry(t) {
    const frozen = db.cases.filter((c) => c.status === "AUTO_FROZEN" && c.detected_at < t);
    if (!frozen.length) return null;
    const c = pick(frozen);
    const r = rejected(c.violator_address, wallet("MAINNET"), rand(100, 5000), t, 403, `FROZEN_${c.rule_id}`);
    const f = { id: "fz_" + hex(10), wallet: c.violator_address, reason: r.reason, ts: t, tx_hash: r.tx_hash, amount: r.amount };
    db.frozenTries.push(f);
    return f;
  }

  /** 들어오는 거래 표본 (실시간 거래 화면용, 봇 2·3 트래픽) */
  function makeSimTransfer(t) {
    const from = R() < 0.5 ? exchange() : wallet("SIM");
    return tx(from, R() < 0.4 ? exchange() : wallet("SIM"), Math.exp(rand(3, 9)), t, { sample: true });
  }

  return { makeCase, makeHold, makeBlock, makeFrozenTry, makeSimTransfer, R, rand, int, pick, hex };
}

/* ── 초기 데이터 (최근 24시간 + 2주 일별 요약) ── */
export function generate(now = Date.now()) {
  const db = {
    now, wallets: {}, ledger: [], holds: [], blocks: [], frozenTries: [], cases: [], audit: [],
    inflow: [], daily: [], exchanges: [], sdn: [],
  };
  const g = createGenerator(db);
  const { R, rand, int } = g;

  for (let i = 0; i < 6; i++) {
    const a = "0x" + g.hex(40);
    db.wallets[a] = { address: a, origin: "EXCHANGE", firstSeen: now - 400 * 864e5, label: `거래소 핫월렛 ${i + 1}` };
    db.exchanges.push(a);
  }
  // 제재 지갑 = 고객 앱 엔진의 블랙리스트와 같은 주소 (R07 일관성)
  T.BLACKLIST.forEach((a, i) => {
    db.wallets[a] = { address: a, origin: "SDN", firstSeen: now - 30 * 864e5, label: `OFAC SDN ${i + 1}` };
    db.sdn.push({ address: a, listedAt: i === 0 ? now - 3 * MIN : now - 185 * MIN });
  });

  // 분당 유입 (최근 24시간, 메인넷 + 시뮬레이터) · 2주 일별 요약
  for (let m = 24 * 60; m >= 0; m--) {
    const t = now - m * MIN;
    const hour = new Date(t).getHours() + new Date(t).getMinutes() / 60;
    const day = 0.78 + 0.22 * Math.sin(((hour - 8) / 24) * 2 * Math.PI);
    db.inflow.push({ t: Math.floor(t / MIN) * MIN, n: Math.round(1000 * day * rand(0.93, 1.07)) });
  }
  for (let d = 14; d >= 1; d--) {
    db.daily.push({ day: now - d * 864e5, inflow: int(1_280_000, 1_440_000), holds: int(150, 230), blocks: int(12, 30), auto: int(10, 22), review: int(6, 14), dismissed: int(1, 5) });
  }

  // 사건: 최근 것일수록 검토 대기가 많고, 오래된 것은 처리 완료
  const plan = [
    ["L5:CRITICAL", 0.5, "PENDING"], ["L8", 1.4], ["L3", 2.6], ["L7", 4.2, "PENDING"], ["L2", 6.5],
    ["L4", 9, "FROZEN"], ["L1", 12], ["L6", 16, "DISMISSED"], ["L5:MEDIUM", 21, "PENDING"],
    ["L7", 33, "PENDING"], ["L8", 48], ["L4", 64, "PENDING"], ["L1", 80], ["L6", 97, "PENDING"],
    ["L2", 121], ["L5:HIGH", 183, "PENDING"], ["L3", 150], ["L7", 170, "DISMISSED"], ["L1", 205], ["L4", 240, "FROZEN"],
    ["L8", 290], ["L6", 330, "PENDING"], ["L3", 380], ["L1", 420, "RELEASED"], ["L5:MEDIUM", 470, "FROZEN"],
    ["L2", 540], ["L7", 600, "DISMISSED"], ["L8", 700], ["L1", 780], ["L4", 860, "PENDING"], ["L3", 950],
    ["L6", 1020, "DISMISSED"], ["L1", 1100], ["L8", 1190], ["L2", 1270], ["L5:MEDIUM", 1340, "FROZEN"], ["L3", 1400],
  ];
  // 오래된 것부터 만들어야 L5 MEDIUM 이 이미 동결된 지갑을 찾을 수 있음
  for (const [spec, ago, status] of [...plan].sort((a, b) => b[1] - a[1])) {
    const c = g.makeCase(spec, now - ago * MIN);
    if (status) c.status = status;
  }

  // 1단계 보류 (사건에 포함된 L1 보류 외) · R07 차단 · 동결 지갑 송금 시도
  for (let i = 0; i < 168; i++) g.makeHold(now - Math.pow(R(), 1.15) * 864e5);
  for (let i = 0; i < 19; i++) g.makeBlock(now - R() * 864e5);
  for (let i = 0; i < 40; i++) g.makeFrozenTry(now - Math.pow(R(), 0.8) * 864e5);
  for (let i = 0; i < 60; i++) g.makeSimTransfer(now - R() * 10 * MIN);

  // 감사 기록 (policy_decisions)
  const ANALYSTS = ["김분석 (준법감시)", "이검토 (분석관)"];
  const REASONS = {
    FROZEN: ["근거 확인 결과 자금세탁 의심 — 동결", "같은 패턴 반복, 고객 소명 불가 — 동결"],
    DISMISSED: ["거래소 정산 지갑으로 확인 — 정상", "급여 일괄 지급 건으로 소명됨 — 정상", "이벤트 에어드롭 지급 — 정상"],
    RELEASED: ["고객 소명 자료 확인 — 동결 해제"],
  };
  for (const c of db.cases) {
    db.audit.push({ ts: c.detected_at, actor: "SYSTEM (Spark)", action: "CASE_CREATED", case_id: c.case_id, wallet: c.violator_address, note: `${c.rule_id} ${L_RULES[c.rule_id].name} · ${c.risk_level}` });
    if (L_RULES[c.rule_id].action === "AUTO") db.audit.push({ ts: c.detected_at + 800, actor: "SYSTEM (Spark)", action: "AUTO_FREEZE", case_id: c.case_id, wallet: c.violator_address, note: `FROZEN_${c.rule_id} → 게이트웨이 동기화` });
    if (["FROZEN", "DISMISSED", "RELEASED"].includes(c.status)) {
      const who = g.pick(ANALYSTS);
      const t1 = Math.min(now - 20_000, c.detected_at + rand(2, 25) * MIN);
      db.audit.push({ ts: t1, actor: who, action: "VIEWED", case_id: c.case_id, wallet: c.violator_address, note: "사건 열람" });
      const act = { FROZEN: "FREEZE", DISMISSED: "DISMISS", RELEASED: "RELEASE" }[c.status];
      db.audit.push({ ts: Math.min(now - 10_000, t1 + rand(1, 8) * MIN), actor: who, action: act, case_id: c.case_id, wallet: c.violator_address, note: g.pick(REASONS[c.status]) });
    }
  }
  return { db, g };
}
