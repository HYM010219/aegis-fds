/* ==========================================================
   Api — 백엔드 연동 어댑터
   - CONFIG.API_BASE 설정 시: 실제 HTTP 호출
       POST /api/v1/transfers                     (송금 요청)
       POST /api/v1/admin/wallets/freeze|unfreeze  (지갑 동결/해제)
       GET  /api/v1/wallets/{address}             (지갑 조회)
   - 미설정 시: localStorage 기반 Mock (같은 응답 스키마)
   ========================================================== */
import { CONFIG as C } from "../config.js";
import { store, randAddr, randHex, randTxHash, hash, sleep, nowIso } from "./util.js";
import { evaluate } from "./fds.js";

export const isLive = () => !!C.API_BASE;

/* ── Mock 지갑 저장소 ── */
function loadWallet() {
  let w = store.get("wallet");
  if (!w) {
    w = { address: randAddr(), usdt: C.INITIAL_USDT, eth: C.INITIAL_ETH, nonce: 0, history: [], frozen: [], idem: {} };
    store.set("wallet", w);
  }
  return w;
}
const save = (w) => store.set("wallet", w);

export async function getWallet() {
  if (isLive()) {
    const r = await fetch(`${C.API_BASE}/api/v1/wallets/${loadWallet().address}`);
    return r.json();
  }
  return loadWallet();
}

export function resetWallet() { store.del("wallet"); return loadWallet(); }

/* ── 송금 요청 ── chainState: { block, gasGwei } (Mock에서 블록·가스 계산용) */
export async function submitTransfer(req, chainState = {}) {
  const correlation_id = "corr_" + randHex(8);
  if (isLive()) {
    const r = await fetch(`${C.API_BASE}/api/v1/transfers`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": req.idempotency_key,
        "X-Correlation-ID": correlation_id,
      },
      body: JSON.stringify(req),
    });
    return { http: r.status, body: await r.json() };
  }
  return mockTransfer(req, correlation_id, chainState);
}

async function mockTransfer(req, correlation_id, chain) {
  const w = loadWallet();
  await sleep(250 + Math.random() * 200); // 네트워크 왕복 모사

  const res = evaluate({ wallet: w, req, history: w.history, frozen: w.frozen, idemCache: w.idem });

  // R01: 최초 처리 결과를 그대로 반환
  if (res.decision === "DUPLICATE") {
    const orig = w.idem[req.idempotency_key];
    return { http: 409, body: { tx_status: "DUPLICATE", reason: res.reason, original_tx_hash: orig.tx_hash, checks: res.checks, risk_score: res.risk, alerts: [] } };
  }

  const gasPrice = chain.gasGwei || 3;
  const gasUsed = Math.round(C.ERC20_GAS_LIMIT * (0.8 + Math.random() * 0.15));
  const fee = (gasUsed * gasPrice) / 1e9;
  const tx_hash = randTxHash();
  const pre = w.usdt;

  const event = {
    event_id: "evt_" + randHex(6),
    tx_hash,
    chain_id: C.CHAIN_ID,
    token_contract: C.USDT_CONTRACT,
    idempotency_key: req.idempotency_key,
    tx_status: res.decision,
    from_wallet: w.address,
    to_wallet: req.to,
    destination_tag: req.destination_tag || null,
    exchange_vasp: req.vasp,
    amount: req.amount.toFixed(2),
    pre_tx_balance: pre.toFixed(2),
    currency: "USDT",
    is_first_counterparty: res.is_first_counterparty,
    nonce: w.nonce,
    gas_used: gasUsed,
    gas_price_gwei: +gasPrice.toFixed(3),
    block_number: res.decision === "SETTLED" ? (chain.block || 0) + 1 : null,
    device_id_hash: hash(navigator.userAgent),
    ip_prefix: "10.0.8.x",
    event_time: nowIso(),
    correlation_id,
  };

  if (res.decision === "SETTLED" || res.decision === "PENDING_REVIEW") {
    w.usdt = +(w.usdt - req.amount).toFixed(2); // HOLD는 reserved로 묶인 것으로 간주
    if (res.decision === "SETTLED") { w.eth = +(w.eth - fee).toFixed(6); w.nonce++; }
    w.history.unshift({ tx_hash, to: req.to, amount: req.amount, vasp: req.vasp, status: res.decision, ts: Date.now(), block: event.block_number, event });
    w.history = w.history.slice(0, 100);
    w.idem[req.idempotency_key] = { tx_hash, at: Date.now() };
  } else {
    w.history.unshift({ tx_hash: null, to: req.to, amount: req.amount, vasp: req.vasp, status: "REJECTED", ts: Date.now(), reason: res.reason, event });
  }
  for (const k in w.idem) if (Date.now() - w.idem[k].at > 3600_000) delete w.idem[k];

  // 위험점수 90+ 경보 → 대상(수신측) 자동 선제 동결
  res.alerts.forEach((a) => {
    const t = a.target_wallet.toLowerCase();
    if (a.action === "AUTO_FREEZE" && t !== w.address.toLowerCase() && !w.frozen.includes(t)) w.frozen.push(t);
  });
  save(w);

  return {
    http: res.http,
    body: {
      tx_status: res.decision, tx_hash: res.decision === "REJECTED" ? null : tx_hash,
      block_number: event.block_number, gas_used: gasUsed, fee_eth: fee,
      reason: res.reason, checks: res.checks, risk_score: res.risk,
      event, alerts: res.alerts,
    },
  };
}

/* ── 지갑 동결 / 해제 (관제 피드백 루프) ── */
export async function freeze(address, on = true) {
  if (isLive()) {
    const r = await fetch(`${C.API_BASE}/api/v1/admin/wallets/${on ? "freeze" : "unfreeze"}`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ wallet: address }),
    });
    return r.json();
  }
  const w = loadWallet();
  const a = address.toLowerCase();
  w.frozen = on ? [...new Set([...w.frozen, a])] : w.frozen.filter((x) => x !== a);
  save(w);
  return { ok: true, frozen: w.frozen };
}
