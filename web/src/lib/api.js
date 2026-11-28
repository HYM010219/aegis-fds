/* ==========================================================
   Api — 백엔드 연동 어댑터 (게이트웨이 규격)
   - CONFIG.API_BASE 설정 시: 실제 HTTP 호출
       POST /api/v1/transfers   요청 {tx_hash, log_index, from, to, amount, block, timestamp}
                                응답 {status, reason} + HTTP 200/403/409/422/429, 429는 Retry-After 헤더
       (아래 두 API는 아직 백엔드에 없음 — BE 담당과 협의 필요)
       GET  /api/v1/wallets/{address}
       POST /api/v1/admin/wallets/freeze|unfreeze
   - 미설정 시: 브라우저 Mock 엔진 (lib/engine.js) — 같은 응답 규격
   ========================================================== */
import { CONFIG as C } from "../config.js";
import { store, randAddr, randHex, hash, nowIso } from "./util.js";
import { classify } from "./rules.js";
import * as Engine from "./engine.js";

export const isLive = () => !!C.API_BASE;
const KEY = "wallet_v2";

/* ── Mock 지갑 (잔액은 화면 표시용 — 게이트웨이는 잔액을 관리하지 않음) ── */
function loadWallet() {
  let w = store.get(KEY);
  if (!w) {
    w = { address: randAddr(), usdt: C.INITIAL_USDT, eth: C.INITIAL_ETH, nonce: 0, history: [] };
    store.set(KEY, w);
  }
  return w;
}
const save = (w) => store.set(KEY, w);

export async function getWallet() {
  if (isLive()) {
    try {
      const r = await fetch(`${C.API_BASE}/api/v1/wallets/${loadWallet().address}`);
      if (r.ok) {
        const data = await r.json();
        if (data && Array.isArray(data.history)) return data;
      }
    } catch {}
  }
  return { ...loadWallet() };
}

export function resetWallet() {
  store.del(KEY);
  Engine.reset();
  return loadWallet();
}

/* ── 송금 요청 ──
   req: { to, amount, tx_hash, vasp, destination_tag }  chain: { block, gasGwei }
   반환: { http, state, body:{status, reason}, retry_after, checks, receiverHold, event, ... } */
export async function submitTransfer(req, chain = {}) {
  const w = loadWallet();
  const payload = {
    tx_hash: req.tx_hash,
    log_index: 0,
    from: w.address,
    to: req.to,
    amount: req.amount,
    block: (chain.block || 0) + 1,
    timestamp: nowIso(),
  };

  let http, body, retry_after = null, checks = null, receiverHold = null;
  if (isLive()) {
    const r = await fetch(`${C.API_BASE}/api/v1/transfers`, {
      method: "POST",
      // X-API-Key 는 브라우저에 두지 않는다 — 웹 전용 경로 또는 서버 쪽 프록시에서 붙일 것
      headers: { "Content-Type": "application/json", "X-Correlation-ID": "corr_" + randHex(8) },
      body: JSON.stringify(payload),
    });
    http = r.status;
    body = await r.json().catch(() => ({ status: "REJECTED", reason: null }));
    retry_after = Number(r.headers.get("Retry-After")) || null;
  } else {
    await new Promise((res) => setTimeout(res, 200 + Math.random() * 200)); // 네트워크 왕복 모사
    ({ http, body, retry_after, checks, receiverHold } = Engine.submit(payload));
  }

  const state = classify(http, body.reason);
  const gasPrice = chain.gasGwei || 3;
  const gasUsed = Math.round(C.ERC20_GAS_LIMIT * (0.8 + Math.random() * 0.15));
  const fee = (gasUsed * gasPrice) / 1e9;
  const event = {
    ...payload,
    status: body.status, reason: body.reason, http,
    exchange_vasp: req.vasp, destination_tag: req.destination_tag || null,
    device_id_hash: hash(navigator.userAgent),
    ...(retry_after ? { retry_after } : {}),
  };

  // 화면용 Mock 지갑 갱신 (실서비스에서는 GET /wallets 로 다시 조회)
  const w2 = loadWallet();
  if (state === "OK") {
    w2.usdt = +(w2.usdt - req.amount).toFixed(2);
    w2.eth = +(w2.eth - fee).toFixed(6);
    w2.nonce++;
  }
  w2.history.unshift({
    tx_hash: req.tx_hash, to: req.to, amount: req.amount, vasp: req.vasp,
    http, reason: body.reason, state, ts: Date.now(),
    block: state === "OK" ? payload.block : null, event,
  });
  w2.history = w2.history.slice(0, 100);
  save(w2);

  return {
    http, state, body, retry_after, checks, receiverHold, event,
    tx_hash: req.tx_hash, block_number: payload.block, gas_used: gasUsed, fee_eth: fee,
  };
}

/* ── 지갑 동결 / 해제 (관리자) ── */
export async function freeze(address, on = true) {
  if (isLive()) {
    const r = await fetch(`${C.API_BASE}/api/v1/admin/wallets/${on ? "freeze" : "unfreeze"}`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ wallet: address }),
    });
    return r.json();
  }
  on ? Engine.manualFreeze(address) : Engine.unfreeze(address);
  return { ok: true };
}
