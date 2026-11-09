/* ==========================================================
   Chain — 이더리움 메인넷 온체인 데이터 수집
   LIVE: 공개 JSON-RPC로 최신 블록·가스·USDT Transfer 로그 조회
   SIM : RPC 불가 시 동일 스키마의 합성 데이터 생성 (자동 폴백)
   createChain() 으로 인스턴스를 만들고 start()/stop() 으로 수명 관리
   ========================================================== */
import { CONFIG } from "../config.js";
import { randAddr, randTxHash, logNormal } from "./util.js";

export function createChain(C = CONFIG) {
  const state = { mode: "AUTO", source: "—", block: null, gasGwei: null, rpcIndex: 0 };
  const listeners = { status: new Set(), transfer: new Set() };
  let stopped = true;
  const emit = (ev, data) => { if (!stopped) listeners[ev].forEach((fn) => fn(data)); };

  let rpcId = 1;
  async function rpc(method, params = []) {
    for (let i = 0; i < C.RPC_URLS.length; i++) {
      const idx = (state.rpcIndex + i) % C.RPC_URLS.length;
      try {
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), 6000);
        const res = await fetch(C.RPC_URLS[idx], {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ jsonrpc: "2.0", id: rpcId++, method, params }),
          signal: ctrl.signal,
        });
        clearTimeout(t);
        const j = await res.json();
        if (j.error) throw new Error(j.error.message);
        state.rpcIndex = idx;
        return j.result;
      } catch { /* 다음 엔드포인트 시도 */ }
    }
    throw new Error("모든 RPC 엔드포인트 응답 실패");
  }

  // Transfer 로그 → 표준 이벤트
  const topicToAddr = (t) => "0x" + t.slice(26);
  const decodeLog = (log, ts) => ({
    tx_hash: log.transactionHash,
    log_index: parseInt(log.logIndex, 16),
    block_number: parseInt(log.blockNumber, 16),
    from: topicToAddr(log.topics[1]),
    to: topicToAddr(log.topics[2]),
    amount: Number(BigInt(log.data === "0x" ? "0x0" : log.data)) / 10 ** C.USDT_DECIMALS,
    token: "USDT",
    event_time: ts,
    source: "LIVE",
  });

  // 블록 주기 동안 나눠서 흘려보내 실시간 스트림처럼 표시
  let dripQueue = [], dripTimer = null;
  function drip(events) {
    dripQueue.push(...events);
    if (dripTimer) return;
    const tick = () => {
      if (!dripQueue.length || stopped) { dripTimer = null; return; }
      emit("transfer", dripQueue.shift());
      const gap = Math.max(120, Math.min(1500, C.POLL_MS / Math.max(dripQueue.length, 1)));
      dripTimer = setTimeout(tick, gap);
    };
    tick();
  }

  const snapshot = () => ({ mode: state.mode, source: state.source, block: state.block, gasGwei: state.gasGwei });

  let lastBlock = null;
  async function pollLive() {
    const [bnHex, gasHex] = await Promise.all([rpc("eth_blockNumber"), rpc("eth_gasPrice")]);
    const bn = parseInt(bnHex, 16);
    state.block = bn;
    state.gasGwei = parseInt(gasHex, 16) / 1e9;
    state.source = "LIVE";
    emit("status", snapshot());
    if (bn === lastBlock) return;
    const from = lastBlock ? Math.max(lastBlock + 1, bn - 1) : bn;
    lastBlock = bn;
    const logs = await rpc("eth_getLogs", [{
      fromBlock: "0x" + from.toString(16),
      toBlock: "0x" + bn.toString(16),
      address: C.USDT_CONTRACT,
      topics: [C.TRANSFER_TOPIC],
    }]);
    const ts = new Date().toISOString();
    const ev = logs.map((l) => decodeLog(l, ts));
    // 블록당 최대 40건 표본 — 밴드(900~990)·고래(≥1M)는 우선 포함
    const priority = ev.filter((e) => (e.amount >= 900 && e.amount <= 990) || e.amount >= 1e6);
    const rest = ev.filter((e) => !priority.includes(e)).sort(() => Math.random() - 0.5)
      .slice(0, Math.max(0, 40 - priority.length));
    drip([...priority, ...rest].sort(() => Math.random() - 0.5));
  }

  // 시뮬레이션 (메인넷과 동일 스키마)
  const simPool = Array.from({ length: 60 }, randAddr);
  function simTransfer() {
    const r = Math.random();
    let amount;
    if (r < 0.06) amount = 900 + Math.random() * 90;          // 트래블룰 회피 밴드
    else if (r < 0.08) amount = 1e6 + Math.random() * 9e6;     // 고래
    else amount = Math.min(logNormal(5.2, 1.6), 500000);       // 일상 분포
    const pick = () => (Math.random() < 0.6 ? simPool[(Math.random() * simPool.length) | 0] : randAddr());
    return {
      tx_hash: randTxHash(), log_index: (Math.random() * 300) | 0,
      block_number: state.block, from: pick(), to: pick(),
      amount: Math.round(amount * 100) / 100, token: "USDT",
      event_time: new Date().toISOString(), source: "SIM",
    };
  }
  function pollSim() {
    state.block = (state.block || 21_850_000 + ((Math.random() * 1000) | 0)) + 1;
    state.gasGwei = Math.max(0.3, (state.gasGwei || 3) + (Math.random() - 0.5) * 0.8);
    state.source = "SIM";
    emit("status", snapshot());
    drip(Array.from({ length: 8 + ((Math.random() * 10) | 0) }, simTransfer));
  }

  async function poll() {
    if (state.mode === "SIM") return pollSim();
    try { await pollLive(); }
    catch (e) {
      if (state.mode === "LIVE") { state.source = "ERROR"; emit("status", { ...snapshot(), error: e.message }); }
      else pollSim(); // AUTO → 시뮬레이션 폴백
    }
  }

  let timer;
  return {
    on(ev, fn) { listeners[ev].add(fn); return () => listeners[ev].delete(fn); },
    start() { stopped = false; poll(); clearInterval(timer); timer = setInterval(poll, C.POLL_MS); },
    stop() { stopped = true; clearInterval(timer); clearTimeout(dripTimer); dripTimer = null; dripQueue = []; },
    setMode(m) {
      state.mode = m; lastBlock = null; dripQueue = [];
      emit("status", snapshot());
      this.start();
    },
    // 데모용: 합성 이벤트를 스트림에 주입
    inject(events) {
      drip(events.map((e) => ({
        block_number: state.block, source: "SIM", token: "USDT",
        event_time: new Date().toISOString(), log_index: 0, ...e,
      })));
    },
    getState: snapshot,
  };
}
