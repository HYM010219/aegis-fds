import { useCallback, useEffect, useRef, useState } from "react";
import { CONFIG } from "./config.js";
import { fmt, short, randHex, sleep } from "./lib/util.js";
import { observe, Scenarios } from "./lib/fds.js";
import * as Api from "./lib/api.js";
import { useChain } from "./hooks/useChain.js";
import Topbar from "./components/Topbar.jsx";
import ProfileCard from "./components/ProfileCard.jsx";
import SendForm from "./components/SendForm.jsx";
import RiskAssessment from "./components/RiskAssessment.jsx";
import AlertFeed from "./components/AlertFeed.jsx";
import EvidencePanel from "./components/EvidencePanel.jsx";
import ChainStream from "./components/ChainStream.jsx";
import History from "./components/History.jsx";

const T = CONFIG.FDS;
const [LO, HI] = T.EVASION_BAND;
let streamSeq = 0;

export default function App() {
  const [wallet, setWallet] = useState(null);
  const [risk, setRisk] = useState(8);
  const [assessment, setAssessment] = useState(null);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [alerts, setAlerts] = useState([]);
  const [stream, setStream] = useState([]);
  const [stats, setStats] = useState({ count: 0, band: 0, whale: 0 });
  const [selected, setSelected] = useState(null);
  const [toast, setToast] = useState(null);

  const walletRef = useRef(null);
  walletRef.current = wallet;
  const lastReq = useRef(null);   // 동일 요청 재전송(R01) 판별
  const demoIdx = useRef(0);

  /* ── 공통 ── */
  const showToast = useCallback((msg) => setToast({ msg, id: Date.now() }), []);
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 2200);
    return () => clearTimeout(t);
  }, [toast]);

  const refreshWallet = useCallback(async () => setWallet(await Api.getWallet()), []);
  useEffect(() => { refreshWallet(); }, [refreshWallet]);

  const select = useCallback((data, topic) => setSelected({ data, topic }), []);

  /* ── 경보 ── */
  const pushAlert = useCallback((a) => {
    setAlerts((list) => [a, ...list].slice(0, 30));
    const w = walletRef.current;
    if (a.action === "AUTO_FREEZE" && w && a.target_wallet.toLowerCase() !== w.address.toLowerCase()) {
      // 위험점수 90+ → Redis fds:frozen_wallets 자동 등록 (Mock: localStorage)
      Api.freeze(a.target_wallet, true).then(refreshWallet);
      showToast(`🔒 위험점수 ${a.risk_score} — ${short(a.target_wallet)} 자동 선제 동결`);
    }
  }, [refreshWallet, showToast]);

  /* ── 메인넷 스트림 ── */
  const onTransfer = useCallback((tr) => {
    const band = tr.amount >= LO && tr.amount <= HI;
    const whale = tr.amount >= 1e6;
    setStream((s) => [{ ...tr, key: ++streamSeq }, ...s].slice(0, 40));
    setStats((s) => ({ count: s.count + 1, band: s.band + (band ? 1 : 0), whale: s.whale + (whale ? 1 : 0) }));
    const alert = observe(tr);
    if (alert) pushAlert(alert);
  }, [pushAlert]);

  const chain = useChain(onTransfer);

  /* ── 송금 ── */
  async function handleSubmit(req) {
    const sig = `${req.to.toLowerCase()}|${req.amount}`;
    const last = lastReq.current;
    const reuse = last && last.sig === sig && Date.now() - last.at < T.IDEMPOTENCY_WINDOW_MS;
    const idempotency_key = reuse ? last.key : `idem_${wallet.address.slice(2, 8)}_${randHex(8)}`;
    lastReq.current = { sig, key: idempotency_key, at: Date.now() };

    setBusy(true);
    setResult({ kind: "pending", content: <>⏳ Fast-Path 입구 검문 중 (동결·중복·잔액·전액유출·트래블룰)…</> });
    try {
      const { http, body } = await Api.submitTransfer({ ...req, idempotency_key }, chain.getState());
      setAssessment({ checks: body.checks || [], decision: body.tx_status, risk: body.risk_score ?? 0 });
      setRisk(body.risk_score ?? 0);

      if (body.tx_status === "SETTLED") {
        setResult({ kind: "pending", content: <>⛓ 사전 검증 통과 · 블록 포함 대기 중…<div className="mono">{body.tx_hash}</div></> });
        await sleep(1100);
        setResult({ kind: "ok", content: <>✅ <b>[정상 송금 완료]</b> {fmt(req.amount)} USDT 전송 성공 · HTTP {http}
          <div className="mono">tx {body.tx_hash}<br />block #{body.block_number?.toLocaleString()} · gas {body.gas_used?.toLocaleString()} · fee {body.fee_eth?.toFixed(6)} ETH</div></> });
      } else if (body.tx_status === "PENDING_REVIEW") {
        setResult({ kind: "hold", content: <>⏸ <b>[사전 보류 · PENDING_REVIEW]</b> HTTP {http}<br />{body.reason}<div className="mono">tx {body.tx_hash} (reserved)</div></> });
      } else if (body.tx_status === "DUPLICATE") {
        setResult({ kind: "err", content: <>⧉ <b>[중복 요청 차단 · R01]</b> HTTP {http}<br />{body.reason}<div className="mono">원본 tx {body.original_tx_hash}</div></> });
      } else {
        setResult({ kind: "err", content: <>⛔ <b>[송금 거절 · REJECTED]</b> HTTP {http}<br />{body.reason}</> });
      }

      if (body.event) select(body.event, "transfer.events.v1");
      // Stream-Path 경보는 1~2초 뒤 도착 (비동기 경로 모사)
      (body.alerts || []).forEach((a, i) => setTimeout(() => pushAlert(a), 1200 + i * 400));
    } catch (err) {
      setResult({ kind: "err", content: <>서버 통신 실패: {err.message}</> });
    } finally {
      setBusy(false);
      await refreshWallet();
    }
  }

  /* ── 데모 공격 (스머핑 → 순환송금 → 빈도급증 순환) ── */
  function runDemo() {
    const kind = ["smurfing", "circular", "velocity"][demoIdx.current++ % 3];
    if (kind === "smurfing") {
      chain.inject(Scenarios.smurfing());
      showToast("⚡ 3B SmurfingSyndicate: 대포 지갑 분할 송금 주입");
    } else if (kind === "circular") {
      const { hops, alert } = Scenarios.circular();
      chain.inject(hops);
      setTimeout(() => pushAlert(alert), 2500);
      showToast("⚡ 3E CircularTransferRing: 순환 송금 고리 주입");
    } else {
      const { txs, alert } = Scenarios.velocity();
      chain.inject(txs);
      setTimeout(() => pushAlert(alert), 2000);
      showToast("⚡ 3C VelocitySpammer: 1분 다건 송금 주입");
    }
  }

  async function toggleFreeze(target, on) {
    await Api.freeze(target, on);
    await refreshWallet();
    showToast(on ? `🔒 ${short(target)} 동결 — 이후 송금은 403 차단` : `🔓 ${short(target)} 동결 해제`);
  }

  function reset() {
    if (!confirm("데모 지갑의 잔액·내역·동결 목록을 초기화할까요?")) return;
    setWallet(Api.resetWallet());
    setRisk(8); setAssessment(null); setResult(null);
    showToast("초기화 완료");
  }

  const copy = (text, msg) => navigator.clipboard?.writeText(text).then(() => showToast(msg));

  if (!wallet) return <div className="foot mono" style={{ paddingTop: 40 }}>지갑 불러오는 중…</div>;

  return (
    <>
      <Topbar status={chain.status} onModeChange={(m) => { chain.setMode(m); showToast(`데이터 모드: ${m}`); }} />

      <main className="grid">
        <section className="col">
          <ProfileCard wallet={wallet} risk={risk} onCopy={() => copy(wallet.address, "내 지갑 주소가 복사되었습니다")} />
          <SendForm wallet={wallet} gasGwei={chain.status.gasGwei} busy={busy} result={result} onSubmit={handleSubmit} onToast={showToast} />
          <RiskAssessment assessment={assessment} />
        </section>

        <section className="col">
          <AlertFeed alerts={alerts} onSelect={select} onDemo={runDemo} />
          <EvidencePanel selected={selected} frozen={wallet.frozen}
            onCopy={(d) => copy(JSON.stringify(d, null, 2), "JSON 복사 완료")} onFreezeToggle={toggleFreeze} />
          <ChainStream items={stream} stats={stats} source={chain.status.source} onSelect={select} />
          <History history={wallet.history} onSelect={select} onReset={reset} />
        </section>
      </main>

      <footer className="foot mono">
        Aegis-FDS v7.4 · React + Vite — 송금 처리는 <b>{Api.isLive() ? `API (${CONFIG.API_BASE})` : "Mock API"}</b>
        {Api.isLive() ? "로 처리됩니다." : "(브라우저 내 시뮬레이션)이며, 실제 자산은 이동하지 않습니다. 백엔드 연동 시 .env.local 의 VITE_API_BASE 를 설정하세요."}
      </footer>

      {toast && <div className="toast" key={toast.id}>{toast.msg}</div>}
    </>
  );
}
