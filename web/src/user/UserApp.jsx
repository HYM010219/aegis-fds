/* ==========================================================
   사용자 페이지 — USDT 송금 (고객 화면)
   · 관리자 기능 · 룰 코드 · HTTP 코드 · 탐지 근거는 보여 주지 않는다
     (기준을 알려 주면 피해 가기 쉬워지므로 결과만 쉬운 말로 안내)
   · 송금 요청은 게이트웨이 규격(lib/api.js)으로 보내고, 백엔드 전에는 Mock 엔진이 판정
   ========================================================== */
import { useCallback, useEffect, useMemo, useState } from "react";
import * as Api from "../lib/api.js";
import * as Engine from "../lib/engine.js";
import { CONFIG } from "../config.js";
import { isAddress, randAddr, randTxHash, fmt, short } from "../lib/util.js";
import "./user.css";

const SUPPORT = "고객센터 1588-0000";
const p2 = (n) => String(n).padStart(2, "0");
/** 11월 16일 14:32 */
const when = (ts) => { const d = new Date(ts); return `${d.getMonth() + 1}월 ${d.getDate()}일 ${p2(d.getHours())}:${p2(d.getMinutes())}`; };

/** 게이트웨이 응답 → 고객에게 보여 줄 문구 */
function customerMessage(r) {
  if (r.state === "OK") return { tone: "ok", title: "송금이 완료되었습니다", text: `${fmt(r.amount)} USDT를 ${short(r.to, 6, 4)}(으)로 보냈습니다.` };
  if (r.state === "HOLD") return { tone: "wait", title: "송금이 잠시 지연되고 있습니다", text: "보안 확인 중입니다. 잠시 후 다시 시도해 주세요. 받는 것(입금)은 그대로 됩니다." };
  if (r.state === "FROZEN") return { tone: "stop", title: "송금이 제한된 계정입니다", text: `계정 보호를 위해 송금이 제한되었습니다. ${SUPPORT}로 문의해 주세요.` };
  const reason = r.body?.reason || "";
  if (reason.startsWith("BLACKLISTED")) return { tone: "stop", title: "보낼 수 없는 주소입니다", text: "이 주소로는 송금할 수 없습니다. 주소를 다시 확인해 주세요." };
  if (reason === "DUPLICATE_REQUEST") return { tone: "wait", title: "이미 처리된 요청입니다", text: "같은 송금 요청이 이미 접수되었습니다. 내역을 확인해 주세요." };
  if (reason === "INVALID_AMOUNT") return { tone: "stop", title: "금액을 확인해 주세요", text: "소수점 6자리까지 입력할 수 있습니다." };
  return { tone: "stop", title: "송금을 처리하지 못했습니다", text: `잠시 후 다시 시도해 주세요. 계속되면 ${SUPPORT}로 문의해 주세요.` };
}
const HISTORY_LABEL = {
  OK: ["완료", "ok"], HOLD: ["지연 · 다시 시도", "wait"], FROZEN: ["송금 제한", "stop"],
};
const histLabel = (h) => HISTORY_LABEL[h.state] || (h.reason?.startsWith("BLACKLISTED") ? ["보낼 수 없는 주소", "stop"] : ["처리 안 됨", "stop"]);

/** 받는 주소 검사 + 주소 오염(앞뒤만 같은 가짜 주소) 경고 */
function checkTo(v, wallet) {
  if (!v) return null;
  if (!isAddress(v)) return { level: "err", text: `주소 형식이 맞지 않습니다 (0x로 시작하는 42자리, 지금 ${v.length}자리)` };
  const lv = v.toLowerCase();
  if (lv === wallet.address.toLowerCase()) return { level: "err", text: "내 지갑으로는 보낼 수 없습니다" };
  const sent = [...new Set(wallet.history.filter((h) => h.state === "OK").map((h) => h.to.toLowerCase()))];
  if (sent.includes(lv)) return { level: "ok", text: "✓ 전에 보낸 적 있는 주소입니다" };
  const look = sent.find((a) => a.slice(0, 6) === lv.slice(0, 6) && a.slice(-4) === lv.slice(-4));
  if (look) return { level: "danger", text: `⚠ 전에 보낸 ${short(look, 6, 4)} 와 앞뒤 글자가 같지만 다른 주소입니다. 가짜 주소(주소 오염) 사기일 수 있으니 전체 주소를 꼭 확인하세요.` };
  return { level: "warn", text: "처음 보내는 주소입니다. 앞뒤 글자만 비슷한 가짜 주소인지 확인하세요." };
}

export default function UserApp() {
  const [wallet, setWallet] = useState(null);
  const [to, setTo] = useState("");
  const [amount, setAmount] = useState("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState(null);
  const [now, setNow] = useState(Date.now());
  const [, force] = useState(0);
  const [ack, setAck] = useState(false);
  useEffect(() => setAck(false), [to]);

  const refresh = useCallback(async () => setWallet(await Api.getWallet()), []);
  useEffect(() => {
    document.documentElement.dataset.theme = "dark";
    refresh();
    Engine.start(); // 2단계(Spark) 주기 분석 흉내
    const off = Engine.subscribe(() => { force((x) => x + 1); refresh(); });
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => { off(); clearInterval(t); Engine.stop(); };
  }, [refresh]);

  const st = wallet ? Engine.statusOf(wallet.address, now) : { state: "OK" };
  const chk = useMemo(() => (wallet ? checkTo(to.trim(), wallet) : null), [to, wallet]);
  if (!wallet) return <div className="boot">불러오는 중…</div>;

  const amt = parseFloat(String(amount).replace(/,/g, "")) || 0;
  const setPct = (p) => setAmount(String(Math.floor(wallet.usdt * p * 100) / 100));
  const canSend = chk && chk.level !== "err" && (chk.level !== "danger" || ack) && amt > 0 && amt <= wallet.usdt && !busy;

  async function send(e) {
    e.preventDefault();
    if (!canSend) return;
    setBusy(true);
    setMsg(null);
    try {
      const target = to.trim();
      const r = await Api.submitTransfer({ to: target, amount: amt, tx_hash: randTxHash(), vasp: null, destination_tag: null });
      setMsg(customerMessage({ ...r, amount: amt, to: target }));
      if (r.state === "OK") setAmount("");
      await refresh();
    } catch {
      setMsg({ tone: "stop", title: "연결에 실패했습니다", text: "네트워크를 확인한 뒤 다시 시도해 주세요." });
    } finally {
      setBusy(false);
    }
  }

  const lastTo = wallet.history.find((h) => h.state === "OK")?.to;
  const minutesLeft = st.state === "HOLD" ? Math.max(1, Math.ceil((st.until - now) / 60000)) : 0;

  return (
    <div className="ua">
      <div className="ua-phone">
        <header className="ua-top">
          <a href="/" className="ua-brand">Aegis Wallet</a>
          <span className="ua-addr mono" title={wallet.address}>{short(wallet.address, 6, 4)}</span>
        </header>

        <h1>USDT 보내기</h1>

        <section className="ua-balance">
          <span>보낼 수 있는 금액</span>
          <b className="mono">{fmt(wallet.usdt)} <small>USDT</small></b>
        </section>

        {st.state === "HOLD" && (
          <div className="ua-banner wait">
            <b>송금이 잠시 지연되고 있습니다</b>
            <span>보안 확인 중입니다. 약 {minutesLeft}분 뒤에 다시 시도해 주세요.</span>
          </div>
        )}
        {st.state === "FROZEN" && (
          <div className="ua-banner stop">
            <b>송금이 제한된 계정입니다</b>
            <span>계정 보호를 위해 송금이 제한되었습니다. {SUPPORT}로 문의해 주세요. (받는 것은 가능)</span>
          </div>
        )}

        <form onSubmit={send} autoComplete="off" noValidate>
          <label className="ua-fld">
            <span>받는 주소</span>
            <div className="ua-row">
              <input className="mono" placeholder="0x로 시작하는 주소" maxLength={42} value={to} onChange={(e) => setTo(e.target.value)} />
              <button type="button" className="ua-ghost" onClick={async () => { try { setTo((await navigator.clipboard.readText()).trim()); } catch { /* 권한 없음 */ } }}>붙여넣기</button>
            </div>
            {chk && <small className={`ua-hint ${chk.level}`}>{chk.text}</small>}
            {chk?.level === "danger" && <span className="ua-ack"><input type="checkbox" checked={ack} onChange={(e) => setAck(e.target.checked)} /> 전체 주소를 직접 확인했습니다</span>}
          </label>

          <label className="ua-fld">
            <span>금액</span>
            <div className="ua-amount">
              <input className="mono" inputMode="decimal" placeholder="0" value={amount} onChange={(e) => setAmount(e.target.value)} />
              <em>USDT</em>
            </div>
            {amt > wallet.usdt && <small className="ua-hint err">보낼 수 있는 금액보다 많습니다</small>}
          </label>
          <div className="ua-pcts">
            {[[0.25, "25%"], [0.5, "50%"], [1, "100%"]].map(([p, l]) => <button type="button" key={l} onClick={() => setPct(p)}>{l}</button>)}
          </div>

          <button className="ua-send" disabled={!canSend}>{busy ? "보내는 중…" : "송금"}</button>
        </form>

        {msg && (
          <div className={`ua-result ${msg.tone}`} role="status">
            <b>{msg.tone === "ok" ? "✓ " : msg.tone === "wait" ? "⏳ " : "✕ "}{msg.title}</b>
            <span>{msg.text}</span>
          </div>
        )}

        <section className="ua-hist">
          <h2>내 송금 내역</h2>
          {wallet.history.length === 0 && <p className="ua-empty">아직 보낸 내역이 없습니다.</p>}
          <ul>
            {wallet.history.slice(0, 12).map((h) => {
              const [label, tone] = histLabel(h);
              return (
                <li key={h.ts + h.tx_hash}>
                  <span><b className="mono">{fmt(h.amount)}</b> → <span className="mono">{short(h.to, 6, 4)}</span><small>{when(h.ts)}</small></span>
                  <em className={tone}>{label}</em>
                </li>
              );
            })}
          </ul>
        </section>
      </div>

      <details className="ua-demo">
        <summary>시연 도우미 <small>(실제 고객 화면에는 없음)</small></summary>
        <p>관리자 화면에 경보가 생기는 과정을 보여 줄 때 씁니다. 관리자 화면을 다른 탭에 열어 두면 결과가 바로 반영됩니다.</p>
        <div className="ua-demo-btns">
          <button onClick={() => setTo(randAddr())}>새 주소 채우기</button>
          <button onClick={() => setAmount("950")}>950 USDT (쪼개기 금액)</button>
          <button onClick={() => setAmount("60000")}>60,000 USDT (대량)</button>
          <button onClick={() => setTo(CONFIG.FDS.BLACKLIST[0])}>제재 지갑 주소</button>
          <button disabled={!lastTo} onClick={() => setTo(lastTo.slice(0, 6) + randAddr().slice(6, 38) + lastTo.slice(-4))}>가짜 주소 (마지막 받은 주소와 앞뒤만 같게)</button>
          <button onClick={() => { if (confirm("잔액·내역·탐지 기록을 처음 상태로 되돌릴까요?")) { Api.resetWallet(); refresh(); setMsg(null); } }}>초기화</button>
          <a href="/admin/" target="_blank" rel="noreferrer">관리자 화면 열기 ↗</a>
        </div>
        <ul className="ua-demo-tips">
          <li>쪼개기: 같은 주소로 950을 3번 → 지연(보류). 지연 중 몇 번 더 시도해도 L1에는 새 보류만 셉니다.</li>
          <li>새 주소 5곳으로 연달아 보내기 → 지연(R04). 보류 3번이 쌓이면 자동 동결(L1) → 송금 제한.</li>
          <li>관리자 화면에서 그 사건을 "동결 해제"하면 이 화면에서 다시 송금됩니다.</li>
        </ul>
      </details>
    </div>
  );
}
