import { useState } from "react";
import { CONFIG } from "../config.js";
import { fmt, isAddress, randAddr } from "../lib/util.js";

const T = CONFIG.FDS;
const VASPS = [
  ["VASP_KR_01", "VASP_KR_01 · 국내 신고 거래소"],
  ["VASP_KR_02", "VASP_KR_02 · 국내 신고 거래소"],
  ["VASP_SG_07", "VASP_SG_07 · 해외 신고 거래소"],
  ["UNVERIFIED_VASP", "UNVERIFIED · 미신고 해외 VASP"],
  ["UNHOSTED", "UNHOSTED · 개인 지갑"],
];
const CHIPS = [100, 500, 980, 3000, 12000];

/** 수신 주소 실시간 검증 결과 */
function checkAddress(v, wallet) {
  if (!v) return { ok: false, text: "0x + 16진수 40자리", cls: "hint" };
  if (!isAddress(v)) return { ok: false, invalid: true, text: `형식 오류 (${v.length}/42자)`, cls: "hint err" };
  const lv = v.toLowerCase();
  if (lv === wallet.address.toLowerCase()) return { ok: false, invalid: true, text: "본인 지갑으로는 송금할 수 없습니다", cls: "hint err" };
  if (wallet.frozen.includes(lv)) return { ok: true, text: "⚠ 동결(블랙리스트) 지갑입니다", cls: "hint err" };
  const known = wallet.history.some((h) => h.to.toLowerCase() === lv && h.status === "SETTLED");
  return { ok: true, text: known ? "✓ 이전 거래 이력이 있는 지갑" : "✓ 유효한 주소 · 최초 거래 지갑", cls: "hint ok" };
}

export default function SendForm({ wallet, gasGwei, busy, result, onSubmit, onToast }) {
  const [form, setForm] = useState({ to: "", vasp: "VASP_KR_01", destTag: "", amount: "", beneName: "", purpose: "개인 간 송금" });
  const set = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }));

  const addr = checkAddress(form.to.trim(), wallet);
  const amount = parseFloat(String(form.amount).replace(/,/g, "")) || 0;
  const fee = (CONFIG.ERC20_GAS_LIMIT * (gasGwei || 3)) / 1e9;
  const after = wallet.usdt - amount;
  const ratio = wallet.usdt > 0 ? amount / wallet.usdt : 0;
  const travel = amount >= T.TRAVEL_RULE_USDT;

  function submit(e) {
    e.preventDefault();
    if (!addr.ok) return onToast("수신 지갑 주소를 확인해주세요");
    if (!(amount > 0)) return onToast("송금 수량을 입력해주세요");
    onSubmit({
      to: form.to.trim(), amount, vasp: form.vasp,
      destination_tag: form.destTag.trim() || null,
      beneficiary_name: form.beneName.trim() || null,
      purpose: form.purpose,
    });
  }

  return (
    <article className="card">
      <div className="card-head">
        <h2>USDT 송금하기 <small className="mono">ERC-20</small></h2>
        <span className="tag tag-violet">특금법 트래블룰 적용</span>
      </div>

      <form onSubmit={submit} autoComplete="off" noValidate>
        <label className="field">
          <span>수신 지갑 주소 <em className="mono">to</em></span>
          <div className="input-row">
            <input className={`mono ${addr.invalid ? "invalid" : ""}`} placeholder="0x로 시작하는 42자리 주소" maxLength={42} value={form.to} onChange={set("to")} />
            <button type="button" className="ghost" title="예시 주소 채우기" onClick={() => setForm((f) => ({ ...f, to: randAddr() }))}>예시</button>
          </div>
          <small className={addr.cls}>{addr.text}</small>
        </label>

        <div className="field-2">
          <label className="field">
            <span>수취 거래소 (VASP)</span>
            <select value={form.vasp} onChange={set("vasp")}>
              {VASPS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </select>
          </label>
          <label className="field">
            <span>Destination Tag / Memo</span>
            <input className="mono" placeholder="tag_104928 (선택)" value={form.destTag} onChange={set("destTag")} />
          </label>
        </div>

        <label className="field">
          <span>송금 수량 (USDT)</span>
          <div className="input-row">
            <input className="mono" inputMode="decimal" placeholder="0.00" value={form.amount} onChange={set("amount")} />
            <button type="button" className="ghost" onClick={() => setForm((f) => ({ ...f, amount: wallet.usdt.toFixed(2) }))}>MAX</button>
          </div>
          <div className="chips">
            {CHIPS.map((c) => (
              <button key={c} type="button" className="chip" onClick={() => setForm((f) => ({ ...f, amount: String(c) }))}>{c.toLocaleString()}</button>
            ))}
          </div>
        </label>

        {travel && (
          <div className="travel">
            <div className="travel-head">⚖ 트래블룰 대상 (1,000 USDT 이상) — 수취인 정보 필수</div>
            <div className="field-2">
              <label className="field"><span>수취인 성명</span><input placeholder="홍길동" value={form.beneName} onChange={set("beneName")} /></label>
              <label className="field"><span>송금 목적</span>
                <select value={form.purpose} onChange={set("purpose")}>
                  {["개인 간 송금", "거래소 입금", "상거래 대금", "기타"].map((p) => <option key={p}>{p}</option>)}
                </select>
              </label>
            </div>
          </div>
        )}

        <div className="summary mono">
          <div><span>예상 네트워크 수수료</span><b>≈ {fee.toFixed(6)} ETH</b></div>
          <div><span>송금 후 잔액</span><b className={after < 0 ? "danger" : ""}>{fmt(after)} USDT</b></div>
          <div><span>잔액 소진율</span><b className={ratio >= T.DRAIN_RATIO ? "danger" : ratio >= 0.7 ? "warn" : ""}>{(ratio * 100).toFixed(1)}%</b></div>
        </div>

        <button type="submit" className="primary" disabled={busy}>{busy ? "사전 검증 중…" : "안심 송금 및 사전 검증 요청"}</button>
        {result && <div className={`result ${result.kind}`}>{result.content}</div>}
      </form>
    </article>
  );
}
