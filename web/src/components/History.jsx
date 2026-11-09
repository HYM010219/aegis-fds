import { fmt } from "../lib/util.js";

const CLS = { SETTLED: "sev-low", PENDING_REVIEW: "sev-mid", REJECTED: "sev-high" };
const LBL = { SETTLED: "완료", PENDING_REVIEW: "보류", REJECTED: "거절" };

export default function History({ history, onSelect, onReset }) {
  return (
    <article className="card">
      <div className="card-head">
        <h2>내 송금 내역</h2>
        <button className="ghost small" title="잔액·내역 초기화" onClick={onReset}>초기화</button>
      </div>
      <ul className="history">
        {history.length === 0 && <li className="empty">송금 내역이 없습니다.</li>}
        {history.slice(0, 30).map((h) => (
          <li key={`${h.ts}-${h.tx_hash ?? "rej"}`} onClick={() => onSelect(h.event, "transfer.events.v1")}>
            <div className="h-left">
              <div className="h-to">→ {h.to} · {h.vasp}</div>
              <div className="mono" style={{ fontSize: 11, color: "var(--dim)" }}>
                {new Date(h.ts).toLocaleString("ko-KR")}{h.block ? ` · #${h.block.toLocaleString()}` : ""}
              </div>
            </div>
            <div>
              <div className="h-amt">-{fmt(h.amount)} USDT</div>
              <div style={{ textAlign: "right" }}><span className={`sev ${CLS[h.status]}`}>{LBL[h.status]}</span></div>
            </div>
          </li>
        ))}
      </ul>
    </article>
  );
}
