import { CONFIG } from "../config.js";
import { fmt, short } from "../lib/util.js";

const [LO, HI] = CONFIG.FDS.EVASION_BAND;

export default function ChainStream({ items, stats, source, onSelect }) {
  const srcLabel = source === "LIVE" ? "LIVE · JSON-RPC" : source === "SIM" ? "SIM · 합성 데이터" : source === "ERROR" ? "ERROR" : "—";
  return (
    <article className="card">
      <div className="card-head">
        <h2><i className="dot dot-blue" />이더리움 메인넷 USDT 실시간 스트림</h2>
        <span className="tag mono">{srcLabel}</span>
      </div>
      <div className="stream-head mono"><span>Tx Hash</span><span>From → To</span><span className="r">Amount</span></div>
      <ul className="stream">
        {items.map((tr) => {
          const band = tr.amount >= LO && tr.amount <= HI;
          const whale = tr.amount >= 1e6;
          const flag = band ? "BAND" : whale ? "WHALE" : tr.demo ? "DEMO" : null;
          return (
            <li key={tr.key} className={band ? "band" : whale ? "whale" : ""} style={{ cursor: "pointer" }}
                onClick={(e) => { if (e.target.tagName !== "A") onSelect(tr, `onchain · ${tr.source}`); }}>
              {tr.source === "LIVE"
                ? <a className="h" href={CONFIG.EXPLORER_TX + tr.tx_hash} target="_blank" rel="noopener noreferrer" title={tr.tx_hash}>{short(tr.tx_hash, 8, 4)}</a>
                : <span className="h" title={`${tr.tx_hash} (시뮬레이션)`}>{short(tr.tx_hash, 8, 4)}</span>}
              <span className="ft" title={`${tr.from} → ${tr.to}`}>{short(tr.from, 6, 3)} → {short(tr.to, 6, 3)}</span>
              <span className="amt">{fmt(tr.amount)} USDT{flag && <span className="flag">{flag}</span>}</span>
            </li>
          );
        })}
      </ul>
      <div className="stream-foot mono">
        <span>수신 <b>{stats.count.toLocaleString()}</b>건</span>
        <span>트래블룰 회피 밴드(900~990) <b>{stats.band}</b>건</span>
        <span>고래(≥1M) <b>{stats.whale}</b>건</span>
      </div>
    </article>
  );
}
