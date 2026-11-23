/* 관제 웹 공통 UI 조각 */
import { useEffect, useState } from "react";
import { toast } from "sonner";
import { GRADES, STATUS, ORIGINS, ruleName, rulePlain, R_RULES } from "./data/meta.js";

/* ── 포맷 ── */
export const usd = (v, d = 2) => Number(v || 0).toLocaleString("en-US", { maximumFractionDigits: d });
export const short = (a, h = 6, t = 4) => (a && a.length > h + t + 2 ? `${a.slice(0, h)}…${a.slice(-t)}` : a || "");
const p2 = (n) => String(n).padStart(2, "0");
/** 14:32:07 */
export const clock = (ts) => { const d = new Date(ts); return `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}`; };
/** 2026-11-16 14:32:07 */
export const dateTime = (ts) => { const d = new Date(ts); return `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())} ${clock(ts)}`; };
export function ago(ts, now = Date.now()) {
  const s = Math.max(0, Math.round((now - ts) / 1000));
  if (s < 5) return "방금";
  if (s < 60) return `${s}초 전`;
  if (s < 3600) return `${Math.floor(s / 60)}분 전`;
  if (s < 86400) return `${Math.floor(s / 3600)}시간 전`;
  return `${Math.floor(s / 86400)}일 전`;
}

export function useNow(ms = 1000) {
  const [n, setN] = useState(Date.now());
  useEffect(() => { const t = setInterval(() => setN(Date.now()), ms); return () => clearInterval(t); }, [ms]);
  return n;
}

export const copy = (text, label = "복사했습니다") => {
  navigator.clipboard?.writeText(text).then(() => toast.success(label), () => toast.error("복사할 수 없습니다"));
};

/* ── 배지 ── */
export const Grade = ({ g }) => <span className={`grade g-${g}`}>{GRADES[g]?.label || g}</span>;

export function Status({ s }) {
  const st = STATUS[s] || { label: s, tone: "muted", icon: "" };
  return <span className={`status t-${st.tone}`} title={st.desc}><i>{st.icon}</i>{st.label}</span>;
}

export function Rule({ id, plain = false }) {
  return (
    <span className="rule" title={rulePlain(id)}>
      <b className={R_RULES[id] ? "r1" : "r2"}>{id}</b> {ruleName(id)}
      {plain && <small>{rulePlain(id)}</small>}
    </span>
  );
}

export function OriginTag({ origin, isNew }) {
  const o = ORIGINS[origin] || ORIGINS.MAINNET;
  return (
    <>
      <span className={`otag o-${o.tone}`}>{o.label}</span>
      {isNew && <span className="otag o-new" title="처음 본 지 24시간 이내">새 지갑</span>}
    </>
  );
}

export function Avatar({ origin, size = 26, label }) {
  const o = ORIGINS[origin] || ORIGINS.MAINNET;
  return <span className={`avatar o-${o.tone}`} style={{ width: size, height: size, fontSize: size * 0.42 }}>{label || o.short}</span>;
}

export function Addr({ a, full = false, copyable = false, h, t }) {
  return (
    <span className="addr mono" title={a}>
      {full ? a : short(a, h, t)}
      {copyable && (
        <button className="icon-btn" title="주소 복사" onClick={(e) => { e.stopPropagation(); copy(a, "주소를 복사했습니다"); }}>
          <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" strokeWidth="2"><rect x="9" y="9" width="12" height="12" rx="2" /><path d="M5 15V5a2 2 0 0 1 2-2h10" /></svg>
        </button>
      )}
    </span>
  );
}

export const Money = ({ v, unit = true }) => (
  <span className="money mono">{usd(v)}{unit && <small> USDT</small>}</span>
);

export function Ago({ ts, now }) {
  return <span className="ago" title={dateTime(ts)}>{ago(ts, now)}</span>;
}

/* ── 레이아웃 ── */
export function Card({ title, desc, right, children, className = "", pad = true }) {
  return (
    <section className={`card ${className}`}>
      {(title || right) && (
        <header className="card-h">
          <div>
            {title && <h2>{title}</h2>}
            {desc && <p>{desc}</p>}
          </div>
          {right && <div className="card-r">{right}</div>}
        </header>
      )}
      <div className={pad ? "card-b" : ""}>{children}</div>
    </section>
  );
}

export function Kpi({ label, value, desc, tone = "", spark, onClick, sub }) {
  const Tag = onClick ? "button" : "div";
  return (
    <Tag className={`kpi ${tone ? "k-" + tone : ""} ${onClick ? "clickable" : ""}`} onClick={onClick}>
      <div className="kpi-top">
        <span className="kpi-label">{label}</span>
        {spark && <Spark data={spark} />}
      </div>
      <div className="kpi-val">{typeof value === "number" ? value.toLocaleString() : value}{sub && <small>{sub}</small>}</div>
      {desc && <div className="kpi-desc">{desc}</div>}
    </Tag>
  );
}

/** 작은 추세선 (SVG) */
export function Spark({ data, w = 60, h = 22 }) {
  const vals = data.map((d) => d.v);
  const max = Math.max(1, ...vals), min = Math.min(0, ...vals);
  const pts = vals.map((v, i) => `${(i / Math.max(1, vals.length - 1)) * w},${h - 2 - ((v - min) / (max - min || 1)) * (h - 4)}`).join(" ");
  return <svg className="spark" width={w} height={h} viewBox={`0 0 ${w} ${h}`}><polyline points={pts} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" strokeLinecap="round" /></svg>;
}

/** 실측값 vs 기준값 막대 */
export function EvidenceBar({ item, compact = false }) {
  const { label, value = 0, threshold = 0, unit, op } = item;
  const hit = op === ">=" ? value >= threshold : op === ">" ? value > threshold : op === "<" ? value < threshold : value <= threshold;
  const meaningful = op === "<" || op === "<=" ? true : value > 0 || threshold > 0;
  const scale = Math.max(value, threshold, 1) * (op === ">" && threshold === 0 ? 1 : 1.15);
  const pct = Math.min(100, (value / scale) * 100);
  const tpct = Math.min(100, (threshold / scale) * 100);
  const opText = { ">=": "이상", ">": "초과", "<": "미만", "<=": "이하" }[op];
  return (
    <div className={`ev ${hit ? "hit" : ""} ${compact ? "compact" : ""}`}>
      <div className="ev-top">
        <span className="ev-label">{label}</span>
        <span className={`ev-flag ${hit ? "on" : ""}`}>{hit ? "기준 넘음" : meaningful ? "해당 없음" : "해당 없음"}</span>
      </div>
      <div className="ev-val mono">{usd(value, 1)} <small>{unit}</small></div>
      <div className="ev-track">
        <div className="ev-fill" style={{ width: `${pct}%` }} />
        {!(op === ">" && threshold === 0) && <div className="ev-mark" style={{ left: `${tpct}%` }} title={`기준 ${usd(threshold)}`} />}
      </div>
      <div className="ev-th">기준 {usd(threshold)} {unit} {opText}</div>
    </div>
  );
}

export function Modal({ open, title, children, footer, onClose, wide = false }) {
  useEffect(() => {
    if (!open) return;
    const k = (e) => e.key === "Escape" && onClose?.();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <div className="modal-bg" onMouseDown={(e) => e.target === e.currentTarget && onClose?.()}>
      <div className={`modal ${wide ? "wide" : ""}`} role="dialog" aria-modal="true" aria-label={title}>
        <header><h3>{title}</h3><button className="icon-btn" onClick={onClose} aria-label="닫기">✕</button></header>
        <div className="modal-b">{children}</div>
        {footer && <footer>{footer}</footer>}
      </div>
    </div>
  );
}

export const Empty = ({ children }) => <div className="empty">{children}</div>;

export function Seg({ value, options, onChange, size = "" }) {
  return (
    <div className={`seg ${size}`} role="tablist">
      {options.map(([v, l]) => (
        <button key={v} role="tab" aria-selected={value === v} className={value === v ? "on" : ""} onClick={() => onChange(v)}>{l}</button>
      ))}
    </div>
  );
}
