// JSON 문자열을 토큰 단위로 나눠 색상 span 으로 렌더링 (innerHTML 미사용 → XSS 안전)
const TOKEN = /("(\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g;

function JsonView({ data }) {
  const json = JSON.stringify(data, null, 2);
  const parts = [];
  let last = 0;
  for (const m of json.matchAll(TOKEN)) {
    if (m.index > last) parts.push(json.slice(last, m.index));
    const t = m[0];
    const cls = t.startsWith('"') ? (t.trimEnd().endsWith(":") ? "k" : "s") : /true|false|null/.test(t) ? "b" : "n";
    parts.push(<span key={m.index} className={cls}>{t}</span>);
    last = m.index + t.length;
  }
  parts.push(json.slice(last));
  return <pre className="code mono">{parts}</pre>;
}

export default function EvidencePanel({ selected, frozen, onCopy, onFreezeToggle }) {
  const data = selected?.data ?? { info: "송금 또는 경보를 선택하면 이벤트/탐지 근거가 표시됩니다." };
  const target = selected?.data?.target_wallet;
  const isFrozen = target && frozen.includes(target.toLowerCase());
  return (
    <article className="card code-card">
      <div className="card-head">
        <h2>Evidence JSON <small className="mono">{selected?.topic ?? "transfer.events.v1"}</small></h2>
        <div style={{ display: "flex", gap: 6 }}>
          {target && (
            <button className="ghost small" onClick={() => onFreezeToggle(target, !isFrozen)}>
              {isFrozen ? "🔓 동결 해제" : "🔒 대상 지갑 동결"}
            </button>
          )}
          <button className="ghost small" onClick={() => onCopy(data)}>복사</button>
        </div>
      </div>
      <JsonView data={data} />
    </article>
  );
}
