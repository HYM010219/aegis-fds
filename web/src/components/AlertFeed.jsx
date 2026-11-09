export default function AlertFeed({ alerts, onSelect, onDemo }) {
  return (
    <article className="card">
      <div className="card-head">
        <h2><i className="dot dot-red" />FDS 실시간 위험 감시 피드</h2>
        <button className="ghost small" title="공격 시나리오 데모 주입" onClick={onDemo}>⚡ 공격 시연</button>
      </div>
      <ul className="feed">
        {alerts.length === 0 && <li className="empty">아직 경보가 없습니다.</li>}
        {alerts.map((a) => (
          <li key={a.case_id} className={`alert ${a.severity === "high" ? "" : a.severity === "mid" ? "mid" : "info"}`} onClick={() => onSelect(a, "fds.alerts.v1")}>
            <div className="a-top">
              <span className="a-rule">🚨 [{a.rule_code}]</span>
              <span className="a-score">위험도 {a.risk_score}점 · {a.action}</span>
            </div>
            <div className="a-msg">{a.reason}</div>
            <div className="a-target">대상 지갑: {a.target_wallet}</div>
          </li>
        ))}
      </ul>
    </article>
  );
}
