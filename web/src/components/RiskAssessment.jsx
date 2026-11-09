const BADGE = { PASS: ["Pass", "sev-low"], WARN: ["Watch", "sev-mid"], HOLD: ["Hold", "sev-mid"], BLOCK: ["Block", "sev-high"] };
const LEVEL = { SETTLED: ["Low", "sev-low"], PENDING_REVIEW: ["Medium", "sev-mid"], REJECTED: ["High", "sev-high"], DUPLICATE: ["High", "sev-high"] };
const MSG = {
  SETTLED: "모든 입구 검문을 통과하여 송금이 확정되었습니다. 송금 이벤트는 Stream-Path(1~2초)에서 추가 분석됩니다.",
  PENDING_REVIEW: "고위험 조건에 해당하여 출금액이 예약(reserved) 상태로 보류되었고, 관제 담당자 검토 후 확정됩니다.",
  REJECTED: "Fast-Path 입구 검문에서 차단되었습니다. 원장에 기록되지 않았으며 잔액은 변하지 않습니다.",
  DUPLICATE: "동일 Idempotency-Key 재전송이 감지되어 최초 처리 결과를 그대로 반환했습니다(R01).",
};
const READY = [
  { id: "BLACKLIST", icon: "⛔", title: "FROZEN / BLACKLIST", desc: "Redis 동결 목록 O(1) 조회" },
  { id: "R01", icon: "⧉", title: "R01 DOUBLE_SPEND", desc: "Idempotency-Key 중복 검사" },
  { id: "R04", icon: "⇲", title: "R04 ACCOUNT_DRAIN", desc: "잔액 소진율 ≥ 95% & 최초/미신고 지갑" },
  { id: "TRAVEL", icon: "⚖", title: "TRAVEL RULE", desc: "1,000 USDT 이상 송수신인 정보 공유" },
];

/** assessment: null(대기) | { checks, decision, risk } */
export default function RiskAssessment({ assessment }) {
  const [lvl, lvlCls] = assessment ? LEVEL[assessment.decision] || ["—", "sev-none"] : ["대기", "sev-none"];
  const checks = assessment ? assessment.checks : READY;
  return (
    <article className="card">
      <div className="card-head">
        <h2>Risk Assessment <small>Fast-Path 사전 검증</small></h2>
        <span className={`sev ${lvlCls}`}>{lvl}</span>
      </div>
      <p className="muted">
        {assessment
          ? `${MSG[assessment.decision] || ""} (요청 위험점수 ${assessment.risk})`
          : "송금을 요청하면 0.5ms 입구 검문(Fast-Path) 결과가 항목별로 표시됩니다."}
      </p>
      <ul className="checks">
        {checks.map((c) => {
          const [label, cls] = assessment ? BADGE[c.result] : ["Ready", "sev-none"];
          return (
            <li key={c.id}>
              <div className="ck-left">
                <span className="ck-ico">{c.icon}</span>
                <div><div className="ck-title">{c.title}</div><div className="ck-desc">{c.desc}</div></div>
              </div>
              <span className={`sev ${cls}`}>{label}</span>
            </li>
          );
        })}
      </ul>
    </article>
  );
}
