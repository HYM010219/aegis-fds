/* 백엔드 시스템 상태 — 파이프라인 구성 요소가 살아 있는가 */
import { useHealth } from "../data/hooks.js";
import { useChain } from "../../hooks/useChain.js";
import { Card, ago, useNow } from "../ui.jsx";

const ST = {
  UP: ["정상", "ok"], WARN: ["주의", "hold"], DOWN: ["중단", "frozen"], MOCK: ["Mock", "block"],
};

export default function System() {
  const now = useNow(1000);
  const { data: h } = useHealth();
  const chain = useChain(null);
  const st = chain.status;
  if (!h) return <div className="boot">불러오는 중…</div>;
  const counts = h.services.reduce((m, s) => ({ ...m, [s.status]: (m[s.status] || 0) + 1 }), {});
  const rpc = st.source === "LIVE" ? "UP" : st.source === "SIM" ? "WARN" : st.source === "ERROR" ? "DOWN" : "WARN";

  return (
    <>
      <div className="page-h">
        <div>
          <h1>시스템 상태</h1>
          <p>거래 수집부터 분석·저장까지 각 구성 요소가 정상인지 봅니다. 5초마다 새로 고칩니다.</p>
        </div>
        <div className="sys-sum">
          <span className="c-ok">정상 {counts.UP || 0}</span><span className="c-hold">주의 {counts.WARN || 0}</span><span className="c-frozen">중단 {counts.DOWN || 0}</span>
        </div>
      </div>
      {h.mock && (
        <div className="info-box warn">
          <b>백엔드 연결 전입니다.</b> 아래 게이트웨이·Redis·Kafka·Spark·MongoDB 값은 화면 확인용 예시입니다.
          <b> 메인넷 RPC</b> 한 칸만 이 브라우저에서 실제로 확인한 값입니다. 연결 후에는 <code>GET /api/v1/admin/health</code> 응답을 그대로 보여 줍니다.
        </div>
      )}

      <div className="pipeline">
        {["수집기", "게이트웨이", "Kafka", "Spark", "MongoDB", "관제 화면"].map((s, i, a) => (
          <span key={s}><b>{s}</b>{i < a.length - 1 && <i>→</i>}</span>
        ))}
      </div>

      <div className="svc-grid">
        <div className={`svc s-${ST[rpc][1]} real`}>
          <div className="svc-h"><b>메인넷 RPC</b><span className={`svc-st c-${ST[rpc][1]}`}>● {ST[rpc][0]}</span></div>
          <div className="svc-role">이더리움 공개 노드 · <b>실제 확인</b></div>
          <div className="svc-m mono">{st.block ? `블록 #${st.block.toLocaleString()}` : "연결 중…"}{st.gasGwei ? ` · 가스 ${st.gasGwei.toFixed(2)} gwei` : ""}</div>
          <div className="svc-f">{st.source === "LIVE" ? "LIVE 수신 중" : st.source === "SIM" ? "RPC 실패 → 시뮬레이션 대체" : "확인 중"}</div>
        </div>
        {h.services.map((s) => (
          <div key={s.id} className={`svc s-${ST[s.status][1]}`}>
            <div className="svc-h"><b>{s.name}</b><span className={`svc-st c-${ST[s.status][1]}`}>● {ST[s.status][0]}</span></div>
            <div className="svc-role">{s.role}</div>
            <div className="svc-m mono">{s.metric}</div>
            <div className="svc-f">{s.latency != null ? `응답 ${s.latency} ms · ` : ""}{s.last ? `마지막 신호 ${ago(s.last, now)}` : "신호 없음"}</div>
          </div>
        ))}
      </div>

      <Card title="Kafka 토픽" desc="각 3 파티션 · lag = 아직 처리하지 못하고 쌓인 메시지 수 (0에 가까울수록 좋음)" pad={false}>
        <table className="tbl">
          <thead><tr><th>토픽</th><th>내용</th><th className="r">초당 메시지</th><th className="r">lag</th></tr></thead>
          <tbody>
            {h.topics.map((t) => (
              <tr key={t.topic}>
                <td className="mono">{t.topic}</td>
                <td>{t.desc}</td>
                <td className="r mono">{(t.rate / 60).toFixed(1)}</td>
                <td className={`r mono ${t.lag > 100 ? "c-hold" : "c-ok"}`}>{t.lag}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </>
  );
}
