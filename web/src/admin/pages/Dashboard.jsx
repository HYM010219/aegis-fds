/* S4 집계 대시보드 — "지금 전체 상황은 어떤가?" */
import { useState } from "react";
import { ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis, Tooltip, CartesianGrid, Legend } from "recharts";
import { useStats } from "../data/hooks.js";
import { SCORECARD } from "../data/store.js";
import { R_RULES, L_RULES } from "../data/meta.js";
import { navigate } from "../router.js";
import { Card, Kpi, Seg, Grade, Status, Rule, Avatar, Addr, OriginTag, Money, Spark, usd, Empty, dateTime } from "../ui.jsx";

const RANGES = [["1h", "1시간"], ["24h", "24시간"], ["14d", "2주"]];
const RANGE_TEXT = { "1h": "최근 1시간", "24h": "최근 24시간", "14d": "최근 2주" };

function tickLabel(range) {
  return (t) => {
    const d = new Date(t);
    if (range === "14d") return `${d.getMonth() + 1}/${d.getDate()}`;
    return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
  };
}

function FlowChart({ s, range }) {
  const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  const fmt = tickLabel(range);
  return (
    <ResponsiveContainer width="100%" height={430}>
      <ComposedChart data={s.series} margin={{ top: 8, right: 4, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="gIn" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stopColor={css("--accent")} stopOpacity={0.35} />
            <stop offset="1" stopColor={css("--accent")} stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid stroke={css("--line")} vertical={false} />
        <XAxis dataKey="t" tickFormatter={fmt} stroke={css("--dim")} fontSize={12} minTickGap={40} />
        <YAxis yAxisId="in" stroke={css("--dim")} fontSize={12} width={56} tickFormatter={(v) => (v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : v >= 1000 ? `${Math.round(v / 1000)}k` : v)} />
        <YAxis yAxisId="ev" orientation="right" stroke={css("--dim")} fontSize={12} width={34} allowDecimals={false} />
        <Tooltip
          contentStyle={{ background: css("--panel"), border: `1px solid ${css("--line-2")}`, borderRadius: 10, fontSize: 13 }}
          labelFormatter={(t) => dateTime(t)}
          formatter={(v, n) => [v.toLocaleString() + "건", n]}
        />
        <Legend wrapperStyle={{ fontSize: 13 }} />
        <Area yAxisId="in" type="monotone" dataKey="inflow" name="유입 거래 (왼쪽 축)" stroke={css("--accent")} fill="url(#gIn)" strokeWidth={2} isAnimationActive={false} />
        <Line yAxisId="ev" type="monotone" dataKey="hold" name="보류" stroke={css("--hold")} dot={false} strokeWidth={2} isAnimationActive={false} />
        <Line yAxisId="ev" type="monotone" dataKey="block" name="차단" stroke={css("--block")} dot={false} strokeWidth={2} isAnimationActive={false} />
        <Line yAxisId="ev" type="monotone" dataKey="freeze" name="자동 동결" stroke={css("--frozen")} dot={false} strokeWidth={2} isAnimationActive={false} />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

function RuleBars({ counts }) {
  const rows = [
    ...Object.entries(R_RULES).map(([id]) => ({ id, n: counts[id] || 0, tone: id === "R07" ? "block" : "hold" })),
    ...Object.entries(L_RULES).map(([id, r]) => ({ id, n: counts[id] || 0, tone: r.action === "AUTO" ? "frozen" : "review" })),
  ];
  const max = Math.max(1, ...rows.map((r) => r.n));
  return (
    <div className="rulebars">
      <div className="rb-group">1단계 · 게이트웨이 (5분)</div>
      {rows.slice(0, 7).map((r) => <Bar key={r.id} r={r} max={max} />)}
      <div className="rb-group">2단계 · Spark (2주)</div>
      {rows.slice(7).map((r) => <Bar key={r.id} r={r} max={max} />)}
    </div>
  );
}
const Bar = ({ r, max }) => (
  <button className="rb" onClick={() => navigate(r.id.startsWith("L") ? `/alerts?rule=${r.id}` : `/transfers?rule=${r.id}`)} title="눌러서 해당 목록 보기">
    <Rule id={r.id} />
    <span className="rb-track"><span className={`rb-fill t-${r.tone}`} style={{ width: `${(r.n / max) * 100}%` }} /></span>
    <span className="rb-n mono">{r.n}</span>
  </button>
);

export default function Dashboard() {
  const [range, setRange] = useState("24h");
  const { data: s } = useStats(range);
  if (!s) return <div className="boot">불러오는 중…</div>;
  const k = s.kpi;
  const crit = s.grades.find((g) => g.grade === "CRITICAL")?.n || 0;

  return (
    <>
      <div className="page-h">
        <div>
          <h1>집계 대시보드</h1>
          <p>시스템 전체 상황을 한눈에 봅니다. 숫자를 누르면 해당 목록으로 이동합니다.</p>
        </div>
        <Seg value={range} options={RANGES} onChange={setRange} />
      </div>

      {k.pending > 0 ? (
        <div className="todo">
          <span className="todo-ic">!</span>
          <div><b>검토 대기 사건 {k.pending}건</b>이 담당자 판단을 기다리고 있습니다.{crit > 0 && <> <b className="c-crit">CRITICAL {crit}건</b>부터 확인하세요.</>}</div>
          <button className="btn primary" onClick={() => navigate("/alerts?status=PENDING")}>검토 대기 목록 열기 →</button>
        </div>
      ) : (
        <div className="todo ok"><span className="todo-ic">✓</span><div>검토 대기 사건이 없습니다.</div></div>
      )}

      <div className="grid-2">
        <Card title="경보 현황" desc={`2단계 분석(Spark) 사건 · ${RANGE_TEXT[range]} (검토 대기는 현재 남은 수)`}>
          <div className="kpis k4">
            <Kpi label="검토 대기" value={k.pending} tone="review" desc="사람이 판단해야 할 사건" spark={s.sparks.pending} onClick={() => navigate("/alerts?status=PENDING")} />
            <Kpi label="자동 동결" value={k.autoFrozen} tone="frozen" desc="L1·L2·L3·L8 즉시 동결" spark={s.sparks.auto} onClick={() => navigate("/alerts?status=AUTO_FROZEN")} />
            <Kpi label="직원 동결" value={k.staffFrozen} tone="staff" desc="검토 후 담당자가 동결" onClick={() => navigate("/alerts?status=FROZEN")} />
            <Kpi label="오탐 처리" value={k.dismissed} tone="muted" desc="검토 결과 정상으로 종결" spark={s.sparks.dismissed} onClick={() => navigate("/alerts?status=DISMISSED")} />
          </div>
        </Card>
        <Card title="게이트웨이 (1단계)" desc={`입구 검사 결과 · ${RANGE_TEXT[range]}`}>
          <div className="kpis k4">
            <Kpi label="유입" value={k.inflowPerMin} sub="건/분" tone="accent" desc="메인넷 + 시뮬레이터" onClick={() => navigate("/transfers")} />
            <Kpi label="보류 R01~R06" value={k.holds} tone="hold" desc="3분 동안 송금만 막음" spark={s.sparks.holds} onClick={() => navigate("/transfers?result=HOLD")} />
            <Kpi label="차단 R07" value={k.blocks} tone="block" desc="제재 지갑 · 403" spark={s.sparks.blocks} onClick={() => navigate("/transfers?result=BLOCK")} />
            <Kpi label="동결 지갑 송금 시도" value={k.frozenTries} tone="frozen" desc="동결 후에도 보내려 함 · 403" onClick={() => navigate("/transfers?result=FROZEN")} />
          </div>
        </Card>
      </div>

      <div className="grid-main">
        <Card title="거래 흐름" desc={`${range === "1h" ? "1분" : range === "24h" ? "30분" : "하루"} 단위 · 유입은 왼쪽 축, 보류·차단·동결은 오른쪽 축 (단위가 달라 나눠 표시)${s.oldNote ? " · " + s.oldNote : ""}`}>
          <FlowChart s={s} range={range} />
        </Card>
        <Card title="룰별 건수" desc={`${range === "14d" ? "최근 24시간 기록" : RANGE_TEXT[range]} · 주황 보류 · 보라 차단 · 빨강 자동 동결 · 노랑 검토 대기`}>
          <RuleBars counts={s.ruleCounts} />
        </Card>
      </div>

      <div className="grid-main">
        <Card title="위험 지갑 상위" desc="검토 대기 → 등급 → 금액 순. 줄을 누르면 사건 상세로 이동" right={<a href="#/alerts">경보 전체 →</a>} pad={false}>
          {s.top.length === 0 ? <Empty>해당 기간에 사건이 없습니다.</Empty> : (
            <div className="tbl-wrap">
              <table className="tbl">
                <thead><tr><th>지갑</th><th>등급</th><th>걸린 룰</th><th>최근 24시간 활동</th><th className="r">관련 금액</th><th>상태</th></tr></thead>
                <tbody>
                  {s.top.map((c) => (
                    <tr key={c.case_id} className="clickable" onClick={() => navigate(`/cases/${c.case_id}`)}>
                      <td><div className="who"><Avatar origin={c.origin} /><Addr a={c.violator_address} /><OriginTag origin={c.origin} /></div></td>
                      <td><Grade g={c.risk_level} /></td>
                      <td className="mono">{c.rules.join(" · ")}</td>
                      <td className="c-accent"><Spark data={c.spark} w={96} /></td>
                      <td className="r"><Money v={c.amount} /></td>
                      <td><Status s={c.status} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
        <Card title={<>검증 성적표 <span className="pill-warn">예시</span></>} desc="봇 정답 라벨(fds.ground_truth.v1)과 비교한 결과. 정답 라벨 연결 전이라 예시 값입니다." pad={false}>
          <table className="tbl">
            <thead><tr><th>룰</th><th className="r" title="실제 공격 중 잡아낸 비율 (높을수록 좋음)">재현율</th><th className="r" title="정상 거래를 잘못 잡은 비율 (낮을수록 좋음)">오탐률</th></tr></thead>
            <tbody>
              {SCORECARD.map((r) => (
                <tr key={r.rule}>
                  <td><Rule id={r.rule} /></td>
                  <td className="r mono c-ok">{r.recall.toFixed(2)}</td>
                  <td className={`r mono ${r.fpr >= 0.1 ? "c-hold" : "c-muted"}`}>{r.fpr.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="note">재현율 = 실제 공격 중 잡아낸 비율 · 오탐률 = 정상 거래를 잘못 잡은 비율. 메인넷 거래는 정답이 없어 제외합니다.</p>
        </Card>
      </div>
      <p className="note center">유입 {usd(k.inflowTotal, 0)}건 기준 · {RANGE_TEXT[range]}</p>
    </>
  );
}
