/* S3 자금 흐름 — "돈이 어디로 흘러갔나?" (React Flow) */
import { useEffect, useMemo, useState } from "react";
import { ReactFlow, Background, Controls, Handle, Position, MarkerType, BaseEdge, EdgeLabelRenderer } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useCases, useGraph } from "../data/hooks.js";
import { L_RULES, ORIGINS, DAY_LABEL } from "../data/meta.js";
import { navigate } from "../router.js";
import { Card, Grade, Status, Rule, Addr, Money, Seg, Empty, usd, short, clock } from "../ui.jsx";

/* 지갑 노드 모양 */
function WalletNode({ data }) {
  const cls = [
    "wnode",
    data.center && "center",
    data.onPath && "path",
    data.sdn && "sdn",
    data.frozen && !data.center && "frozen",
    data.selected && "selected",
  ].filter(Boolean).join(" ");
  return (
    <div className={cls} title={data.address}>
      <Handle type="target" position={Position.Top} className="h" />
      <div className="wn-circle">{data.letter}</div>
      <div className="wn-label mono">{short(data.address, 5, 4)}</div>
      {data.sub && <div className="wn-sub">{data.sub}</div>}
      <Handle type="source" position={Position.Top} className="h" />
    </div>
  );
}
const nodeTypes = { wallet: WalletNode };

/** 송금 선: 원의 가장자리에서 가장자리까지 직선 + 화살표 (손잡이는 원 중심에 숨김) */
function MoneyEdge({ id, sourceX, sourceY, targetX, targetY, markerEnd, style, label, data }) {
  const dx = targetX - sourceX, dy = targetY - sourceY, len = Math.hypot(dx, dy) || 1;
  const x1 = sourceX + (dx / len) * data.rs, y1 = sourceY + (dy / len) * data.rs;
  const x2 = targetX - (dx / len) * (data.rt + 3), y2 = targetY - (dy / len) * (data.rt + 3);
  return (
    <>
      <BaseEdge id={id} path={`M${x1},${y1} L${x2},${y2}`} markerEnd={markerEnd} style={style} />
      <EdgeLabelRenderer>
        <div className={`edge-lbl ${data.hit ? "hit" : ""}`} style={{ transform: `translate(-50%, -50%) translate(${(x1 + x2) / 2}px, ${(y1 + y2) / 2}px)` }}>{label}</div>
      </EdgeLabelRenderer>
    </>
  );
}
const edgeTypes = { money: MoneyEdge };

const LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/** 룰에 걸린 경로의 지갑 순서 (A, B, C… 글자를 붙일 순서)
 *  1) 경로 근거가 있으면 그 순서 (Mock: cycle / 백엔드: evidence_raw.path·flow_path·cycle_path)
 *  2) 없으면 대상 지갑을 A로 두고, 룰에 걸린 거래를 시간순으로 따라가며 나오는 지갑 순서 */
function pathOrder(c, edges = []) {
  const raw = c.evidence_raw || {};
  const given = [c.cycle, raw.path, raw.flow_path, raw.cycle_path].find((p) => Array.isArray(p) && p.length);
  const seq = given ? [...given] : [c.violator_address, ...edges.filter((e) => e.hit).sort((a, b) => (a.ts || 0) - (b.ts || 0)).flatMap((e) => [e.from, e.to])];
  return [...new Set(seq.filter(Boolean))]; // A→B→C→A처럼 처음 지갑이 다시 나오면 한 번만
}

/** 배치: 대상 지갑을 가운데, 룰에 걸린 경로를 안쪽 원, 나머지를 바깥 원에 */
function layout(g, selected) {
  const center = g.nodes.find((n) => n.center);
  const cyc = pathOrder(g.case, g.edges);
  const inner = g.nodes.filter((n) => !n.center && (n.onPath || cyc.includes(n.address)))
    .sort((a, b) => (cyc.indexOf(a.address) + 1 || 99) - (cyc.indexOf(b.address) + 1 || 99)); // 경로 순서대로 돌아가며 배치
  const outer = g.nodes.filter((n) => !n.center && !inner.includes(n));
  const pos = {};
  pos[center.address] = { x: 0, y: 0 };
  inner.forEach((n, i) => { const a = -Math.PI / 3 + (i / Math.max(1, inner.length)) * Math.PI * 2 * 0.72; pos[n.address] = { x: Math.cos(a) * 240, y: Math.sin(a) * 200 }; });
  outer.forEach((n, i) => { const a = Math.PI * 0.62 + (i / Math.max(1, outer.length)) * Math.PI * 2; const r = n.level > 1 ? 470 : 360; pos[n.address] = { x: Math.cos(a) * r, y: Math.sin(a) * r * 0.75 }; });
  const letters = {};
  cyc.forEach((a, i) => { letters[a] = LETTERS[i]; });
  const nodes = g.nodes.map((n) => ({
    id: n.address, type: "wallet", position: pos[n.address] || { x: 0, y: 0 },
    data: {
      ...n, selected: selected === n.address,
      letter: letters[n.address] || (n.center ? "A" : n.sdn ? "!" : n.frozen ? "🔒" : ORIGINS[n.origin]?.short || "•"),
      sub: n.center ? `대상 · ${g.case.status === "AUTO_FROZEN" ? "자동 동결" : g.case.status === "FROZEN" ? "직원 동결" : "검토 대상"}` : n.sdn ? "OFAC SDN" : n.frozen ? "동결 지갑" : n.label || null,
    },
  }));
  const maxAmt = Math.max(1, ...g.edges.map((e) => e.amount));
  const radius = (a) => (a === center.address ? 29 : 22);
  const edges = g.edges.map((e) => {
    return {
    id: `${e.from}>${e.to}`, source: e.from, target: e.to, type: "money",
    label: usd(e.amount, 0), animated: e.hit,
    markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18, markerUnits: "userSpaceOnUse", color: e.hit ? "#f87171" : "#5f6b80" },
    style: { stroke: e.hit ? "var(--frozen)" : "var(--line-2)", strokeWidth: 1.2 + (e.amount / maxAmt) * 5 },
    data: { ...e, rs: radius(e.from), rt: radius(e.to) },
    };
  });
  return { nodes, edges };
}

export default function Flow({ caseId }) {
  const { data: cases = [] } = useCases();
  const id = caseId || cases.find((c) => c.rule_id === "L8")?.case_id || cases[0]?.case_id;
  const [depth, setDepth] = useState(1);
  const [minAmount, setMin] = useState(0);
  const [expand, setExpand] = useState([]);
  const [sel, setSel] = useState(null);
  const [win, setWin] = useState(null); // [from, to] 0~100%
  useEffect(() => { setExpand([]); setSel(null); setWin(null); }, [id]);

  const { data: base } = useGraph(id, { depth, minAmount, expand });
  const range = base?.window;
  const filtered = useMemo(() => {
    if (!base) return null;
    if (!win) return base;
    const span = range.to - range.from;
    const a = range.from + (win[0] / 100) * span, b = range.from + (win[1] / 100) * span;
    const edges = base.edges.filter((e) => e.ts >= a && e.ts <= b);
    const keep = new Set([base.case.violator_address, ...edges.flatMap((e) => [e.from, e.to])]);
    return { ...base, edges, nodes: base.nodes.filter((n) => keep.has(n.address)) };
  }, [base, win, range]);
  const { nodes, edges } = useMemo(() => (filtered ? layout(filtered, sel) : { nodes: [], edges: [] }), [filtered, sel]);

  if (!id) return <Empty>표시할 사건이 없습니다.</Empty>;
  if (!base) return <div className="boot">그래프를 만드는 중…</div>;
  const c = base.case;
  const r = L_RULES[c.rule_id];
  const selNode = base.nodes.find((n) => n.address === (sel || c.violator_address));
  const selEdges = base.edges.filter((e) => e.from === selNode?.address || e.to === selNode?.address);
  const pathTx = c.related.filter((t) => t.status === "ACCEPTED").sort((a, b) => a.ts - b.ts);
  const order = pathOrder(c, base.edges);
  const letter = (a) => { const i = order.indexOf(a); return i >= 0 ? LETTERS[i] : a === c.violator_address ? "대상" : short(a, 4, 3); };
  const span = range.to - range.from;
  const pct = (t) => ((t - range.from) / span) * 100;
  const w = win || [0, 100];

  return (
    <>
      <div className="page-h">
        <div>
          <h1>자금 흐름</h1>
          <p>지갑 사이에 돈이 어떻게 오갔는지 그림으로 봅니다. <b className="c-frozen">빨간 선</b>이 룰에 걸린 경로이고, 선이 굵을수록 금액이 큽니다.</p>
        </div>
        <select className="inp case-pick" value={id} onChange={(e) => navigate(`/flow/${e.target.value}`)} aria-label="사건 선택">
          {cases.slice(0, 80).map((x) => <option key={x.case_id} value={x.case_id}>{x.case_id} · {x.rule_id} {L_RULES[x.rule_id].name} · {short(x.violator_address)}</option>)}
        </select>
      </div>

      <div className="flow-layout">
        <aside className="card flow-side">
          <h3>펼침 범위</h3>
          <Seg value={depth} options={[[1, "1단계"], [2, "2단계"]]} onChange={(v) => { setDepth(v); setExpand([]); }} />
          <p className="note">1단계 = 대상과 직접 거래한 지갑 · 2단계 = 그 상대의 상대까지 (최대 30개)</p>
          <h3>거래 필터</h3>
          <select className="inp" value={minAmount} onChange={(e) => setMin(Number(e.target.value))}>
            <option value={0}>금액 전체</option><option value={100}>100 USDT 이상</option><option value={1000}>1,000 USDT 이상</option>
          </select>
          <p className="note">룰에 걸린 거래는 금액과 상관없이 항상 표시합니다.</p>
          <h3>범례</h3>
          <ul className="legend">
            <li><i className="lg center" />대상 지갑</li>
            <li><i className="lg path" />룰에 걸린 경로의 지갑</li>
            <li><i className="lg sdn" />OFAC SDN (제재 지갑)</li>
            <li><i className="lg frozen" />동결 지갑</li>
            <li><i className="lg other" />그 밖의 지갑</li>
            <li><i className="lg line" />룰에 걸린 거래 (움직이는 선)</li>
          </ul>
          <p className="note">그래프는 마우스로 끌어 옮기고, 휠로 확대·축소합니다. 지갑을 누르면 오른쪽에 정보가 나옵니다.</p>
        </aside>

        <section className="card flow-main">
          <div className="flow-canvas">
            <ReactFlow
              nodes={nodes} edges={edges} nodeTypes={nodeTypes} edgeTypes={edgeTypes} fitView fitViewOptions={{ padding: 0.2 }}
              onNodeClick={(_, n) => setSel(n.id)} onPaneClick={() => setSel(null)}
              nodesConnectable={false} minZoom={0.3}
            >
              <Background gap={22} size={1} color="var(--line)" />
              <Controls showInteractive={false} />
            </ReactFlow>
          </div>
          <div className="timebar">
            <div className="tb-h"><span>시간 범위 (양쪽 손잡이로 좁히기)</span><span className="mono">{clock(range.from + (w[0] / 100) * span)} → {clock(range.from + (w[1] / 100) * span)}</span></div>
            <div className="tb-track">
              <div className="tb-sel" style={{ left: `${w[0]}%`, width: `${w[1] - w[0]}%` }} />
              {base.hitTimes.map((t, i) => <span key={i} className="tb-hit" style={{ left: `${pct(t)}%` }} title={`룰에 걸린 거래 ${clock(t)}`} />)}
              <input type="range" min={0} max={100} value={w[0]} onChange={(e) => setWin([Math.min(Number(e.target.value), w[1] - 2), w[1]])} aria-label="시작 시각" />
              <input type="range" min={0} max={100} value={w[1]} onChange={(e) => setWin([w[0], Math.max(Number(e.target.value), w[0] + 2)])} aria-label="끝 시각" />
            </div>
            {win && <button className="link" onClick={() => setWin(null)}>전체 기간으로</button>}
          </div>
        </section>

        <aside className="col flow-info">
          <Card title="선택한 지갑">
            {selNode && (
              <>
                <div className="pv-who">
                  <span className={`wn-mini ${selNode.center ? "center" : selNode.sdn ? "sdn" : selNode.frozen ? "frozen" : ""}`}>{letter(selNode.address).slice(0, 2)}</span>
                  <div><Addr a={selNode.address} copyable h={10} t={6} /><div className="c-muted">{selNode.center ? "대상 지갑" : selNode.sdn ? "OFAC SDN 제재 지갑" : selNode.frozen ? "동결 지갑" : ORIGINS[selNode.origin]?.label}</div></div>
                </div>
                {!selNode.center && (
                  <ul className="mini-tx">
                    {selEdges.slice(0, 6).map((e) => <li key={e.from + e.to}><span>{letter(e.from)} → {letter(e.to)}</span><Money v={e.amount} unit={false} /></li>)}
                  </ul>
                )}
                <div className="pv-btns">
                  {!selNode.center && depth === 1 && !expand.includes(selNode.address) && <button className="btn" onClick={() => setExpand([...expand, selNode.address])}>이 지갑 펼치기</button>}
                  <button className="btn primary" onClick={() => navigate(`/cases/${c.case_id}`)}>사건 열기</button>
                </div>
              </>
            )}
          </Card>
          <Card title={<>{c.rule_id} 판정</>}>
            <div className="tags"><Grade g={c.risk_level} /><Status s={c.status} /></div>
            <div className="pv-rule"><Rule id={c.rule_id} plain /></div>
            <p className="pv-head">{c.headline}</p>
            {c.rule_id === "L8" && <p className="note">기준: {DAY_LABEL} 안에 한 바퀴, 각 송금 1,000 이상, 80% 이상 돌아옴</p>}
            <p className="note">{r.action === "AUTO" ? "자동 동결 룰" : "검토 대기 룰"}</p>
          </Card>
          <Card title="경로 거래" pad={false}>
            {pathTx.length === 0 ? <Empty>경로 거래 없음</Empty> : (
              <table className="tbl compact">
                <tbody>
                  {pathTx.slice(0, 10).map((t) => <tr key={t.tx_hash}><td className="mono c-muted">{clock(t.ts).slice(0, 5)}</td><td>{letter(t.from)} → {letter(t.to)}</td><td className="r"><Money v={t.amount} unit={false} /></td></tr>)}
                </tbody>
              </table>
            )}
          </Card>
        </aside>
      </div>
    </>
  );
}
