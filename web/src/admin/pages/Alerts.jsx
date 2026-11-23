/* S1 경보 목록 — "무엇부터 처리할까?" */
import { useEffect, useMemo, useState } from "react";
import { useReactTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel, getPaginationRowModel, flexRender } from "@tanstack/react-table";
import { useCases, useStats } from "../data/hooks.js";
import { L_RULES, ORIGINS, GRADES, STATUS, L5_TYPES } from "../data/meta.js";
import { navigate } from "../router.js";
import { Card, Kpi, Grade, Status, Rule, Avatar, Addr, OriginTag, Money, Ago, EvidenceBar, Empty, Seg, useNow } from "../ui.jsx";

const QUICK = [
  ["ALL", "전체"], ["PENDING", "검토 대기"], ["CRITICAL", "CRITICAL"], ["HIGH", "HIGH"], ["MEDIUM", "MEDIUM"],
  ["FROZEN_ANY", "동결"], ["DISMISSED", "오탐 처리"],
];
const quickTest = {
  ALL: () => true,
  PENDING: (c) => c.status === "PENDING",
  CRITICAL: (c) => c.risk_level === "CRITICAL",
  HIGH: (c) => c.risk_level === "HIGH",
  MEDIUM: (c) => c.risk_level === "MEDIUM",
  FROZEN_ANY: (c) => c.status === "AUTO_FROZEN" || c.status === "FROZEN",
  AUTO_FROZEN: (c) => c.status === "AUTO_FROZEN",
  FROZEN: (c) => c.status === "FROZEN",
  DISMISSED: (c) => c.status === "DISMISSED" || c.status === "RELEASED",
};

function Preview({ c, now }) {
  if (!c) return <Card title="선택한 경보 미리보기"><Empty>표에서 경보를 누르면 여기에 요약이 나옵니다.</Empty></Card>;
  const r = L_RULES[c.rule_id];
  return (
    <Card title="선택한 경보 미리보기" className="preview">
      <div className="pv-who">
        <Avatar origin={c.origin} size={38} />
        <div>
          <Addr a={c.violator_address} copyable h={10} t={6} />
          <div className="tags"><OriginTag origin={c.origin} isNew={c.isNew} /></div>
        </div>
      </div>
      <div className="tags"><Grade g={c.risk_level} /><Status s={c.status} /></div>
      <div className="pv-rule"><Rule id={c.rule_id} plain /></div>
      <div className="pv-head">{c.headline}</div>
      <div className="pv-ev">
        {c.evidence.filter((e) => e.value > 0 || c.rule_id !== "L5").slice(0, 2).map((e) => <EvidenceBar key={e.label} item={e} compact />)}
      </div>
      <dl className="pv-meta">
        <dt>조치 구분</dt><dd>{r.action === "AUTO" ? "자동 동결 대상" : "담당자 검토 대상"}</dd>
        <dt>탐지</dt><dd><Ago ts={c.detected_at} now={now} /> · {c.case_id}</dd>
        <dt>관련 금액</dt><dd><Money v={c.amount} /></dd>
      </dl>
      <div className="pv-btns">
        <button className="btn primary" onClick={() => navigate(`/cases/${c.case_id}`)}>사건 열기</button>
        <button className="btn" onClick={() => navigate(`/flow/${c.case_id}`)}>자금 흐름</button>
      </div>
    </Card>
  );
}

export default function Alerts({ query = {} }) {
  const now = useNow(1000);
  const { data: all = [] } = useCases();
  const { data: s } = useStats("24h");
  const [quick, setQuick] = useState(query.status || "ALL");
  const [rule, setRule] = useState(query.rule || "ALL");
  const [source, setSource] = useState("ALL");
  const [minAmt, setMinAmt] = useState(0);
  const [period, setPeriod] = useState("24h");
  const [text, setText] = useState("");
  const [sorting, setSorting] = useState([{ id: "detected_at", desc: true }]);
  const [selId, setSelId] = useState(null);

  useEffect(() => { if (query.status) setQuick(query.status); if (query.rule) setRule(query.rule); }, [query.status, query.rule]);

  const rows = useMemo(() => {
    const since = { "1h": now - 36e5, "24h": now - 864e5, all: 0 }[period];
    const q = text.trim().toLowerCase();
    return all.filter((c) =>
      (quickTest[quick] || quickTest.ALL)(c) &&
      (rule === "ALL" || c.rule_id === rule) &&
      (source === "ALL" || ORIGINS[c.origin]?.source === source || (source === "앱" && c.app)) &&
      c.amount >= minAmt && c.detected_at >= since &&
      (!q || c.violator_address.includes(q) || c.case_id.toLowerCase().includes(q) || c.rule_id.toLowerCase() === q),
    );
    // now 는 1초마다 바뀌지만 기간 필터에만 쓰므로 목록이 바뀔 때만 다시 계산
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [all, quick, rule, source, minAmt, period, text]);

  const columns = useMemo(() => [
    { id: "detected_at", accessorKey: "detected_at", header: "탐지", cell: (i) => <Ago ts={i.getValue()} now={Date.now()} />, size: 84 },
    { id: "grade", accessorFn: (c) => GRADES[c.risk_level].order, header: "등급", cell: (i) => <Grade g={i.row.original.risk_level} />, size: 96 },
    { id: "rule", accessorKey: "rule_id", header: "룰", cell: (i) => <Rule id={i.getValue()} /> },
    {
      id: "wallet", accessorKey: "violator_address", header: "대상 지갑", enableSorting: false,
      cell: (i) => { const c = i.row.original; return <div className="who"><Avatar origin={c.origin} /><Addr a={c.violator_address} /><OriginTag origin={c.origin} isNew={c.isNew} /></div>; },
    },
    { id: "cp", accessorKey: "counterpart", header: "상대 · 내용", enableSorting: false, cell: (i) => <span className="c-muted wrap">{i.getValue()}</span> },
    { id: "amount", accessorKey: "amount", header: "관련 금액", cell: (i) => <Money v={i.getValue()} unit={false} />, meta: { right: true } },
    { id: "source", accessorFn: (c) => (c.app ? "앱" : ORIGINS[c.origin]?.source), header: "출처", cell: (i) => <span className="c-muted">{i.getValue()}</span> },
    { id: "status", accessorFn: (c) => ["PENDING", "AUTO_FROZEN", "FROZEN", "DISMISSED", "RELEASED"].indexOf(c.status), header: "상태", cell: (i) => <Status s={i.row.original.status} /> },
  ], []);

  const table = useReactTable({
    data: rows, columns, state: { sorting }, onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel(), getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(), initialState: { pagination: { pageSize: 12 } }, autoResetPageIndex: false,
  });
  const sel = all.find((c) => c.case_id === selId) || rows[0];
  const k = s?.kpi;
  const grades = s?.grades || [];
  const gTotal = grades.reduce((a, g) => a + g.n, 0);
  const pg = table.getState().pagination;
  const pageCount = Math.max(1, table.getPageCount());
  useEffect(() => { if (pg.pageIndex >= pageCount) table.setPageIndex(0); }, [pg.pageIndex, pageCount, table]);

  return (
    <>
      <div className="page-h">
        <div>
          <h1>경보 목록</h1>
          <p>2단계 분석(Spark)이 찾은 사건입니다. <b>노란색 "검토 대기"</b>가 담당자가 처리할 일입니다.</p>
        </div>
      </div>

      {k && (
        <div className="kpis k5">
          <Kpi label="검토 대기" value={k.pending} tone="review" desc="L4~L7 · 담당자 판단 필요" spark={s.sparks.pending} onClick={() => setQuick("PENDING")} />
          <Kpi label="자동 동결 (24시간)" value={k.autoFrozen} tone="frozen" desc="L1 · L2 · L3 · L8" spark={s.sparks.auto} onClick={() => setQuick("AUTO_FROZEN")} />
          <Kpi label="보류 (24시간)" value={k.holds} tone="hold" desc="1단계 R01~R06 · 3분" spark={s.sparks.holds} onClick={() => navigate("/transfers?result=HOLD")} />
          <Kpi label="차단 (24시간)" value={k.blocks} tone="block" desc="R07 블랙리스트 · 403" spark={s.sparks.blocks} onClick={() => navigate("/transfers?result=BLOCK")} />
          <Kpi label="오탐 처리 (24시간)" value={k.dismissed} tone="muted" desc="정상으로 종결" spark={s.sparks.dismissed} onClick={() => setQuick("DISMISSED")} />
        </div>
      )}

      {gTotal > 0 && (
        <div className="gradebar" title="아직 처리하지 않은(검토 대기) 경보의 등급 비율">
          <span className="gb-label">검토 대기 등급 분포</span>
          <div className="gb-track">
            {grades.map((g) => g.n > 0 && <span key={g.grade} className={`gb-seg g-${g.grade}`} style={{ flex: g.n }} />)}
          </div>
          <span className="gb-legend">{grades.map((g) => <span key={g.grade} className={`c-${g.grade.toLowerCase()}`}>{g.grade} {g.n}</span>)}</span>
        </div>
      )}

      <div className="filters">
        <Seg value={quick} options={QUICK} onChange={setQuick} />
        <div className="f-right">
          <input className="inp" placeholder="주소 · 사건 ID · 룰 코드" value={text} onChange={(e) => setText(e.target.value)} />
          <select className="inp" value={rule} onChange={(e) => setRule(e.target.value)} aria-label="룰">
            <option value="ALL">룰 전체</option>
            {Object.entries(L_RULES).map(([id, r]) => <option key={id} value={id}>{id} {r.name}</option>)}
          </select>
          <select className="inp" value={source} onChange={(e) => setSource(e.target.value)} aria-label="출처">
            <option value="ALL">출처 전체</option><option value="온체인">온체인</option><option value="SIM">시뮬레이터</option><option value="앱">고객 앱</option>
          </select>
          <select className="inp" value={minAmt} onChange={(e) => setMinAmt(Number(e.target.value))} aria-label="금액">
            <option value={0}>금액 전체</option><option value={1000}>1,000 USDT 이상</option><option value={10000}>10,000 USDT 이상</option>
          </select>
          <select className="inp" value={period} onChange={(e) => setPeriod(e.target.value)} aria-label="기간">
            <option value="1h">최근 1시간</option><option value="24h">최근 24시간</option><option value="all">전체</option>
          </select>
        </div>
      </div>

      <div className="split">
        <Card pad={false} className="tbl-card">
          {rows.length === 0 ? <Empty>조건에 맞는 경보가 없습니다. 필터를 바꿔 보세요.</Empty> : (
            <div className="tbl-wrap">
              <table className="tbl">
                <thead>
                  {table.getHeaderGroups().map((hg) => (
                    <tr key={hg.id}>
                      {hg.headers.map((h) => (
                        <th key={h.id} className={`${h.column.columnDef.meta?.right ? "r" : ""} ${h.column.getCanSort() ? "sortable" : ""}`} onClick={h.column.getToggleSortingHandler()}>
                          {flexRender(h.column.columnDef.header, h.getContext())}
                          {{ asc: " ▲", desc: " ▼" }[h.column.getIsSorted()] ?? ""}
                        </th>
                      ))}
                    </tr>
                  ))}
                </thead>
                <tbody>
                  {table.getRowModel().rows.map((r) => {
                    const c = r.original;
                    const fresh = now - c.detected_at < 8000;
                    return (
                      <tr key={r.id} tabIndex={0}
                        className={`clickable ${sel?.case_id === c.case_id ? "sel" : ""} ${fresh ? "fresh" : ""} ${c.status === "PENDING" ? "pending" : ""}`}
                        onClick={() => setSelId(c.case_id)}
                        onDoubleClick={() => navigate(`/cases/${c.case_id}`)}
                        onKeyDown={(e) => e.key === "Enter" && navigate(`/cases/${c.case_id}`)}>
                        {r.getVisibleCells().map((cell) => (
                          <td key={cell.id} className={cell.column.columnDef.meta?.right ? "r" : ""}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                        ))}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          <div className="tbl-foot">
            <span>한 번 누르면 미리보기, 두 번 누르면(또는 Enter) 사건 상세 · 새 경보는 잠깐 강조됩니다</span>
            <span className="pager">
              {rows.length ? `${pg.pageIndex * pg.pageSize + 1}–${Math.min(rows.length, (pg.pageIndex + 1) * pg.pageSize)} / ${rows.length}` : "0"}
              <button className="btn sm" disabled={!table.getCanPreviousPage()} onClick={() => table.previousPage()}>이전</button>
              <button className="btn sm" disabled={!table.getCanNextPage()} onClick={() => table.nextPage()}>다음</button>
            </span>
          </div>
        </Card>
        <Preview c={sel} now={now} />
      </div>
      <p className="note">
        등급: CRITICAL(긴급) · HIGH(높음) · MEDIUM(보통). L5는 유형별 — {Object.entries(L5_TYPES).map(([g, t]) => `${g} ${t}`).join(" / ")}.
        {" "}상태: {Object.values(STATUS).map((x) => x.label).join(" · ")}. 주소에 마우스를 올리면 전체 주소가 보입니다.
      </p>
    </>
  );
}
