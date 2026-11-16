/* 실시간 거래 — 게이트웨이를 지나는 거래와 판정 결과 + 메인넷 원본 스트림 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { useReactTable, getCoreRowModel, getSortedRowModel, getPaginationRowModel, flexRender } from "@tanstack/react-table";
import { useTransfers, useStats, usePaused } from "../data/hooks.js";
import { useChain } from "../../hooks/useChain.js";
import { CONFIG } from "../../config.js";
import { Card, Kpi, Seg, Rule, Addr, Money, Empty, OriginTag, clock, short } from "../ui.jsx";

const RESULTS = {
  PASS: { label: "통과", cls: "c-ok", desc: "200 · 정상 처리" },
  PASS_HOLD_OTHER: { label: "통과 · 상대 보류", cls: "c-hold", desc: "거래는 통과, 상대 지갑 3분 보류 (R03·R06)" },
  HOLD: { label: "보류", cls: "c-hold", desc: "429 · 3분 동안 송금 불가" },
  BLOCK: { label: "차단", cls: "c-block", desc: "403 · R07 블랙리스트" },
  FROZEN: { label: "동결 지갑 거절", cls: "c-frozen", desc: "403 · 동결된 지갑의 송금 시도" },
};
const FILTERS = [["ALL", "전체"], ["PASS", "통과"], ["HOLD", "보류"], ["BLOCK", "차단"], ["FROZEN", "동결 거절"]];

function Gateway({ query }) {
  const { data: rows = [] } = useTransfers();
  const [f, setF] = useState(query.result || "ALL");
  const [q, setQ] = useState(query.q || "");
  const [rule, setRule] = useState(query.rule || "");
  useEffect(() => { if (query.result) setF(query.result); if (query.rule) setRule(query.rule); }, [query.result, query.rule]);

  const data = useMemo(() => {
    const s = q.trim().toLowerCase();
    return rows.filter((t) =>
      (f === "ALL" || t.result === f || (f === "HOLD" && t.result === "PASS_HOLD_OTHER") || (f === "PASS" && t.result === "PASS_HOLD_OTHER")) &&
      (!rule || t.rule === rule) &&
      (!s || t.from.includes(s) || t.to.includes(s) || t.tx_hash.includes(s)));
  }, [rows, f, q, rule]);

  const columns = useMemo(() => [
    { accessorKey: "ts", header: "시각", cell: (i) => <span className="mono">{clock(i.getValue())}</span> },
    { accessorKey: "source", header: "출처", cell: (i) => <span className="c-muted">{i.getValue()}</span> },
    { accessorKey: "from", header: "보낸 지갑", enableSorting: false, cell: (i) => <span className="who"><Addr a={i.getValue()} /><OriginTag origin={i.row.original.fromOrigin} /></span> },
    { accessorKey: "to", header: "받은 지갑", enableSorting: false, cell: (i) => <span className="who"><Addr a={i.getValue()} /><OriginTag origin={i.row.original.toOrigin} /></span> },
    { accessorKey: "amount", header: "금액", cell: (i) => <Money v={i.getValue()} unit={false} />, meta: { right: true } },
    { accessorKey: "result", header: "판정", cell: (i) => { const r = RESULTS[i.getValue()]; return <span className={r.cls} title={r.desc}>{r.label}</span>; } },
    { accessorKey: "rule", header: "룰", cell: (i) => (i.getValue() ? <Rule id={i.getValue()} /> : <span className="c-dim">—</span>) },
  ], []);
  const [sorting, setSorting] = useState([{ id: "ts", desc: true }]);
  const table = useReactTable({ data, columns, state: { sorting }, onSortingChange: setSorting, getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel(), getPaginationRowModel: getPaginationRowModel(), initialState: { pagination: { pageSize: 15 } }, autoResetPageIndex: false });
  const pg = table.getState().pagination;

  return (
    <>
      <div className="filters">
        <Seg value={f} options={FILTERS} onChange={(v) => { setF(v); table.setPageIndex(0); }} />
        <div className="f-right">
          {rule && <button className="chip-x" onClick={() => setRule("")}>룰 {rule} ✕</button>}
          <input className="inp" placeholder="주소 · tx_hash" value={q} onChange={(e) => setQ(e.target.value)} />
        </div>
      </div>
      <Card pad={false}>
        {data.length === 0 ? <Empty>조건에 맞는 거래가 없습니다.</Empty> : (
          <div className="tbl-wrap">
            <table className="tbl">
              <thead>{table.getHeaderGroups().map((hg) => <tr key={hg.id}>{hg.headers.map((h) => <th key={h.id} className={`${h.column.columnDef.meta?.right ? "r" : ""} ${h.column.getCanSort() ? "sortable" : ""}`} onClick={h.column.getToggleSortingHandler()}>{flexRender(h.column.columnDef.header, h.getContext())}{{ asc: " ▲", desc: " ▼" }[h.column.getIsSorted()] ?? ""}</th>)}</tr>)}</thead>
              <tbody>{table.getRowModel().rows.map((r) => <tr key={r.id} className={`res-${r.original.result}`}>{r.getVisibleCells().map((c) => <td key={c.id} className={c.column.columnDef.meta?.right ? "r" : ""}>{flexRender(c.column.columnDef.cell, c.getContext())}</td>)}</tr>)}</tbody>
            </table>
          </div>
        )}
        <div className="tbl-foot">
          <span>표본: 최근 거래 일부 + 모든 보류·차단·거절 거래 (분당 약 1,000건을 모두 표시하지는 않음)</span>
          <span className="pager">{data.length ? `${pg.pageIndex * pg.pageSize + 1}–${Math.min(data.length, (pg.pageIndex + 1) * pg.pageSize)} / ${data.length}` : "0"}
            <button className="btn sm" disabled={!table.getCanPreviousPage()} onClick={() => table.previousPage()}>이전</button>
            <button className="btn sm" disabled={!table.getCanNextPage()} onClick={() => table.nextPage()}>다음</button></span>
        </div>
      </Card>
    </>
  );
}

/** 이더리움 메인넷 USDT Transfer 원본 (실제 데이터, 판정 전) */
function Mainnet() {
  const [rows, setRows] = useState([]);
  const [paused] = usePaused();
  const onT = useCallback((t) => { if (!paused) setRows((r) => [{ ...t, key: t.tx_hash + t.log_index }, ...r].slice(0, 60)); }, [paused]);
  const chain = useChain(onT);
  const st = chain.status;
  return (
    <Card pad={false} title="메인넷 USDT 원본 스트림" desc="이더리움 공개 RPC에서 약 12초마다 받는 실제 거래입니다. 백엔드 연결 전이라 이 화면에서는 판정하지 않습니다."
      right={<span className={`src-badge ${st.source === "LIVE" ? "live" : ""}`}>{st.source === "LIVE" ? "● LIVE 메인넷" : st.source === "SIM" ? "● 시뮬레이션 (RPC 연결 실패)" : "연결 중…"}{st.block ? ` · 블록 #${st.block.toLocaleString()}` : ""}</span>}>
      {rows.length === 0 ? <Empty>첫 블록을 기다리는 중입니다 (최대 12초)…</Empty> : (
        <div className="tbl-wrap">
          <table className="tbl">
            <thead><tr><th>블록</th><th>보낸 지갑</th><th>받은 지갑</th><th className="r">금액</th><th>tx</th></tr></thead>
            <tbody>
              {rows.map((t) => (
                <tr key={t.key}>
                  <td className="mono c-muted">#{t.block_number?.toLocaleString()}</td>
                  <td><Addr a={t.from} /></td>
                  <td><Addr a={t.to} /></td>
                  <td className="r"><Money v={t.amount} unit={false} /></td>
                  <td>{t.source === "LIVE" ? <a className="mono" href={CONFIG.EXPLORER_TX + t.tx_hash} target="_blank" rel="noreferrer">{short(t.tx_hash, 8, 4)} ↗</a> : <span className="mono c-dim">{short(t.tx_hash, 8, 4)}</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

export default function Transfers({ query = {} }) {
  const [tab, setTab] = useState("gw");
  const { data: s } = useStats("1h");
  return (
    <>
      <div className="page-h">
        <div>
          <h1>실시간 거래</h1>
          <p>게이트웨이(1단계)를 지나는 거래와 판정 결과입니다. 상단 <b>실시간</b> 버튼으로 잠시 멈추고 천천히 볼 수 있습니다.</p>
        </div>
        <Seg value={tab} options={[["gw", "게이트웨이 판정"], ["main", "메인넷 원본 (실데이터)"]]} onChange={setTab} />
      </div>
      {s && (
        <div className="kpis k4">
          <Kpi label="유입" value={s.kpi.inflowPerMin} sub="건/분" tone="accent" desc="메인넷 + 시뮬레이터" />
          <Kpi label="보류 (1시간)" value={s.kpi.holds} tone="hold" desc="R01~R06 · 3분 송금 제한" />
          <Kpi label="차단 (1시간)" value={s.kpi.blocks} tone="block" desc="R07 블랙리스트" />
          <Kpi label="동결 지갑 송금 시도 (1시간)" value={s.kpi.frozenTries} tone="frozen" desc="403 FROZEN" />
        </div>
      )}
      {tab === "gw" ? <Gateway query={query} /> : <Mainnet />}
    </>
  );
}
