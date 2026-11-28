/* 감사 로그 (policy_decisions) — 누가 · 언제 · 무엇을 · 왜 */
import { useMemo, useState } from "react";
import { useAudit } from "../data/hooks.js";
import { Card, Empty, Seg, Addr, dateTime, copy } from "../ui.jsx";

const ACTIONS = {
  CASE_CREATED: ["사건 생성", "c-muted"], AUTO_FREEZE: ["자동 동결", "c-frozen"], VIEWED: ["열람", "c-muted"],
  FREEZE: ["동결", "c-frozen"], DISMISS: ["오탐 처리", "c-ok"], RELEASE: ["동결 해제", "c-accent"], STR_DRAFT: ["STR 초안", "c-block"],
};
const FILTERS = [["ALL", "전체"], ["HUMAN", "담당자 조치만"], ["FREEZE", "동결"], ["DISMISS", "오탐 처리"], ["RELEASE", "해제"], ["STR_DRAFT", "STR"]];

export default function Audit() {
  const { data: rows = [] } = useAudit();
  const [f, setF] = useState("HUMAN");
  const [q, setQ] = useState("");
  const [limit, setLimit] = useState(100);
  const data = useMemo(() => {
    const s = q.trim().toLowerCase();
    return rows.filter((r) =>
      (f === "ALL" || (f === "HUMAN" ? !r.actor.startsWith("SYSTEM") : r.action === f)) &&
      (!s || r.case_id?.toLowerCase().includes(s) || r.wallet?.includes(s) || r.actor.toLowerCase().includes(s) || r.note?.toLowerCase().includes(s)));
  }, [rows, f, q]);

  const exportCsv = () => {
    const head = "seq,time,actor,action,case_id,wallet,note,prev_hash,hash";
    const body = data.map((r) => [r.seq, new Date(r.ts).toISOString(), r.actor, r.action, r.case_id, r.wallet, (r.note || "").replace(/"/g, "'"), r.prev, r.hash].map((x) => `"${x ?? ""}"`).join(","));
    const blob = new Blob(["﻿" + [head, ...body].join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `policy_decisions_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  return (
    <>
      <div className="page-h">
        <div>
          <h1>감사 로그</h1>
          <p>동결·해제·오탐 처리·STR 작성을 <b>누가, 언제, 왜</b> 했는지 남긴 기록입니다. 이 화면에서는 수정하거나 지울 수 없습니다.</p>
        </div>
        <button className="btn" onClick={exportCsv}>CSV 내려받기</button>
      </div>
      <div className="info-box">
        <b>수정되지 않았음을 확인하는 방법</b> — 각 기록은 바로 앞 기록의 해시(prev)를 포함해 새 해시를 만듭니다. 중간 기록 하나라도 바뀌면 그 뒤의 해시가 모두 달라집니다.
        (백엔드에서는 MongoDB policy_decisions 에 추가만 가능한 권한으로 저장)
      </div>
      <div className="filters">
        <Seg value={f} options={FILTERS} onChange={setF} />
        <div className="f-right"><input className="inp" placeholder="사건 ID · 지갑 · 담당자 · 내용" value={q} onChange={(e) => setQ(e.target.value)} /></div>
      </div>
      <Card pad={false}>
        {data.length === 0 ? <Empty>기록이 없습니다.</Empty> : (
          <div className="tbl-wrap">
            <table className="tbl">
              <thead><tr><th>#</th><th>시각</th><th>담당자</th><th>조치</th><th>사건</th><th>지갑</th><th>내용 · 사유</th><th>해시</th></tr></thead>
              <tbody>
                {data.slice(0, limit).map((r) => {
                  const [label, cls] = ACTIONS[r.action] || [r.action, ""];
                  return (
                    <tr key={r.seq}>
                      <td className="mono c-dim">{r.seq}</td>
                      <td className="mono nowrap">{dateTime(r.ts)}</td>
                      <td className="nowrap">{r.actor}</td>
                      <td><b className={cls}>{label}</b></td>
                      <td>{r.case_id ? <a className="mono" href={`#/cases/${r.case_id}`}>{r.case_id}</a> : "—"}</td>
                      <td>{r.wallet ? <Addr a={r.wallet} /> : "—"}</td>
                      <td className="note-cell">{r.note}</td>
                      <td><button className="link mono c-dim" title={`prev ${r.prev} → ${r.hash}`} onClick={() => copy(r.hash)}>{r.hash}</button></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <div className="tbl-foot">
          <span>{data.length.toLocaleString()}건 중 {Math.min(limit, data.length)}건 표시</span>
          {data.length > limit && <button className="btn sm" onClick={() => setLimit(limit + 100)}>100건 더 보기</button>}
        </div>
      </Card>
    </>
  );
}
