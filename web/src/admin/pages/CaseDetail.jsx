/* S2 사건 상세 — "정말 이상한 거래인가? 어떻게 조치할까?" */
import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, ReferenceLine, CartesianGrid } from "recharts";
import { useCase, useDecide } from "../data/hooks.js";
import { api } from "../data/api.js";
import { L_RULES, LONG_LABEL, ORIGINS } from "../data/meta.js";
import { can, actorName } from "../auth.js";
import { navigate } from "../router.js";
import { CONFIG } from "../../config.js";
import { Card, Grade, Status, Rule, Avatar, Addr, OriginTag, Money, EvidenceBar, Modal, Empty, usd, short, clock, dateTime, ago, copy, useNow } from "../ui.jsx";

const ACTION_TEXT = {
  CASE_CREATED: "사건 생성", AUTO_FREEZE: "자동 동결", VIEWED: "열람", FREEZE: "동결", DISMISS: "오탐 처리", RELEASE: "동결 해제", STR_DRAFT: "STR 초안 작성",
};

function Steps({ status }) {
  const done = ["FROZEN", "DISMISSED", "RELEASED", "AUTO_FROZEN"].includes(status);
  const end = { FROZEN: "동결", DISMISSED: "오탐 종결", RELEASED: "해제", AUTO_FROZEN: "자동 동결" }[status] || "종결 (동결 · 오탐)";
  return (
    <ol className="steps">
      <li className="done">탐지</li>
      <li className={status === "PENDING" ? "now" : status === "AUTO_FROZEN" ? "skip" : "done"}>검토 대기</li>
      <li className={done ? "done end" : ""}>{end}</li>
    </ol>
  );
}

function FlowBars({ flows, from, to }) {
  const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  const N = 24, step = (to - from) / N;
  const data = Array.from({ length: N }, (_, i) => {
    const a = from + i * step, b = a + step;
    const f = flows.filter((x) => x.ts >= a && x.ts < b);
    return { t: a, 받음: f.reduce((s, x) => s + x.in, 0), 보냄: -f.reduce((s, x) => s + x.out, 0) };
  });
  return (
    <ResponsiveContainer width="100%" height={190}>
      <BarChart data={data} stackOffset="sign" margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid stroke={css("--line")} vertical={false} />
        <XAxis dataKey="t" tickFormatter={(t) => clock(t).slice(0, 5)} stroke={css("--dim")} fontSize={12} minTickGap={40} />
        <YAxis stroke={css("--dim")} fontSize={12} width={56} tickFormatter={(v) => (Math.abs(v) >= 1000 ? `${Math.round(v / 1000)}k` : v)} />
        <ReferenceLine y={0} stroke={css("--line-2")} />
        <Tooltip contentStyle={{ background: css("--panel"), border: `1px solid ${css("--line-2")}`, borderRadius: 10, fontSize: 13 }}
          labelFormatter={(t) => `${clock(t)} 부터`} formatter={(v, n) => [`${usd(Math.abs(v))} USDT`, n]} />
        <Bar dataKey="받음" stackId="a" fill={css("--ok")} radius={[3, 3, 0, 0]} isAnimationActive={false} />
        <Bar dataKey="보냄" stackId="a" fill={css("--frozen")} radius={[3, 3, 0, 0]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}

/** 조치 확인 창: 결과를 설명하고 사유를 받는다 */
function ConfirmAction({ open, action, c, onClose, onDone, me }) {
  const [note, setNote] = useState("");
  const decide = useDecide();
  useEffect(() => { if (open) setNote(""); }, [open]);
  if (!open) return null;
  const info = {
    FREEZE: { title: `지갑 동결 (FROZEN_${c.rule_id})`, btn: "동결하기", tone: "danger", need: true,
      effect: ["이 지갑의 송금 요청은 403 FROZEN으로 즉시 거절됩니다 (입금은 계속 가능).", "게이트웨이 동결 목록(Redis fds:frozen_wallets)에 추가됩니다.", "누가·언제·왜 동결했는지 감사 기록에 남습니다."],
      ph: "예: 제재 지갑에서 받은 자금을 바로 다른 지갑으로 옮김 — 자금세탁 의심" },
    DISMISS: { title: "오탐 처리 (정상 거래로 종결)", btn: "오탐으로 종결", tone: "primary", need: true,
      effect: ["사건이 '오탐 처리'로 종결되고 검토 대기 목록에서 빠집니다.", "지갑에는 아무 제한도 걸리지 않습니다.", "사유는 룰 기준 조정(성적표)에 쓰이므로 구체적으로 적어 주세요."],
      ph: "예: 거래소 정산 지갑으로 확인됨" },
    RELEASE: { title: "동결 해제", btn: "해제하기", tone: "primary", need: true,
      effect: ["이 지갑은 다시 송금할 수 있게 됩니다.", "Redis fds:frozen_wallets · fds:frozen_reasons 에서 함께 삭제하고 사건 상태를 맞춥니다.", "해제 이전 기록으로는 다시 자동 동결하지 않습니다."],
      ph: "예: 고객 소명 자료(거래 목적 증빙) 확인" },
  }[action];
  const submit = () => {
    decide.mutate({ id: c.case_id, action, actor: actorName(me), note: note.trim() }, {
      onSuccess: () => { toast.success(`${info.title} 완료 — 감사 기록에 저장했습니다`); onDone(); },
      onError: (e) => toast.error(`실패: ${e.message}`),
    });
  };
  return (
    <Modal open title={info.title} onClose={onClose}
      footer={<>
        <button className="btn" onClick={onClose}>취소</button>
        <button className={`btn ${info.tone}`} disabled={(info.need && note.trim().length < 4) || decide.isPending} onClick={submit}>{decide.isPending ? "처리 중…" : info.btn}</button>
      </>}>
      <div className="confirm-who"><Avatar origin={c.origin} /> <Addr a={c.violator_address} full /></div>
      <ul className="effect">{info.effect.map((t) => <li key={t}>{t}</li>)}</ul>
      <label className="fld">
        <span>사유 <em>(필수 · 감사 기록에 저장)</em></span>
        <textarea rows={3} value={note} onChange={(e) => setNote(e.target.value)} placeholder={info.ph} autoFocus />
      </label>
    </Modal>
  );
}

/** STR(의심거래보고) 초안 — KoFIU 제출 양식 항목은 팀 확인 후 맞출 예정 */
function StrDraft({ open, c, onClose, me }) {
  const text = useMemo(() => {
    if (!c) return "";
    const r = L_RULES[c.rule_id];
    const lines = [
      "[의심거래보고(STR) 초안]  ※ 자동 생성 초안 — 보고책임자 검토 후 제출",
      "",
      `1. 보고 기관      : (회사명 입력) 준법감시실`,
      `2. 작성자         : ${actorName(me)}`,
      `3. 작성 일시      : ${dateTime(Date.now())}`,
      `4. 사건 번호      : ${c.case_id}`,
      "",
      "5. 혐의 대상",
      `   - 지갑 주소    : ${c.violator_address}`,
      `   - 지갑 구분    : ${ORIGINS[c.origin]?.label || c.origin}`,
      `   - 네트워크/자산: Ethereum Mainnet / USDT (ERC-20)`,
      "",
      "6. 의심 거래 요약",
      `   - 탐지 룰      : ${c.rule_id} ${r.name} (${c.risk_level})`,
      `   - 탐지 일시    : ${dateTime(c.detected_at)}`,
      `   - 관련 금액    : ${usd(c.amount)} USDT`,
      `   - 요약         : ${c.headline}`,
      "",
      "7. 의심 근거 (실측값 / 기준값)",
      ...c.evidence.map((e) => `   - ${e.label}: ${usd(e.value, 1)} ${e.unit} / 기준 ${usd(e.threshold)} ${e.unit}`),
      "",
      "8. 관련 거래",
      ...c.related.slice(0, 15).map((t) => `   - ${dateTime(t.ts)}  ${t.from} → ${t.to}  ${usd(t.amount)} USDT  ${t.tx_hash}`),
      ...(c.related.length > 15 ? [`   - 외 ${c.related.length - 15}건`] : []),
      "",
      "9. 조치 내용",
      `   - 현재 상태    : ${{ PENDING: "검토 대기", AUTO_FROZEN: "자동 동결", FROZEN: "담당자 동결", DISMISSED: "오탐 종결", RELEASED: "동결 해제" }[c.status]}`,
      ...c.audit.filter((a) => ["FREEZE", "DISMISS", "RELEASE", "AUTO_FREEZE"].includes(a.action)).map((a) => `   - ${dateTime(a.ts)} ${a.actor}: ${ACTION_TEXT[a.action]} — ${a.note}`),
      "",
      "10. 보고책임자 의견: (작성)",
    ];
    return lines.join("\n");
  }, [c, me]);
  if (!open) return null;
  const save = async () => {
    await api.decide(c.case_id, "STR_DRAFT", { actor: actorName(me), note: "STR 초안 생성 · 다운로드" }).catch(() => {});
    const blob = new Blob(["﻿" + text], { type: "text/plain;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `STR_${c.case_id}.txt`;
    a.click();
    URL.revokeObjectURL(a.href);
    toast.success("STR 초안을 내려받고 감사 기록에 남겼습니다");
  };
  return (
    <Modal open wide title="STR 초안 (의심거래보고)" onClose={onClose}
      footer={<>
        <span className="note">KoFIU 실제 제출 양식 항목은 팀 확인 후 맞출 예정입니다.</span>
        <button className="btn" onClick={() => copy(text, "STR 초안을 복사했습니다")}>복사</button>
        <button className="btn primary" disabled={!can(me, "act")} onClick={save}>내려받기 (.txt)</button>
      </>}>
      <pre className="str">{text}</pre>
    </Modal>
  );
}

export default function CaseDetail({ id, me }) {
  const now = useNow(5000);
  const { data: c, isLoading } = useCase(id);
  const [act, setAct] = useState(null);
  const [str, setStr] = useState(false);
  const [raw, setRaw] = useState(false);
  useEffect(() => { if (id) api.view(id, actorName(me)); }, [id, me]);

  if (isLoading) return <div className="boot">불러오는 중…</div>;
  if (!c) return <Card title="사건을 찾을 수 없습니다"><Empty>사건 번호 {id}가 없습니다. <a href="#/alerts">경보 목록으로</a></Empty></Card>;

  const r = L_RULES[c.rule_id];
  const allowed = can(me, "act");
  const deny = allowed ? undefined : "조회 전용 계정은 조치할 수 없습니다";
  const isFrozen = c.status === "AUTO_FROZEN" || c.status === "FROZEN";
  const evidence = c.rule_id === "L5" ? c.evidence : c.evidence;

  return (
    <>
      <a className="back" href="#/alerts">← 경보 목록</a>

      <section className="card case-head">
        <div className="ch-left">
          <Avatar origin={c.origin} size={48} />
          <div>
            <div className="ch-addr">
              <Addr a={c.violator_address} full copyable />
              <a href={`https://etherscan.io/address/${c.violator_address}`} target="_blank" rel="noreferrer" className="ext">Etherscan ↗</a>
            </div>
            <div className="tags">
              <Grade g={c.risk_level} /><Rule id={c.rule_id} /><OriginTag origin={c.origin} isNew={c.isNew} /><Status s={c.status} />
              <span className="c-muted mono">{c.case_id} · {dateTime(c.detected_at)} 탐지 ({ago(c.detected_at, now)})</span>
            </div>
          </div>
        </div>
        <div className="ch-right">
          <Steps status={c.status} />
          <div className="ch-btns">
            {c.status === "PENDING" && <>
              <button className="btn danger" disabled={!allowed} title={deny} onClick={() => setAct("FREEZE")}>🔒 동결 (FROZEN_{c.rule_id})</button>
              <button className="btn" disabled={!allowed} title={deny} onClick={() => setAct("DISMISS")}>오탐 처리</button>
            </>}
            {isFrozen && <button className="btn" disabled={!allowed} title={deny} onClick={() => setAct("RELEASE")}>동결 해제</button>}
            <button className="btn" onClick={() => setStr(true)}>STR 초안</button>
          </div>
        </div>
      </section>

      <div className="split wide-r">
        <div className="col">
          <Card title="왜 걸렸나요?" className="why">
            <p className="why-head">{c.headline}</p>
            <div className="why-rule">
              <Rule id={c.rule_id} plain />
              <span className={`why-act ${r.action === "AUTO" ? "auto" : "review"}`}>
                {r.action === "AUTO" ? "자동 동결 룰 — 기준을 넘으면 사람 확인 없이 바로 동결" : "검토 대기 룰 — 담당자가 근거를 보고 동결 또는 오탐 처리"}
              </span>
            </div>
            {c.status === "PENDING" && allowed && <p className="why-next">→ 아래 근거와 관련 거래를 확인한 뒤, 위의 <b>동결</b> 또는 <b>오탐 처리</b>를 눌러 종결하세요.</p>}
            {c.decided && <p className="why-next">→ {dateTime(c.decided.ts)} {c.decided.actor}: <b>{ACTION_TEXT[{ FROZEN: "FREEZE", DISMISSED: "DISMISS", RELEASED: "RELEASE" }[c.decided.status]]}</b> — {c.decided.note}</p>}
          </Card>

          <Card title="판정 근거" desc="실측값과 기준값. 빨간 테두리가 기준을 넘은 항목입니다.">
            <div className="ev-grid">{evidence.map((e) => <EvidenceBar key={e.label} item={e} />)}</div>
          </Card>

          <Card title="입출금 흐름" desc={`분석 기간 ${LONG_LABEL} · 초록 = 받음(위) · 빨강 = 보냄(아래)`}>
            {c.flows.length ? <FlowBars flows={c.flows} from={c.detected_at - (CONFIG.FDS.PERIODS[CONFIG.FDS.PERIOD_PROFILE] || CONFIG.FDS.PERIODS.demo).LONG} to={Math.max(now, c.detected_at + 60_000)} /> : <Empty>분석 기간 안의 거래가 없습니다.</Empty>}
          </Card>

          <Card title={<>관련 거래 <small className="c-muted mono">tx_hashes {c.related.length}건</small></>} desc="이 사건의 근거가 된 거래입니다. tx를 누르면 복사됩니다." pad={false}>
            {c.related.length === 0 ? <Empty>관련 거래 없음 (보류 기록만 있는 경우)</Empty> : (
              <div className="tbl-wrap">
                <table className="tbl">
                  <thead><tr><th>시각</th><th>보낸 지갑</th><th /><th>받은 지갑</th><th className="r">금액</th><th>결과</th><th>tx</th></tr></thead>
                  <tbody>
                    {c.related.map((t) => (
                      <tr key={t.tx_hash}>
                        <td className="mono">{clock(t.ts)}</td>
                        <td><WalletCell a={t.from} me={c.violator_address} sdn={c.sdn} /></td>
                        <td className="c-dim">→</td>
                        <td><WalletCell a={t.to} me={c.violator_address} sdn={c.sdn} /></td>
                        <td className="r"><Money v={t.amount} unit={false} /></td>
                        <td>{t.status === "ACCEPTED" ? <span className="c-ok">통과</span> : <span className="c-hold">{t.reason}</span>}</td>
                        <td><button className="link mono" onClick={() => copy(t.tx_hash, "tx_hash를 복사했습니다")}>{short(t.tx_hash, 6, 4)}</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>

          <Card title="사건 원본 (fds_cases)" right={<button className="btn sm" onClick={() => setRaw(!raw)}>{raw ? "접기" : "펼치기"}</button>}>
            {raw ? <pre className="json">{JSON.stringify({
              case_id: c.case_id, rule_id: c.rule_id, risk_level: c.risk_level, violator_address: c.violator_address,
              tx_hashes: c.tx_hashes, evidence: c.evidence, action_taken: r.action === "AUTO" ? "AUTO_FREEZE" : "REVIEW",
              review_status: c.status, detected_at: new Date(c.detected_at).toISOString(), period_profile: CONFIG.FDS.PERIOD_PROFILE,
            }, null, 2)}</pre> : <p className="c-muted" style={{ margin: 0 }}>MongoDB에 저장되는 원본 데이터입니다. 개발·감사용으로만 필요합니다.</p>}
          </Card>
        </div>

        <div className="col">
          <Card title="지갑 요약" desc={`최근 ${LONG_LABEL}`}>
            <dl className="kv">
              <dt>받은 금액</dt><dd className="c-ok"><Money v={c.summary.received} /></dd>
              <dt>보낸 금액</dt><dd className="c-frozen"><Money v={c.summary.sent} /></dd>
              <dt>거래 상대</dt><dd>{c.summary.counterparties}곳</dd>
              <dt>1단계 보류 이력</dt><dd>{c.summary.holds.length ? Object.entries(c.summary.holds.reduce((m, x) => ({ ...m, [x]: (m[x] || 0) + 1 }), {})).map(([k, v]) => `${k} × ${v}`).join(", ") : "없음"}</dd>
              <dt>처음 본 시각</dt><dd>{c.summary.firstSeen ? ago(c.summary.firstSeen, now) : "-"}</dd>
              <dt>이전 사건</dt><dd>{c.otherCases.length ? c.otherCases.map((o) => <a key={o.case_id} href={`#/cases/${o.case_id}`} className="mono">{o.rule_id} </a>) : "없음"}</dd>
            </dl>
          </Card>

          <Card title="주요 거래 상대" desc="금액이 큰 순서 · 막대 = 거래 금액">
            {c.counterparties.length === 0 ? <Empty>거래 상대 없음</Empty> : (
              <div className="cps">
                {c.counterparties.map((p) => {
                  const max = c.counterparties[0].in + c.counterparties[0].out;
                  return (
                    <div className="cp" key={p.address}>
                      <div className="cp-top"><span className="who"><Avatar origin={p.origin} size={22} /><Addr a={p.address} /><OriginTag origin={p.origin} /></span><Money v={p.in + p.out} unit={false} /></div>
                      <div className="cp-bar"><span className="in" style={{ width: `${(p.in / max) * 100}%` }} /><span className="out" style={{ width: `${(p.out / max) * 100}%` }} /></div>
                    </div>
                  );
                })}
              </div>
            )}
            <button className="btn block" onClick={() => navigate(`/flow/${c.case_id}`)}>자금 흐름 그래프로 보기 →</button>
          </Card>

          <Card title="조치 기록" desc="수정·삭제할 수 없는 감사 기록 (policy_decisions)">
            <ol className="timeline">
              {[...c.audit].reverse().map((a) => (
                <li key={a.seq} className={`tl-${a.action}`}>
                  <span className="tl-time mono">{clock(a.ts)}</span>
                  <b>{ACTION_TEXT[a.action] || a.action}</b> · {a.actor}
                  {a.note && <div className="tl-note">{a.note}</div>}
                </li>
              ))}
              {c.status === "PENDING" && <li className="tl-wait"><span className="tl-time">—</span>동결 또는 오탐 처리 시 여기에 기록됩니다</li>}
            </ol>
            <a href="#/audit" className="note">전체 감사 로그 보기 →</a>
          </Card>
        </div>
      </div>

      <ConfirmAction open={!!act} action={act} c={c} me={me} onClose={() => setAct(null)} onDone={() => setAct(null)} />
      <StrDraft open={str} c={c} me={me} onClose={() => setStr(false)} />
    </>
  );
}

function WalletCell({ a, me, sdn }) {
  const isSdn = sdn?.some((s) => s.address === a);
  return (
    <span className="who">
      <Addr a={a} />
      {a === me ? <span className="otag o-target">대상</span> : isSdn ? <span className="otag o-violet">OFAC SDN</span> : null}
    </span>
  );
}
