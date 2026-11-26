/* ==========================================================
   관제 API 어댑터
   - VITE_API_BASE 가 있으면 FastAPI 관제 API 호출 (아래 주소는 BE와 협의할 제안안)
   - 없으면 브라우저 Mock 저장소(store.js)
   화면 코드는 이 파일의 함수만 쓰므로, 백엔드 연결 시 이 파일만 바꾸면 된다
   ========================================================== */
import { CONFIG } from "../../config.js";
import * as S from "./store.js";
import { originOf, ORIGINS } from "./meta.js";

const BASE = CONFIG.API_BASE;
export const isLive = () => !!BASE;

/* 백엔드는 거래소·제재 지갑이 아니면 모두 MAINNET(온체인)으로 보낸다.
   봇 합성 지갑도 온체인으로 보이지 않도록, 응답 안의 모든 출처 값을 주소 모양으로 다시 판정한다
   (사건 목록·상세·상대 지갑·그래프 노드·송금 내역·대시보드 공통) */
function fixOrigins(v) {
  if (Array.isArray(v)) { v.forEach(fixOrigins); return v; }
  if (!v || typeof v !== "object") return v;
  if ("origin" in v) v.origin = originOf(v.violator_address || v.address || v.wallet, v.origin);
  if ("fromOrigin" in v) {
    v.fromOrigin = originOf(v.from, v.fromOrigin);
    if (v.fromOrigin === "BOT" && "source" in v) v.source = ORIGINS.BOT.source; // 송금 내역 "출처" 칸 (백엔드는 "모의"로 보냄)
  }
  if ("toOrigin" in v) v.toOrigin = originOf(v.to, v.toOrigin);
  Object.values(v).forEach((x) => { if (x && typeof x === "object") fixOrigins(x); });
  return v;
}

async function http(path, opts = {}) {
  const r = await fetch(`${BASE}/api/v1/admin${path}`, {
    ...opts,
    headers: { "Content-Type": "application/json", "X-Session-Token": `Bearer ${sessionStorage.getItem("aegis:admin_token") || ""}`, ...(opts.headers || {}) },
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return fixOrigins(await r.json());
}
const mock = (fn) => new Promise((res, rej) => setTimeout(() => { try { res(fn()); } catch (e) { rej(e); } }, 40));

export const api = {
  cases: () => (isLive() ? http("/cases") : mock(S.listCases)),                                  // GET  /cases
  case: (id) => (isLive() ? http(`/cases/${id}`) : mock(() => S.getCase(id))),                  // GET  /cases/{id}
  stats: (range) => (isLive() ? http(`/stats?range=${range}`) : mock(() => S.getStats(range))), // GET  /stats
  transfers: () => (isLive() ? http("/transfers?limit=300") : mock(() => S.listTransfers(300))), // GET  /transfers
  audit: () => (isLive() ? http("/audit") : mock(S.getAudit)),                                    // GET  /audit
  health: () => (isLive() ? http("/health") : mock(S.getHealth)),                                 // GET  /health
  graph: (id, o) => (isLive() ? http(`/cases/${id}/graph?${new URLSearchParams(o)}`) : mock(() => S.getGraph(id, o))),
  decide: (id, action, body) =>                                                                   // POST /cases/{id}/decision
    isLive() ? http(`/cases/${id}/decision`, { method: "POST", body: JSON.stringify({ action, ...body }) }) : mock(() => S.decide(id, action, body)),
  view: (id, actor) => (isLive() ? Promise.resolve() : mock(() => S.logView(id, actor))),
  // STR 초안: 백엔드가 사건 증거 그대로 만든 초안. Mock에는 없어서 null (화면이 직접 만든 초안을 쓴다)
  strDraft: (id) => (isLive() ? http(`/cases/${id}/str-draft`).then((r) => r.str_report) : Promise.resolve(null)),        // GET   /cases/{id}/str-draft
  saveStrOpinion: (id, operator, finalOpinion) =>                                                                         // PATCH /cases/{id}/str-draft
    http(`/cases/${id}/str-draft`, { method: "PATCH", body: JSON.stringify({ operator, final_opinion: finalOpinion }) }).then((r) => r.str_report),
  search: (q) => mock(() => S.search(q)),
};
