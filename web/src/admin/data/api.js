/* ==========================================================
   관제 API 어댑터
   - VITE_API_BASE 가 있으면 FastAPI 관제 API 호출 (아래 주소는 BE와 협의할 제안안)
   - 없으면 브라우저 Mock 저장소(store.js)
   화면 코드는 이 파일의 함수만 쓰므로, 백엔드 연결 시 이 파일만 바꾸면 된다
   ========================================================== */
import { CONFIG } from "../../config.js";
import * as S from "./store.js";

const BASE = CONFIG.API_BASE;
export const isLive = () => !!BASE;

async function http(path, opts = {}) {
  const r = await fetch(`${BASE}/api/v1/admin${path}`, {
    ...opts,
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${sessionStorage.getItem("aegis:admin_token") || ""}`, ...(opts.headers || {}) },
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
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
  search: (q) => mock(() => S.search(q)),
};
