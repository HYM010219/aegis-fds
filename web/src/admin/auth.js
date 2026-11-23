/* ==========================================================
   관리자 로그인 · 권한 (임시)
   백엔드 인증 API(POST /api/v1/admin/login → JWT)가 나오기 전까지 쓰는 시연용 계정
   - ANALYST: 조회 + 동결 · 해제 · 오탐 처리 · STR 초안
   - VIEWER : 조회만
   ========================================================== */
const ACCOUNTS = {
  analyst: { pw: "aegis1234", name: "김분석", role: "ANALYST" },
  viewer: { pw: "view1234", name: "박조회", role: "VIEWER" },
};
export const DEMO_ACCOUNTS = [
  { id: "analyst", pw: "aegis1234", desc: "분석관 — 동결·오탐 처리 가능" },
  { id: "viewer", pw: "view1234", desc: "조회 전용" },
];
export const ROLE_LABEL = { ANALYST: "분석관", VIEWER: "조회 전용" };
const KEY = "aegis:admin_session";

export function login(id, pw) {
  const a = ACCOUNTS[id?.trim()];
  if (!a || a.pw !== pw) return null;
  const s = { id: id.trim(), name: a.name, role: a.role, at: Date.now() };
  try { sessionStorage.setItem(KEY, JSON.stringify(s)); } catch { /* 저장 불가 */ }
  return s;
}
export function session() {
  try { return JSON.parse(sessionStorage.getItem(KEY)); } catch { return null; }
}
export function logout() {
  try { sessionStorage.removeItem(KEY); } catch { /* 무시 */ }
}
export const can = (s, action) => s?.role === "ANALYST" || action === "view";
export const actorName = (s) => `${s?.name || "알 수 없음"} (${ROLE_LABEL[s?.role] || "-"})`;
