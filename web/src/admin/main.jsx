/* 관리자 페이지 진입점 — http://주소:8080/admin/
   로그인하지 않았으면 어떤 화면을 열어도 로그인 화면으로 보냄 (권한 분리) */
import { StrictMode, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { useRoute, navigate } from "./router.js";
import { session } from "./auth.js";
import AdminApp from "./AdminApp.jsx";
import Login from "./pages/Login.jsx";
import "../base.css";

function AdminRoot() {
  const { path } = useRoute();
  const blocked = path !== "/login" && !session();
  useEffect(() => {
    if (blocked) navigate(`/login?next=${encodeURIComponent(path)}`, { replace: true });
  }, [blocked, path]);
  if (path === "/login") return <Login />;
  return blocked ? null : <AdminApp />;
}

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <AdminRoot />
  </StrictMode>
);
