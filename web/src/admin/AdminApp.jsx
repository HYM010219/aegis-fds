/* 관리자(관제) 페이지 — 공통 틀 + 화면 전환 */
import { lazy, Suspense, useEffect, useRef, useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Toaster, toast } from "sonner";
import { useRoute, navigate, match } from "./router.js";
import { session, logout, ROLE_LABEL } from "./auth.js";
import { useCases, useLiveSync, usePaused } from "./data/hooks.js";
import { api, isLive } from "./data/api.js";
import * as Store from "./data/store.js";
import { PROFILE, L_RULES } from "./data/meta.js";
import { short } from "./ui.jsx";
import "./admin.css";

import Dashboard from "./pages/Dashboard.jsx";
import Alerts from "./pages/Alerts.jsx";
import CaseDetail from "./pages/CaseDetail.jsx";
import Transfers from "./pages/Transfers.jsx";
import Audit from "./pages/Audit.jsx";
import System from "./pages/System.jsx";
const Flow = lazy(() => import("./pages/Flow.jsx")); // React Flow 는 무거워서 열 때만 불러옴

const qc = new QueryClient({ defaultOptions: { queries: { refetchOnWindowFocus: false, retry: 1 } } });

const NAV = [
  ["/", "대시보드"],
  ["/transfers", "실시간 거래"],
  ["/alerts", "경보"],
  ["/flow", "자금 흐름"],
  ["/audit", "감사 로그"],
  ["/system", "시스템 상태"],
];

function useTheme() {
  const [theme, setTheme] = useState(() => { try { return localStorage.getItem("aegis:theme") || "dark"; } catch { return "dark"; } });
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try { localStorage.setItem("aegis:theme", theme); } catch { /* 무시 */ }
  }, [theme]);
  return [theme, setTheme];
}

function Search() {
  const [q, setQ] = useState("");
  const [res, setRes] = useState([]);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    let alive = true;
    api.search(q).then((r) => alive && setRes(r));
    return () => { alive = false; };
  }, [q]);
  const go = (to) => { navigate(to); setQ(""); setOpen(false); };
  return (
    <div className="search">
      <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" /></svg>
      <input
        placeholder="지갑 주소 · 사건 ID · tx_hash 검색"
        value={q}
        onChange={(e) => { setQ(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        onKeyDown={(e) => e.key === "Enter" && res[0] && go(res[0].to)}
      />
      {open && q.trim().length >= 3 && (
        <div className="search-pop">
          {res.length === 0 && <div className="sp-empty">검색 결과가 없습니다 (3글자 이상, 0x로 시작하는 주소 일부도 가능)</div>}
          {res.map((r) => (
            <button key={r.kind + r.id} onMouseDown={() => go(r.to)}><span className="sp-kind">{r.kind}</span><span className="mono">{r.text}</span></button>
          ))}
        </div>
      )}
    </div>
  );
}

function Shell() {
  const { path, query } = useRoute();
  const me = session();
  const [theme, setTheme] = useTheme();
  const [paused, setPaused] = usePaused();
  const { data: cases = [] } = useCases();
  const pending = cases.filter((c) => c.status === "PENDING").length;
  useLiveSync();

  // 새 사건 알림 (검토 대기 · 고위험만 — 너무 잦으면 안 보게 되므로)
  const pathRef = useRef(path);
  pathRef.current = path;
  useEffect(() => Store.onNewCase((c) => {
    if (c.status !== "PENDING" && c.risk_level !== "CRITICAL") return;
    toast(`새 경보 · ${c.rule_id} ${L_RULES[c.rule_id].name}`, {
      description: `${short(c.violator_address)} · ${c.status === "PENDING" ? "검토 대기" : "자동 동결"}`,
      action: { label: "열기", onClick: () => navigate(`/cases/${c.case_id}`) },
    });
  }), []);

  let page;
  let m;
  if (path === "/") page = <Dashboard />;
  else if (path === "/alerts") page = <Alerts query={query} />;
  else if ((m = match("/cases/:id", path))) page = <CaseDetail id={m.id} me={me} />;
  else if (path === "/flow") page = <Flow caseId={query.case} />;
  else if ((m = match("/flow/:id", path))) page = <Flow caseId={m.id} />;
  else if (path === "/transfers") page = <Transfers query={query} />;
  else if (path === "/audit") page = <Audit />;
  else if (path === "/system") page = <System />;
  else page = <Dashboard />;

  const active = (to) => (to === "/" ? path === "/" : path.startsWith(to) || (to === "/alerts" && path.startsWith("/cases")));

  return (
    <div className="adm">
      <header className="topbar">
        <a className="brand" href="#/">
          <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="1.9"><path d="M12 2 4 5v6c0 5 3.4 9.4 8 11 4.6-1.6 8-6 8-11V5l-8-3Z" /></svg>
          <b>Aegis-FDS</b><span>관제</span>
        </a>
        <nav className="nav">
          {NAV.map(([to, label]) => (
            <a key={to} href={"#" + to} className={active(to) ? "on" : ""}>
              {label}{to === "/alerts" && pending > 0 && <em className="nav-badge" title="검토 대기">{pending}</em>}
            </a>
          ))}
        </nav>
        <Search />
        <div className="top-r">
          <button className={`live ${paused ? "paused" : ""}`} onClick={() => setPaused(!paused)} title={paused ? "실시간 수신 다시 시작" : "실시간 수신 잠시 멈춤 (화면을 천천히 볼 때)"}>
            <i />{paused ? "일시정지" : "실시간"} · {PROFILE}{!isLive() && <small>Mock</small>}
          </button>
          <button className="theme" onClick={() => setTheme(theme === "dark" ? "light" : "dark")} title="화면 밝기 전환">{theme === "dark" ? "☾ 다크" : "☀ 라이트"}</button>
          <div className="me">
            <span title={me?.id}>{me?.name}<small>{ROLE_LABEL[me?.role]}</small></span>
            <button className="link" onClick={() => { logout(); navigate("/login", { replace: true }); }}>로그아웃</button>
          </div>
        </div>
      </header>
      <main className="page">
        <Suspense fallback={<div className="boot">불러오는 중…</div>}>{page}</Suspense>
      </main>
      <footer className="foot">
        Aegis-FDS 관제 콘솔 · 기획서 v8.0 기준 · {isLive() ? "백엔드 연결됨" : "백엔드 연결 전: 브라우저 Mock 데이터 (같은 브라우저의 고객 앱 송금 결과도 함께 표시)"} · <a href="/" target="_blank" rel="noreferrer">사용자 송금 화면 열기 ↗</a>
      </footer>
      <Toaster theme={theme} position="bottom-right" richColors closeButton />
    </div>
  );
}

export default function AdminApp() {
  return (
    <QueryClientProvider client={qc}>
      <Shell />
    </QueryClientProvider>
  );
}
