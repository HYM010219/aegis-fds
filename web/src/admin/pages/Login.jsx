/* 관리자 로그인 (임시) — 백엔드 인증 API 연결 전 시연용 */
import { useEffect, useState } from "react";
import { login, session, DEMO_ACCOUNTS } from "../auth.js";
import { navigate, useRoute } from "../router.js";
import "../admin.css";

export default function Login() {
  const { query } = useRoute();
  const [id, setId] = useState("");
  const [pw, setPw] = useState("");
  const [err, setErr] = useState("");
  const next = query.next && query.next.startsWith("/") ? query.next : "/";

  useEffect(() => {
    try { document.documentElement.dataset.theme = localStorage.getItem("aegis:theme") || "dark"; } catch { /* 무시 */ }
    if (session()) navigate(next, { replace: true });
  }, [next]);

  const submit = (e) => {
    e.preventDefault();
    if (login(id, pw)) navigate(next, { replace: true });
    else setErr("아이디 또는 비밀번호가 맞지 않습니다.");
  };

  return (
    <main className="login">
      <form className="card login-box" onSubmit={submit}>
        <div className="login-logo">
          <svg viewBox="0 0 24 24" width="26" height="26" fill="none" stroke="currentColor" strokeWidth="1.9"><path d="M12 2 4 5v6c0 5 3.4 9.4 8 11 4.6-1.6 8-6 8-11V5l-8-3Z" /></svg>
          <b>Aegis-FDS 관제 콘솔</b>
        </div>
        <p className="c-muted">관리자 전용 페이지입니다. 로그인한 담당자 이름이 모든 조치 기록에 남습니다.</p>
        <label className="fld"><span>아이디</span><input className="inp" value={id} onChange={(e) => setId(e.target.value)} autoFocus autoComplete="username" /></label>
        <label className="fld"><span>비밀번호</span><input className="inp" type="password" value={pw} onChange={(e) => setPw(e.target.value)} autoComplete="current-password" /></label>
        {err && <div className="login-err">{err}</div>}
        <button className="btn primary block" type="submit">로그인</button>
        <div className="demo-acc">
          <b>시연용 계정</b> (백엔드 인증 연결 전 임시)
          {DEMO_ACCOUNTS.map((a) => (
            <button type="button" key={a.id} className="link" onClick={() => { setId(a.id); setPw(a.pw); }}>
              <span className="mono">{a.id} / {a.pw}</span> — {a.desc}
            </button>
          ))}
        </div>
        <a href="/" className="note">← 사용자 송금 화면</a>
      </form>
    </main>
  );
}
