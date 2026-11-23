/* 해시 라우터 (#/app, #/admin/...) — 정적 서버 어디에 올려도 새로 고침 404가 나지 않음 */
import { useEffect, useState } from "react";

const read = () => {
  const raw = window.location.hash.replace(/^#/, "") || "/";
  const [path, qs = ""] = raw.split("?");
  return { path: path.replace(/\/+$/, "") || "/", query: Object.fromEntries(new URLSearchParams(qs)) };
};

export function useRoute() {
  const [r, setR] = useState(read);
  useEffect(() => {
    const on = () => { setR(read()); window.scrollTo(0, 0); };
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return r;
}

export const navigate = (to, { replace = false } = {}) => {
  if (replace) window.location.replace("#" + to);
  else window.location.hash = to;
};

/** "/admin/cases/:id" 형태 매칭 → { id } 또는 null */
export function match(pattern, path) {
  const a = pattern.split("/"), b = path.split("/");
  if (a.length !== b.length) return null;
  const out = {};
  for (let i = 0; i < a.length; i++) {
    if (a[i].startsWith(":")) out[a[i].slice(1)] = decodeURIComponent(b[i]);
    else if (a[i] !== b[i]) return null;
  }
  return out;
}
