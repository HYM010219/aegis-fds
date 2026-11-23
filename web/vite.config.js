import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

// /admin 으로 들어오면 /admin/ (관리자 페이지)로 보냄 — 개발 서버 · preview 공통
const adminRedirect = {
  name: "admin-redirect",
  configureServer(server) { server.middlewares.use(redirect); },
  configurePreviewServer(server) { server.middlewares.use(redirect); },
};
function redirect(req, res, next) {
  if (req.url === "/admin" || req.url.startsWith("/admin?")) {
    res.statusCode = 301;
    res.setHeader("Location", "/admin/" + req.url.slice(6));
    return res.end();
  }
  next();
}

// 읽기 전용 데모 가드: VITE_READONLY=1 일 때 GET 외 모든 변경 요청(POST/PUT/DELETE)을 403으로 차단
function readonlyGuard(isReadonly) {
  return {
    name: "readonly-guard",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (isReadonly && req.url.startsWith("/api") && req.method !== "GET") {
          res.statusCode = 403;
          res.setHeader("Content-Type", "application/json");
          res.end('{"status":"ERROR","reason":"READ_ONLY_DEMO"}');
          return;
        }
        next();
      });
    },
  };
}

// 프록시가 요청을 백엔드로 넘길 때 서버 쪽에서 헤더를 붙인다 (브라우저 코드에는 키가 없음)
function withHeader(target, name, value) {
  return {
    target,
    changeOrigin: true,
    headers: {
      [name]: value,
    },
    configure(proxy) {
      proxy.on("proxyReq", (proxyReq) => proxyReq.setHeader(name, value));
    },
  };
}

export default defineConfig(({ mode }) => {
  // web/.env.local : VITE_API_BASE, VITE_PROXY_TARGET, VITE_PERIOD_PROFILE
  const web = loadEnv(mode, import.meta.dirname, "");
  // 저장소 루트 .env : GATEWAY_API_KEY, ADMIN_API_KEY (값은 이 파일에 적지 않는다)
  const root = loadEnv(mode, resolve(import.meta.dirname, ".."), "");

  const BACKEND = web.VITE_PROXY_TARGET || process.env.VITE_PROXY_TARGET || "http://localhost:8000";
  const live = !!(web.VITE_API_BASE || "").trim();
  const isReadonly = web.VITE_READONLY === "1" || process.env.VITE_READONLY === "1";

  const proxy = {};
  if (live) {
    const gw = root.GATEWAY_API_KEY;
    const adm = root.ADMIN_API_KEY;
    if (!gw || !adm) {
      throw new Error("VITE_API_BASE가 켜져 있는데 루트 .env에 GATEWAY_API_KEY / ADMIN_API_KEY가 없습니다.");
    }
    // 구체적인 경로를 먼저 둔다 (앞에서부터 맞는 규칙을 쓴다)
    proxy["/api/v1/admin"] = withHeader(BACKEND, "X-Admin-Key", adm);
    proxy["/api/v1/transfers"] = withHeader(BACKEND, "X-API-Key", gw);
  }
  proxy["/api"] = { target: BACKEND, changeOrigin: true };
  proxy["/ws"] = { target: BACKEND, ws: true, changeOrigin: true };

  return {
    plugins: [react(), adminRedirect, readonlyGuard(isReadonly)],
    // 페이지 2개를 따로 빌드: 사용자(/) · 관리자(/admin/)
    build: {
      rollupOptions: {
        input: {
          user: resolve(import.meta.dirname, "index.html"),
          admin: resolve(import.meta.dirname, "admin/index.html"),
        },
      },
    },
    optimizeDeps: { include: ["@xyflow/react", "recharts", "@tanstack/react-table", "@tanstack/react-query", "sonner"] },
    server: {
      host: live && web.VITE_ALLOW_LAN !== "1" ? "127.0.0.1" : "0.0.0.0",
      port: 8080,
      strictPort: true,
      allowedHosts: true,
      proxy,
    },
    preview: { host: "0.0.0.0", port: 8080, strictPort: true },
  };
});
