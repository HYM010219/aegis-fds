import { defineConfig } from "vite";
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

// 백엔드(FastAPI 또는 Spring Boot) 주소 — 개발 중 /api, /ws 요청을 여기로 전달
const BACKEND = process.env.VITE_PROXY_TARGET || "http://localhost:8000";

export default defineConfig({
  plugins: [react(), adminRedirect],
  // 페이지 2개를 따로 빌드: 사용자(/) · 관리자(/admin/)
  build: {
    rollupOptions: {
      input: {
        user: resolve(import.meta.dirname, "index.html"),
        admin: resolve(import.meta.dirname, "admin/index.html"),
      },
    },
  },
  // 관리자 화면 라이브러리를 미리 준비 (처음 열 때 개발 서버가 페이지를 새로 고치지 않도록)
  optimizeDeps: { include: ["@xyflow/react", "recharts", "@tanstack/react-table", "@tanstack/react-query", "sonner"] },
  server: {
    host: "0.0.0.0", // localhost + 사설 IP(192.168.0.22) 모두 접속 허용
    port: 8080,      // 기존 방화벽 규칙(8080)을 그대로 사용
    strictPort: true,
    proxy: {
      "/api": { target: BACKEND, changeOrigin: true },
      "/ws": { target: BACKEND, ws: true, changeOrigin: true },
    },
  },
  preview: { host: "0.0.0.0", port: 8080, strictPort: true },
});
