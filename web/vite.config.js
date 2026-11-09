import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 백엔드(FastAPI 또는 Spring Boot) 주소 — 개발 중 /api, /ws 요청을 여기로 전달
const BACKEND = process.env.VITE_PROXY_TARGET || "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
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
