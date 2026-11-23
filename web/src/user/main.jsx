/* 사용자 페이지 진입점 — http://주소:8080/  (관리자 코드는 포함되지 않음) */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import UserApp from "./UserApp.jsx";
import "../base.css";

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <UserApp />
  </StrictMode>
);
