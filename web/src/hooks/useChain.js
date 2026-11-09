import { useEffect, useRef, useState } from "react";
import { createChain } from "../lib/chain.js";

/** 메인넷 데이터 소스를 컴포넌트 수명에 맞춰 시작/정지하는 훅 */
export function useChain(onTransfer) {
  const chainRef = useRef(null);
  const cbRef = useRef(onTransfer);
  cbRef.current = onTransfer; // 항상 최신 콜백 사용 (stale closure 방지)
  const [status, setStatus] = useState({ mode: "AUTO", source: "—", block: null, gasGwei: null });

  useEffect(() => {
    const chain = createChain();
    chainRef.current = chain;
    const offStatus = chain.on("status", setStatus);
    const offTransfer = chain.on("transfer", (t) => cbRef.current?.(t));
    chain.start();
    return () => { offStatus(); offTransfer(); chain.stop(); };
  }, []);

  return {
    status,
    setMode: (m) => chainRef.current?.setMode(m),
    inject: (events) => chainRef.current?.inject(events),
    getState: () => chainRef.current?.getState() ?? status,
  };
}
