/* TanStack Query 훅 — 화면은 이 훅으로만 데이터를 받는다 */
import { useEffect, useState } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, isLive } from "./api.js";
import * as S from "./store.js";

const opts = { placeholderData: keepPreviousData, staleTime: 1000 };

export const useCases = () => useQuery({ queryKey: ["cases"], queryFn: api.cases, ...opts });
export const useCase = (id) => useQuery({ queryKey: ["case", id], queryFn: () => api.case(id), enabled: !!id, ...opts });
export const useStats = (range) => useQuery({ queryKey: ["stats", range], queryFn: () => api.stats(range), ...opts });
export const useTransfers = () => useQuery({ queryKey: ["transfers"], queryFn: api.transfers, ...opts });
export const useAudit = () => useQuery({ queryKey: ["audit"], queryFn: api.audit, ...opts });
export const useHealth = () => useQuery({ queryKey: ["health"], queryFn: api.health, refetchInterval: 5000, ...opts });
export const useGraph = (id, o) => useQuery({ queryKey: ["graph", id, o], queryFn: () => api.graph(id, o), enabled: !!id, ...opts });

export function useDecide() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, action, actor, note }) => api.decide(id, action, { actor, note }),
    onSuccess: () => qc.invalidateQueries(),
  });
}

/**
 * 실시간 연결: Mock 저장소가 바뀌면(=WebSocket 메시지 도착) 화면 데이터를 새로 고침
 * 백엔드 연결 시: WS /ws/admin 메시지를 받아 같은 방식으로 invalidateQueries
 */
export function useLiveSync() {
  const qc = useQueryClient();
  useEffect(() => {
    if (isLive()) {
      const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/admin`);
      ws.onmessage = () => qc.invalidateQueries();
      return () => ws.close();
    }
    S.startLive();
    let t = null;
    const off = S.subscribe(() => {
      if (t) return;
      t = setTimeout(() => { t = null; qc.invalidateQueries(); }, 400); // 너무 잦은 갱신 묶기
    });
    return () => { off(); clearTimeout(t); S.stopLive(); };
  }, [qc]);
}

export function usePaused() {
  const [p, setP] = useState(S.isPaused());
  useEffect(() => S.subscribe(() => setP(S.isPaused())), []);
  return [p, (v) => S.setPaused(v)];
}
