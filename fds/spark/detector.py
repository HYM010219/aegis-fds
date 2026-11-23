"""2단계 배치 흐름: stream.py가 10초마다 Kafka 메시지를 넘기면 분석할 지갑을 골라 L1~L8 경보를 돌려준다.

    1) 메시지에서 지갑 모으기   계기 ① 보류(바로), ② 거래(재분석 간격 적용), ③ 동결
    2) 계기 ③                   새로 동결·블랙리스트된 지갑의 상대를 L5 대기 목록에 넣기
    3) 분석할 지갑 고르기        보류 → 간격이 지난 대기 지갑 → 새 거래 순
    4) 빼기                     화이트리스트 L, 동결, 블랙리스트 지갑
    5) 계산                     조회(queries.py) → 판단(rules.py). 직원이 동결 해제한 지갑은 해제 이후 거래만
    6) 처리 표시                 재분석 간격 TTL, 대기 목록 정리

기간·기준값은 config.py, 조회는 queries.py, L1~L8 판단은 rules.py.
Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import redis
from pymongo import MongoClient

import queries
import rules
from common.keys import (BLACKLIST_KEY, CONFIRMED_FROZEN_KEY, FREEZE_TOPIC, FROZEN_KEY, HOLD_TOPIC, TRANSFER_TOPIC,
                         UNFROZEN_AT_KEY)
from config import (
    ANALYZED_PREFIX,
    BLACKLIST_SEEN_KEY,
    CONFIRMED_SEEN_KEY,
    L5_EXPOSURE,
    MAX_WALLETS_PER_BATCH,
    PENDING_KEY,
    PERIOD,
    RECHECK_KEY,
    RECHECK_PER_BATCH,
    TICK_TOPIC,
    WHITELIST_L_KEY,
)

logger = logging.getLogger("aegis.detector")


class Detector:
    def __init__(self, mongo_uri, mongo_db, redis_host, redis_port=6379):
        self.db = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000, tz_aware=True)[mongo_db]
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, socket_timeout=5)

    # ================= 배치 처리 =================

    def process(self, messages, now=None, batch_id=None):
        """messages: [(토픽, JSON 문자열), ...]. 돌려주는 값: (경보 목록, 통계).
        batch_id: Spark 배치 번호. 경보에 남겨 '어느 배치에서 계산한 값인지' 추적한다."""
        now = now or datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)

        # 1) 메시지에서 지갑 모으기
        normal, urgent, frozen, bad = self._collect(messages)

        # 2) 계기 ③: 새로 동결되거나 블랙리스트에 오른 지갑의 상대를 L5 대기 목록에 넣기
        #    confirmed 모드: 자동 동결 이벤트는 쓰지 않고, 새로 '확인된 동결'만 (L5 근거와 같은 기준)
        if L5_EXPOSURE == "confirmed":
            frozen = self._new_members(CONFIRMED_FROZEN_KEY, CONFIRMED_SEEN_KEY)
        frozen |= self._new_blacklist()
        queued = self._queue_recheck(frozen, now, now_ms) if frozen else 0

        # 3) 분석할 지갑 고르기
        full, cooling = self._pick_full(normal, urgent, now_ms)
        recheck = self.r.zrange(RECHECK_KEY, 0, RECHECK_PER_BATCH - 1)

        # 4) 면제, 동결, 블랙리스트 지갑 빼기
        targets = self._exclude(set(full) | set(recheck))
        full_targets = [w for w in full if w in targets]
        recheck_targets = [w for w in recheck if w in targets and w not in set(full_targets)]

        # 5) 계산: 전체 분석(L1~L8) + 계기 ③ 상대 지갑(L5만). 해제한 지갑은 따로, 해제 시각부터 본다.
        unfrozen = self._unfrozen_at(full_targets + recheck_targets, now)
        alerts = []
        for group, start in self._by_start(full_targets, unfrozen):
            alerts += self._analyze_full(group, now, start)
        for group, start in self._by_start(recheck_targets, unfrozen):
            alerts += self._analyze_l5(group, now, start)

        for a in alerts:
            a["batch_id"] = batch_id

        # 6) 처리한 지갑 표시
        pipe = self.r.pipeline(transaction=False)
        for w in full_targets:
            pipe.set(ANALYZED_PREFIX + w, 1, px=PERIOD["cooldown_ms"])
        if full:
            pipe.zrem(PENDING_KEY, *full)
        if recheck:
            pipe.zrem(RECHECK_KEY, *recheck)
        pipe.zcard(PENDING_KEY)
        pipe.zcard(RECHECK_KEY)
        *_, pending_size, recheck_size = pipe.execute()

        stats = {
            "messages": len(messages),
            "bad": bad,
            "analyzed": len(full_targets),
            "cooling": cooling,
            "l5_recheck": len(recheck_targets),
            "recheck_queued": queued,
            "l5_exposure": L5_EXPOSURE,
            "alerts": len(alerts),
            "pending_size": pending_size,
            "recheck_size": recheck_size,
        }
        return alerts, stats

    # ---------- 1) 메시지 → 지갑 ----------

    @staticmethod
    def _collect(messages):
        """(계기 ② 거래 지갑, 계기 ① 보류 지갑, 계기 ③ 동결 지갑, 읽지 못한 메시지 수)."""
        normal, urgent, frozen = set(), set(), set()
        bad = 0
        for topic, value in messages:
            if topic == TICK_TOPIC or value is None:
                continue
            try:
                msg = json.loads(value)
            except ValueError:
                bad += 1
                continue
            if topic == TRANSFER_TOPIC:
                # 계기 ②: 보낸 지갑과 받은 지갑 (L8 순환은 마지막에 받은 지갑에서 찾는다)
                normal.update(w for w in (msg.get("from"), msg.get("to")) if w)
            elif topic == HOLD_TOPIC and msg.get("wallet"):
                # 계기 ①: 보류는 간격 없이 바로, R07 차단 시도는 일반 거래처럼 간격 적용
                (urgent if msg.get("event") == "HOLD" else normal).add(msg["wallet"])
            elif topic == FREEZE_TOPIC and msg.get("wallet"):
                # 계기 ③은 동결만. 직원의 동결 해제(UNFREEZE)는 상대 지갑을 다시 볼 이유가 없다.
                if msg.get("event", "FREEZE") == "FREEZE":
                    frozen.add(msg["wallet"])
            else:
                bad += 1
        return normal, urgent, frozen, bad

    # ---------- 2) 계기 ③ ----------

    def _new_blacklist(self):
        """블랙리스트는 스크립트로 Redis에 직접 넣으므로 Kafka 대신 이전 목록과 비교해 새 주소를 찾는다."""
        return self._new_members(BLACKLIST_KEY, BLACKLIST_SEEN_KEY)

    def _new_members(self, key, seen_key):
        """Set에 새로 들어온 주소. 이전 목록(seen_key)과 비교한다 (빠진 주소는 seen에서도 뺀다)."""
        new = self.r.sdiff(key, seen_key)
        gone = self.r.sdiff(seen_key, key)
        pipe = self.r.pipeline(transaction=False)
        if new:
            pipe.sadd(seen_key, *new)
        if gone:
            pipe.srem(seen_key, *gone)
        pipe.execute()
        return set(new)

    def _queue_recheck(self, wallets, now, now_ms):
        """동결·블랙리스트 지갑과 최근 2주 안에 거래한 상대 지갑을 L5 대기 목록에 넣는다 (Mongo 집계 1번)."""
        cps = queries.counterparties(self.db, wallets, now)
        for i in range(0, len(cps), 1000):
            self.r.zadd(RECHECK_KEY, {w: now_ms for w in cps[i:i + 1000]}, nx=True)
        return len(cps)

    # ---------- 3) 분석할 지갑 고르기 ----------

    def _pick_full(self, normal, urgent, now_ms):
        """간격 안에 분석한 지갑은 대기 목록으로 미루고, 때가 된 대기 지갑을 꺼낸다."""
        candidates = sorted(normal - urgent)
        ready, cooling = [], 0
        if candidates:
            pipe = self.r.pipeline(transaction=False)
            for w in candidates:
                pipe.pttl(ANALYZED_PREFIX + w)
            delays = pipe.execute()
            later = {}
            for w, ttl in zip(candidates, delays):
                if ttl and ttl > 0:
                    later[w] = now_ms + ttl
                else:
                    ready.append(w)
            if later:
                # 이미 대기 중이면 처음 정한 시각을 유지한다
                self.r.zadd(PENDING_KEY, later, nx=True)
                cooling = len(later)

        due = self.r.zrangebyscore(PENDING_KEY, "-inf", now_ms, start=0, num=MAX_WALLETS_PER_BATCH)
        # 보류(계기 ①)를 먼저, 그다음 대기 목록, 새 거래 순
        ordered = list(dict.fromkeys(sorted(urgent) + due + ready))
        full, overflow = ordered[:MAX_WALLETS_PER_BATCH], ordered[MAX_WALLETS_PER_BATCH:]
        if overflow:
            self.r.zadd(PENDING_KEY, {w: now_ms for w in overflow}, nx=True)
        return full, cooling

    # ---------- 4) 빼기 ----------

    def _exclude(self, wallets):
        """화이트리스트 L, 동결, 블랙리스트 지갑을 뺀 나머지."""
        if not wallets:
            return set()
        ws = sorted(wallets)
        pipe = self.r.pipeline(transaction=False)
        pipe.smismember(WHITELIST_L_KEY, ws)
        pipe.smismember(FROZEN_KEY, ws)
        pipe.smismember(BLACKLIST_KEY, ws)
        white, frozen, black = pipe.execute()
        return {w for w, a, b, c in zip(ws, white, frozen, black) if not (a or b or c)}

    # ---------- 5) 계산: 조회 → 판단 ----------

    def _unfrozen_at(self, wallets, now):
        """직원이 최근 2주 안에 동결 해제한 지갑 → 해제 시각. 이 지갑은 해제 이후 거래만 본다 (재동결 방지)."""
        if not wallets:
            return {}
        since = now - PERIOD["long"]
        out = {}
        for w, ms in zip(wallets, self.r.hmget(UNFROZEN_AT_KEY, wallets)):
            if ms:
                at = datetime.fromtimestamp(int(ms) / 1000, timezone.utc)
                if at > since:
                    out[w] = at
        return out

    @staticmethod
    def _by_start(wallets, unfrozen):
        """(지갑 묶음, 조회 시작 시각). 보통 지갑은 한 번에, 해제한 지갑은 하나씩 해제 시각부터."""
        normal = [w for w in wallets if w not in unfrozen]
        if normal:
            yield normal, None
        for w in wallets:
            if w in unfrozen:
                yield [w], unfrozen[w]

    def _analyze_full(self, wallets, now, start=None):
        """L1~L8 전부. 조회는 L2·L3·L6이 1번, L4·L8이 1번을 같이 쓴다. start: 동결 해제 시각."""
        alerts = []
        alerts += rules.l1(queries.hold_summary(self.db, wallets, now, start), now)
        history = now - start if start else None
        alerts += rules.l2_l3_l6(queries.sent_summary(self.db, wallets, now, start), now, history)
        alerts += self._analyze_l5(wallets, now, start)
        alerts += rules.l7(queries.new_counterparties(self.db, wallets, now, start), now)
        big = queries.big_transfers(self.db, wallets, now, start)
        alerts += rules.l4(wallets, big, now)
        white = self.r.smembers(WHITELIST_L_KEY)
        alerts += rules.l8(wallets, big, white, now,
                           transfers_from=lambda senders, since, to=None: queries.transfers_from(self.db, senders, since, to))
        return alerts

    def _analyze_l5(self, wallets, now, start=None):
        """L5: 블랙리스트·동결 지갑(분석 지갑 자신은 제외)과 주고받은 거래.
        동결 지갑 = 직원이 확인한 동결만 (L5_EXPOSURE=all 이면 모든 동결)."""
        black = self.r.smembers(BLACKLIST_KEY)
        frozen = self.r.smembers(CONFIRMED_FROZEN_KEY if L5_EXPOSURE == "confirmed" else FROZEN_KEY)
        risky = sorted((black | frozen) - set(wallets))
        if not risky:
            return []
        inflow, outflow = queries.risky_flows(self.db, wallets, risky, now, start)
        return rules.l5(inflow, outflow, black, now)
