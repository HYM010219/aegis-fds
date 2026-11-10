"""2단계 장기 분석: 최근 2주(운영) 기록으로 L1~L8을 계산한다.

stream.py가 10초마다 Kafka 메시지를 넘기면
1) 분석할 지갑을 고르고 (계기 ① 보류, ② 거래, ③ 동결·블랙리스트)
2) Mongo에서 그 지갑들의 기록을 한 번에 조회해
3) 걸린 지갑의 경보 목록을 돌려준다. 경보는 stream.py가 fds.alerts.v1로 보낸다.

Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
"""
from __future__ import annotations

import json
import logging
import os
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import redis
from bson.decimal128 import Decimal128
from pymongo import MongoClient

logger = logging.getLogger("aegis.detector")

TRANSFER_TOPIC = os.getenv("TRANSFER_TOPIC", "transfer.events.v1")
HOLD_TOPIC = os.getenv("HOLD_TOPIC", "fds.holds.v1")
FREEZE_TOPIC = os.getenv("FREEZE_TOPIC", "fds.freezes.v1")
TICK_TOPIC = "tick"  # 거래가 없어도 대기 목록을 처리하도록 stream.py가 넣는 시계 신호

# ---------- 기간 설정 (PERIOD_PROFILE=prod|demo) ----------
PROFILES = {
    #        장기(L1·L3·L4·L5·L6·L7)  하루(L2·L6·L7·L8)      경유(L4)               재분석 간격(계기 ②)
    "prod": {"long": timedelta(days=14), "day": timedelta(hours=24), "hour": timedelta(hours=1), "cooldown_ms": 60_000},
    "demo": {"long": timedelta(minutes=20), "day": timedelta(minutes=5), "hour": timedelta(minutes=1), "cooldown_ms": 15_000},
}
PROFILE_NAME = os.getenv("PERIOD_PROFILE", "prod")
if PROFILE_NAME not in PROFILES:
    raise ValueError(f"PERIOD_PROFILE은 prod 또는 demo: {PROFILE_NAME}")
PERIOD = PROFILES[PROFILE_NAME]

# ---------- 룰 기준 (초안, 시험 결과로 조정) ----------
L1_MIN_HOLDS = 3              # 2주 안에 1단계 보류 3회 이상
L2_MIN_DAY_SUM = 50_000       # 하루 누적 송금 5만 이상, 모든 거래는 5만 미만
L3_MIN_SPLITS = 10            # 2주 안에 900~990 송금 10회 이상
SPLIT_MIN, SPLIT_MAX = 900, 990
BIG_AMOUNT = 1_000            # L4는 1,000 이상 받은 거래, L8은 1,000 이상 거래만 따라간다 (부하 제한)
L4_FORWARD_RATIO = 0.8        # 받은 금액의 80% 이상을 1시간 안에 다시 송금
L4_MIN_TIMES = 3              # 2주 안에 3회 이상
L5_MEDIUM_SUM = 10_000        # 동결 지갑과 2주 누적 1만 이상이면 MEDIUM
L6_RATIO = 10                 # 오늘 거래 수가 평소 하루 평균의 10배 이상
L6_MIN_COUNT = 20             # 최소 20건
L7_MIN_NEW = 20               # 하루 안에 처음 거래하는 상대 20개 이상
L8_RETURN_RATIO = 0.8         # 처음 보낸 금액의 80% 이상이 돌아옴
L8_MAX_FANOUT = 200           # 상대가 이보다 많은 지갑은 더 넓혀 가지 않는다 (거래소, 허브)

# ---------- 처리량 제한 ----------
MAX_WALLETS_PER_BATCH = 2_000  # 한 배치에서 전체 분석할 최대 지갑 수 (넘으면 다음 배치로)
RECHECK_PER_BATCH = 500        # 계기 ③: 한 배치에서 L5를 다시 계산할 상대 지갑 수
MAX_HASHES = 50                # 경보에 담는 거래 해시 수

RULE_NAMES = {
    "L1": "REPEATED_HOLD",
    "L2": "SPLIT_DAILY_SUM",
    "L3": "LONG_SMURFING",
    "L4": "PASS_THROUGH",
    "L5": "RISK_EXPOSURE",
    "L6": "ACTIVITY_SPIKE",
    "L7": "NEW_COUNTERPARTY_BURST",
    "L8": "CIRCULAR_FLOW",
}

# ---------- Redis 키 (fds/app과 같은 값) ----------
FROZEN_KEY = "fds:frozen_wallets"
BLACKLIST_KEY = "fds:blacklist_wallets"
WHITELIST_L_KEY = "fds:whitelist:L"          # 2단계 분석 면제 (거래소 등)
ANALYZED_PREFIX = "fds:analyzed:"            # 최근에 분석한 지갑 (재분석 간격 TTL)
PENDING_KEY = "fds:analyze_pending"          # 간격 때문에 미룬 지갑 (점수 = 분석할 시각 ms)
RECHECK_KEY = "fds:recheck_l5"               # 계기 ③: L5를 다시 볼 상대 지갑 (점수 = 넣은 시각 ms)
BLACKLIST_SEEN_KEY = "fds:blacklist_seen"    # 이미 계기 ③을 처리한 블랙리스트


def _num(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value)


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def _alert(rule, wallet, hashes, first, last, evidence, now, risk=None, counterparty=None):
    """경보 소비자가 읽는 형식 (fds/app/alert_consumer.py)."""
    unique = sorted({h for h in hashes if h})
    return {
        "rule_id": rule,
        "rule_name": RULE_NAMES[rule],
        "violator_address": wallet,
        "tx_hashes": unique[:MAX_HASHES],
        "first_tx_at": _iso(first or now),
        "last_tx_at": _iso(last or now),
        "evidence": evidence,
        "risk_level": risk,
        "counterparty_address": counterparty,
        "period_profile": PROFILE_NAME,
    }


class Detector:
    def __init__(self, mongo_uri, mongo_db, redis_host, redis_port=6379):
        self.db = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000, tz_aware=True)[mongo_db]
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, socket_timeout=5)

    # ================= 배치 처리 =================

    def process(self, messages, now=None):
        """messages: [(토픽, JSON 문자열), ...]. 돌려주는 값: (경보 목록, 통계)."""
        now = now or datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)

        # 1) 메시지에서 지갑 모으기
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
                frozen.add(msg["wallet"])
            else:
                bad += 1

        # 2) 계기 ③: 새로 동결되거나 블랙리스트에 오른 지갑의 상대를 L5 대기 목록에 넣기
        frozen |= self._new_blacklist()
        queued = self._queue_recheck(frozen, now, now_ms) if frozen else 0

        # 3) 분석할 지갑 고르기
        full, cooling = self._pick_full(normal, urgent, now_ms)
        recheck = self.r.zrange(RECHECK_KEY, 0, RECHECK_PER_BATCH - 1)

        # 4) 면제, 동결, 블랙리스트 지갑 빼기
        targets = self._exclude(set(full) | set(recheck))
        full_targets = [w for w in full if w in targets]
        recheck_targets = [w for w in recheck if w in targets and w not in set(full_targets)]

        # 5) 계산
        alerts = []
        if full_targets:
            alerts += self._analyze_full(full_targets, now)
        if recheck_targets:
            alerts += self._l5(recheck_targets, now)

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
            "alerts": len(alerts),
            "pending_size": pending_size,
            "recheck_size": recheck_size,
        }
        return alerts, stats

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

    def _exclude(self, wallets):
        if not wallets:
            return set()
        ws = sorted(wallets)
        pipe = self.r.pipeline(transaction=False)
        pipe.smismember(WHITELIST_L_KEY, ws)
        pipe.smismember(FROZEN_KEY, ws)
        pipe.smismember(BLACKLIST_KEY, ws)
        white, frozen, black = pipe.execute()
        return {w for w, a, b, c in zip(ws, white, frozen, black) if not (a or b or c)}

    def _new_blacklist(self):
        """블랙리스트는 스크립트로 Redis에 직접 넣으므로 Kafka 대신 이전 목록과 비교해 새 주소를 찾는다."""
        new = self.r.sdiff(BLACKLIST_KEY, BLACKLIST_SEEN_KEY)
        gone = self.r.sdiff(BLACKLIST_SEEN_KEY, BLACKLIST_KEY)
        pipe = self.r.pipeline(transaction=False)
        if new:
            pipe.sadd(BLACKLIST_SEEN_KEY, *new)
        if gone:
            pipe.srem(BLACKLIST_SEEN_KEY, *gone)
        pipe.execute()
        return set(new)

    def _queue_recheck(self, wallets, now, now_ms):
        """동결·블랙리스트 지갑과 최근 2주 안에 거래한 상대 지갑을 찾는다 (Mongo 집계 1번)."""
        ws = sorted(wallets)
        rows = self.db.transfers.aggregate([
            {"$match": {
                "status": "ACCEPTED",
                "timestamp": {"$gte": now - PERIOD["long"]},
                "$or": [{"from": {"$in": ws}}, {"to": {"$in": ws}}],
            }},
            {"$project": {"cp": {"$cond": [{"$in": ["$from", ws]}, "$to", "$from"]}}},
            {"$group": {"_id": "$cp"}},
        ])
        counterparties = [row["_id"] for row in rows if row["_id"] not in wallets]
        for i in range(0, len(counterparties), 1000):
            self.r.zadd(RECHECK_KEY, {w: now_ms for w in counterparties[i:i + 1000]}, nx=True)
        return len(counterparties)

    # ================= 룰 계산 =================

    def _analyze_full(self, wallets, now):
        alerts = []
        alerts += self._l1(wallets, now)
        alerts += self._l2_l3_l6(wallets, now)
        alerts += self._l5(wallets, now)
        alerts += self._l7(wallets, now)
        big = self._big_transfers(wallets, now)
        alerts += self._l4(wallets, big, now)
        alerts += self._l8(wallets, big, now)
        return alerts

    def _l1(self, wallets, now):
        """L1 반복 보류: 2주 안에 1단계 보류 3회 이상."""
        rows = self.db.fds_holds.aggregate([
            {"$match": {"wallet": {"$in": wallets}, "held_at": {"$gte": now - PERIOD["long"]}}},
            {"$group": {
                "_id": "$wallet",
                "holds": {"$sum": 1},
                "rules": {"$addToSet": "$rule"},
                "hashes": {"$push": "$tx_hash"},
                "first": {"$min": "$held_at"},
                "last": {"$max": "$held_at"},
            }},
            {"$match": {"holds": {"$gte": L1_MIN_HOLDS}}},
        ])
        return [
            _alert("L1", row["_id"], row["hashes"], row["first"], row["last"],
                   {"hold_count": row["holds"], "hold_rules": sorted(row["rules"])}, now)
            for row in rows
        ]

    def _l2_l3_l6(self, wallets, now):
        """보낸 거래 요약 하나로 L2, L3, L6을 함께 계산한다 (Mongo가 지갑별 숫자만 돌려줌)."""
        t_day = now - PERIOD["day"]
        in_day = {"$gte": ["$timestamp", t_day]}
        is_split = {"$and": [{"$gte": ["$amount", SPLIT_MIN]}, {"$lte": ["$amount", SPLIT_MAX]}]}

        def only_hashes(field):
            return {"$slice": [{"$filter": {"input": field, "cond": {"$ne": ["$$this", None]}}}, MAX_HASHES]}

        rows = self.db.transfers.aggregate([
            {"$match": {"from": {"$in": wallets}, "status": "ACCEPTED", "timestamp": {"$gte": now - PERIOD["long"]}}},
            {"$group": {
                "_id": "$from",
                "long_count": {"$sum": 1},
                "day_count": {"$sum": {"$cond": [in_day, 1, 0]}},
                "day_sum": {"$sum": {"$cond": [in_day, "$amount", 0]}},
                "day_max": {"$max": {"$cond": [in_day, "$amount", None]}},
                "day_first": {"$min": {"$cond": [in_day, "$timestamp", None]}},
                "day_last": {"$max": {"$cond": [in_day, "$timestamp", None]}},
                "day_hashes": {"$push": {"$cond": [in_day, "$tx_hash", None]}},
                "split_count": {"$sum": {"$cond": [is_split, 1, 0]}},
                "split_sum": {"$sum": {"$cond": [is_split, "$amount", 0]}},
                "split_first": {"$min": {"$cond": [is_split, "$timestamp", None]}},
                "split_last": {"$max": {"$cond": [is_split, "$timestamp", None]}},
                "split_hashes": {"$push": {"$cond": [is_split, "$tx_hash", None]}},
            }},
            {"$project": {
                "long_count": 1, "day_count": 1, "day_sum": 1, "day_max": 1, "day_first": 1, "day_last": 1,
                "split_count": 1, "split_sum": 1, "split_first": 1, "split_last": 1,
                "day_hashes": only_hashes("$day_hashes"),
                "split_hashes": only_hashes("$split_hashes"),
            }},
        ])

        days = PERIOD["long"] / PERIOD["day"]
        alerts = []
        for row in rows:
            w = row["_id"]
            day_sum, day_max = _num(row["day_sum"]), _num(row["day_max"])

            # L2 쪼개서 대량 송금 (R05 회피)
            if day_sum >= L2_MIN_DAY_SUM and day_max < L2_MIN_DAY_SUM:
                alerts.append(_alert("L2", w, row["day_hashes"], row["day_first"], row["day_last"], {
                    "day_sum": day_sum, "day_max": day_max, "day_count": row["day_count"],
                }, now))

            # L3 장기 쪼개기 (R02 회피)
            if row["split_count"] >= L3_MIN_SPLITS:
                alerts.append(_alert("L3", w, row["split_hashes"], row["split_first"], row["split_last"], {
                    "split_count": row["split_count"], "split_sum": _num(row["split_sum"]),
                }, now))

            # L6 급격한 활동 증가: 오늘을 뺀 나머지 기간의 하루 평균과 비교
            today = row["day_count"]
            baseline = (row["long_count"] - today) / max(days - 1, 1)
            if today >= L6_MIN_COUNT and today >= L6_RATIO * baseline:
                alerts.append(_alert("L6", w, row["day_hashes"], row["day_first"], row["day_last"], {
                    "today_count": today, "baseline_daily_avg": round(baseline, 2),
                }, now))
        return alerts

    def _l7(self, wallets, now):
        """L7 새 상대 집중: 하루 안에 처음 보내는 상대 20개 이상 (2주 기록 기준 '처음')."""
        rows = self.db.transfers.aggregate([
            {"$match": {"from": {"$in": wallets}, "status": "ACCEPTED", "timestamp": {"$gte": now - PERIOD["long"]}}},
            {"$group": {"_id": {"w": "$from", "c": "$to"}, "first": {"$min": "$timestamp"}}},
            {"$match": {"first": {"$gte": now - PERIOD["day"]}}},
            {"$group": {
                "_id": "$_id.w",
                "new_count": {"$sum": 1},
                "counterparties": {"$push": "$_id.c"},
                "first": {"$min": "$first"},
                "last": {"$max": "$first"},
            }},
            {"$match": {"new_count": {"$gte": L7_MIN_NEW}}},
        ])
        return [
            _alert("L7", row["_id"], [], row["first"], row["last"], {
                "new_counterparty_count": row["new_count"],
                "new_counterparties": sorted(row["counterparties"])[:MAX_HASHES],
            }, now)
            for row in rows
        ]

    def _l5(self, wallets, now):
        """L5 위험 지갑 접촉 (금액·방향 등급). 통과된 거래만 센다 (R07로 막힌 시도는 제외).

        CRITICAL: 블랙리스트 지갑에게서 직접 받음
        HIGH:     블랙리스트 지갑에게 보냄
        MEDIUM:   동결 지갑과 2주 누적 1만 USDT 이상 (주고받은 합계)
        """
        black = self.r.smembers(BLACKLIST_KEY)
        frozen = self.r.smembers(FROZEN_KEY)
        risky = sorted((black | frozen) - set(wallets))
        if not risky:
            return []

        base = {"status": "ACCEPTED", "timestamp": {"$gte": now - PERIOD["long"]}}
        group = {
            "count": {"$sum": 1},
            "amount": {"$sum": "$amount"},
            "hashes": {"$push": "$tx_hash"},
            "first": {"$min": "$timestamp"},
            "last": {"$max": "$timestamp"},
        }
        inflow = self.db.transfers.aggregate([
            {"$match": dict(base, to={"$in": wallets}, **{"from": {"$in": risky}})},
            {"$group": dict(group, _id={"w": "$to", "cp": "$from"})},
        ])
        outflow = self.db.transfers.aggregate([
            {"$match": dict(base, to={"$in": risky}, **{"from": {"$in": wallets}})},
            {"$group": dict(group, _id={"w": "$from", "cp": "$to"})},
        ])

        per_wallet = defaultdict(lambda: {"in_black": [], "out_black": [], "frozen_sum": 0.0,
                                          "hashes": [], "first": None, "last": None})
        for direction, rows in (("in", inflow), ("out", outflow)):
            for row in rows:
                info = per_wallet[row["_id"]["w"]]
                cp, amount = row["_id"]["cp"], _num(row["amount"])
                if cp in black:
                    info[f"{direction}_black"].append({"wallet": cp, "count": row["count"], "amount": amount})
                else:
                    info["frozen_sum"] += amount
                info["hashes"] += row["hashes"]
                info["first"] = min(filter(None, [info["first"], row["first"]]))
                info["last"] = max(filter(None, [info["last"], row["last"]]))

        alerts = []
        for w, info in per_wallet.items():
            if info["in_black"]:
                risk = "CRITICAL"
            elif info["out_black"]:
                risk = "HIGH"
            elif info["frozen_sum"] >= L5_MEDIUM_SUM:
                risk = "MEDIUM"
            else:
                continue
            alerts.append(_alert("L5", w, info["hashes"], info["first"], info["last"], {
                "grade": risk,
                "inflow_from_blacklist": info["in_black"],
                "outflow_to_blacklist": info["out_black"],
                "frozen_exposure_sum": round(info["frozen_sum"], 6),
            }, now, risk=risk))
        return alerts

    def _big_transfers(self, wallets, now):
        """L4, L8용 거래 원본: 분석 지갑이 받은 1,000 USDT 이상 거래 + 분석 지갑이 보낸 거래.
        보낸 거래는 금액 제한 없이 읽는다 (1,000을 받아 900만 넘겨도 L4 경유로 보기 위해)."""
        cursor = self.db.transfers.find(
            {
                "status": "ACCEPTED",
                "timestamp": {"$gte": now - PERIOD["long"]},
                "$or": [
                    {"from": {"$in": wallets}},
                    {"to": {"$in": wallets}, "amount": {"$gte": BIG_AMOUNT}},
                ],
            },
            {"_id": 0, "from": 1, "to": 1, "amount": 1, "timestamp": 1, "tx_hash": 1},
        )
        return [
            {"from": d["from"], "to": d["to"], "amount": _num(d["amount"]), "ts": d["timestamp"], "tx_hash": d["tx_hash"]}
            for d in cursor
        ]

    def _l4(self, wallets, big, now):
        """L4 경유 지갑: 받은 금액의 80% 이상을 1시간 안에 다시 송금, 2주 안에 3회 이상 (1,000 이상 거래만)."""
        targets = set(wallets)
        received, sent = defaultdict(list), defaultdict(list)
        for t in big:
            if t["from"] == t["to"]:
                continue
            if t["to"] in targets and t["amount"] >= BIG_AMOUNT:
                received[t["to"]].append(t)
            if t["from"] in targets:
                sent[t["from"]].append(t)

        alerts = []
        for w in wallets:
            ins = sorted(received.get(w, []), key=lambda t: t["ts"])
            outs = sorted(sent.get(w, []), key=lambda t: t["ts"])
            if len(ins) < L4_MIN_TIMES or not outs:
                continue
            out_times = [t["ts"] for t in outs]
            hits = []
            for r in ins:
                lo = bisect_right(out_times, r["ts"])
                hi = bisect_right(out_times, r["ts"] + PERIOD["hour"])
                forwarded = outs[lo:hi]
                total = sum(t["amount"] for t in forwarded)
                if forwarded and total >= L4_FORWARD_RATIO * r["amount"]:
                    hits.append((r, forwarded, total))
            if len(hits) >= L4_MIN_TIMES:
                hashes = [r["tx_hash"] for r, _, _ in hits] + [t["tx_hash"] for _, f, _ in hits for t in f]
                alerts.append(_alert("L4", w, hashes, hits[0][0]["ts"], max(f[-1]["ts"] for _, f, _ in hits), {
                    "pass_through_count": len(hits),
                    "examples": [
                        {"received": r["amount"], "forwarded": round(total, 6), "minutes": round((f[-1]["ts"] - r["ts"]).total_seconds() / 60, 1)}
                        for r, f, total in hits[:5]
                    ],
                }, now))
        return alerts

    def _l8(self, wallets, big, now):
        """L8 순환 거래: A→B→A 또는 A→B→C→A를 하루 안에 시간 순서대로 돌고,
        각 거래 1,000 이상, 처음 보낸 금액의 80% 이상이 돌아옴. 화이트리스트 지갑을 거치면 제외."""
        t_day = now - PERIOD["day"]
        white = self.r.smembers(WHITELIST_L_KEY)
        targets = set(wallets)

        first_legs = defaultdict(list)
        for t in big:
            if (t["from"] in targets and t["ts"] >= t_day and t["amount"] >= BIG_AMOUNT
                    and t["to"] != t["from"] and t["to"] not in white):
                first_legs[t["from"]].append(t)
        if not first_legs:
            return []

        # 2번째 거래: B가 보낸 거래 (상대가 너무 많은 B는 넓혀 가지 않음)
        bs = sorted({t["to"] for legs in first_legs.values() for t in legs})
        second = self._transfers_from(bs, t_day)
        out_degree = defaultdict(set)
        for t in second:
            out_degree[t["from"]].add(t["to"])
        hubs = {b for b, cps in out_degree.items() if len(cps) > L8_MAX_FANOUT}
        by_from = defaultdict(list)
        for t in second:
            if t["from"] not in hubs:
                by_from[t["from"]].append(t)

        # 3번째 거래: C가 A에게 돌려준 거래
        cs = sorted({t["to"] for t in second if t["from"] not in hubs and t["to"] not in white})
        third = self._transfers_from(cs, t_day, to=sorted(targets)) if cs else []
        back = defaultdict(list)
        for t in third:
            back[(t["from"], t["to"])].append(t)

        alerts = []
        for a, legs in first_legs.items():
            found = None
            for l1 in sorted(legs, key=lambda t: t["ts"]):
                need = L8_RETURN_RATIO * l1["amount"]
                for l2 in by_from.get(l1["to"], []):
                    if l2["ts"] <= l1["ts"]:
                        continue
                    c = l2["to"]
                    # A→B→A
                    if c == a and l2["amount"] >= need:
                        found = [l1, l2]
                        break
                    if c in (a, l1["to"]) or c in white:
                        continue
                    # A→B→C→A
                    for l3 in back.get((c, a), []):
                        if l3["ts"] > l2["ts"] and l3["amount"] >= need:
                            found = [l1, l2, l3]
                            break
                    if found:
                        break
                if found:
                    break
            if found:
                path = [found[0]["from"]] + [t["to"] for t in found]
                alerts.append(_alert("L8", a, [t["tx_hash"] for t in found], found[0]["ts"], found[-1]["ts"], {
                    "path": path,
                    "amounts": [t["amount"] for t in found],
                    "return_ratio": round(found[-1]["amount"] / found[0]["amount"], 4),
                    "hops": len(found),
                }, now, counterparty=path[1]))
        return alerts

    def _transfers_from(self, senders, since, to=None):
        if not senders:
            return []
        query = {"status": "ACCEPTED", "amount": {"$gte": BIG_AMOUNT}, "timestamp": {"$gte": since},
                 "from": {"$in": senders}}
        if to is not None:
            query["to"] = {"$in": to}
        cursor = self.db.transfers.find(query, {"_id": 0, "from": 1, "to": 1, "amount": 1, "timestamp": 1, "tx_hash": 1})
        return [
            {"from": d["from"], "to": d["to"], "amount": _num(d["amount"]), "ts": d["timestamp"], "tx_hash": d["tx_hash"]}
            for d in cursor
        ]
