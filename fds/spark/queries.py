"""Mongo 조회: 분석할 지갑 여러 개를 한 번에 조회한다. 판단은 rules.py.

조회 횟수가 부하를 정하므로 룰마다 따로 조회하지 않는다.
L2·L3·L6은 sent_summary() 한 번을, L4·L8은 big_transfers() 한 번을 같이 쓴다.
금액은 여기서 float로 바꿔서 돌려준다 (Mongo에는 Decimal128로 저장됨).

start: 직원이 동결 해제한 지갑은 해제 시각(start) 이후 기록만 본다 (해제 전 거래로 다시 동결되지 않게).
"""
from bson.decimal128 import Decimal128

from config import BIG_AMOUNT, L1_HOLD_SIDE, L1_MIN_HOLDS, L7_MIN_NEW, MAX_HASHES, PERIOD, SPLIT_MAX, SPLIT_MIN


def num(value):
    """Mongo 금액(Decimal128, int, float, None)을 float로. None은 0."""
    if value is None:
        return 0.0
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value)


def _since(now, span, start=None):
    """조회 시작 시각: now - span. 동결 해제 시각(start)이 더 늦으면 그 시각부터."""
    t = now - span
    return max(t, start) if start else t


# ---------- 계기 ③: 동결·블랙리스트 지갑의 상대 ----------

def counterparties(db, wallets, now):
    """이 지갑들과 최근 2주 안에 거래한 상대 지갑 (이 지갑들 자신은 뺀다)."""
    ws = sorted(wallets)
    rows = db.transfers.aggregate([
        {"$match": {
            "status": "ACCEPTED",
            "timestamp": {"$gte": now - PERIOD["long"]},
            "$or": [{"from": {"$in": ws}}, {"to": {"$in": ws}}],
        }},
        {"$project": {"cp": {"$cond": [{"$in": ["$from", ws]}, "$to", "$from"]}}},
        {"$group": {"_id": "$cp"}},
    ])
    return [row["_id"] for row in rows if row["_id"] not in wallets]


# ---------- L1: 보류 기록 ----------

def hold_summary(db, wallets, now, start=None):
    """지갑별 2주 보류 횟수 (L1 기준 이상인 지갑만)."""
    match_q = {"wallet": {"$in": wallets}, "held_at": {"$gte": _since(now, PERIOD["long"], start)}}
    if L1_HOLD_SIDE == "sender":
        match_q["side"] = "SENDER"
    return list(db.fds_holds.aggregate([
        {"$match": match_q},
        {"$group": {
            "_id": "$wallet",
            "holds": {"$sum": 1},
            "rules": {"$addToSet": "$rule"},
            "hashes": {"$push": "$tx_hash"},
            "first": {"$min": "$held_at"},
            "last": {"$max": "$held_at"},
        }},
        {"$match": {"holds": {"$gte": L1_MIN_HOLDS}}},
    ]))


# ---------- L2·L3·L6: 보낸 거래 요약 (조회 1번) ----------

def sent_summary(db, wallets, now, start=None):
    """지갑별 보낸 거래 요약: 2주 건수, 하루 건수·합계·최대, 900~990 건수·합계. Mongo가 숫자만 돌려준다."""
    t_day = _since(now, PERIOD["day"], start)
    in_day = {"$gte": ["$timestamp", t_day]}
    is_split = {"$and": [{"$gte": ["$amount", SPLIT_MIN]}, {"$lte": ["$amount", SPLIT_MAX]}]}

    def only_hashes(field):
        return {"$slice": [{"$filter": {"input": field, "cond": {"$ne": ["$$this", None]}}}, MAX_HASHES]}

    rows = db.transfers.aggregate([
        {"$match": {"from": {"$in": wallets}, "status": "ACCEPTED",
                    "timestamp": {"$gte": _since(now, PERIOD["long"], start)}}},
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
    out = []
    for row in rows:
        for field in ("day_sum", "day_max", "split_sum"):
            row[field] = num(row[field])
        out.append(row)
    return out


# ---------- L7: 처음 보낸 상대 ----------

def new_counterparties(db, wallets, now, start=None):
    """지갑별로 하루 안에 처음 보낸 상대 수 (2주 기록 기준 '처음', L7 기준 이상인 지갑만).
    '처음'인지는 해제 전 기록까지 2주 전체로 판단하고, 세는 건 해제 이후에 처음 보낸 상대만."""
    return list(db.transfers.aggregate([
        {"$match": {"from": {"$in": wallets}, "status": "ACCEPTED", "timestamp": {"$gte": now - PERIOD["long"]}}},
        {"$group": {"_id": {"w": "$from", "c": "$to"}, "first": {"$min": "$timestamp"}}},
        {"$match": {"first": {"$gte": _since(now, PERIOD["day"], start)}}},
        {"$group": {
            "_id": "$_id.w",
            "new_count": {"$sum": 1},
            "counterparties": {"$push": "$_id.c"},
            "first": {"$min": "$first"},
            "last": {"$max": "$first"},
        }},
        {"$match": {"new_count": {"$gte": L7_MIN_NEW}}},
    ]))


# ---------- L5: 위험 지갑과 주고받은 거래 ----------

def risky_flows(db, wallets, risky, now, start=None):
    """(받은 거래 요약, 보낸 거래 요약). 각 줄은 _id={w: 분석 지갑, cp: 위험 지갑}. 통과된 거래만."""
    base = {"status": "ACCEPTED", "timestamp": {"$gte": _since(now, PERIOD["long"], start)}}
    group = {
        "count": {"$sum": 1},
        "amount": {"$sum": "$amount"},
        "hashes": {"$push": "$tx_hash"},
        "first": {"$min": "$timestamp"},
        "last": {"$max": "$timestamp"},
    }
    inflow = db.transfers.aggregate([
        {"$match": dict(base, to={"$in": wallets}, **{"from": {"$in": risky}})},
        {"$group": dict(group, _id={"w": "$to", "cp": "$from"})},
    ])
    outflow = db.transfers.aggregate([
        {"$match": dict(base, to={"$in": risky}, **{"from": {"$in": wallets}})},
        {"$group": dict(group, _id={"w": "$from", "cp": "$to"})},
    ])
    return _with_amount(inflow), _with_amount(outflow)


def _with_amount(rows):
    out = []
    for row in rows:
        row["amount"] = num(row["amount"])
        out.append(row)
    return out


# ---------- L4·L8: 거래 원본 ----------

def _transfer(d):
    return {"from": d["from"], "to": d["to"], "amount": num(d["amount"]), "ts": d["timestamp"], "tx_hash": d["tx_hash"]}


_FIELDS = {"_id": 0, "from": 1, "to": 1, "amount": 1, "timestamp": 1, "tx_hash": 1}


def big_transfers(db, wallets, now, start=None):
    """L4, L8용: 분석 지갑이 받은 1,000 USDT 이상 거래 + 분석 지갑이 보낸 거래.
    보낸 거래는 금액 제한 없이 읽는다 (1,000을 받아 900만 넘겨도 L4 경유로 보기 위해)."""
    cursor = db.transfers.find(
        {
            "status": "ACCEPTED",
            "timestamp": {"$gte": _since(now, PERIOD["long"], start)},
            "$or": [
                {"from": {"$in": wallets}},
                {"to": {"$in": wallets}, "amount": {"$gte": BIG_AMOUNT}},
            ],
        },
        _FIELDS,
    )
    return [_transfer(d) for d in cursor]


def transfers_from(db, senders, since, to=None):
    """L8 2·3번째 거래: 이 지갑들이 since 이후 보낸 1,000 이상 거래 (to를 주면 그 지갑들에게 보낸 것만)."""
    if not senders:
        return []
    query = {"status": "ACCEPTED", "amount": {"$gte": BIG_AMOUNT}, "timestamp": {"$gte": since},
             "from": {"$in": senders}}
    if to is not None:
        query["to"] = {"$in": to}
    return [_transfer(d) for d in db.transfers.find(query, _FIELDS)]
