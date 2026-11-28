"""Spark 근거 재계산 (L2·L6·L7): 경보가 뜨면 원본 거래를 Spark로 다시 계산해 근거 묶음(recompute)을 붙인다.

판단은 하지 않는다. 경보 여부·자동 동결은 기존 규칙(queries.py → rules.py)이 정하고, 여기서는
    1) 경보 지갑의 원본 거래(Mongo transfers, 승인된 송금)를 한 번에 꺼내
    2) Spark DataFrame으로 1분 단위 건수·금액·상대 수, 5분 이동 윈도(Spark window(), 1분 간격)와 규칙별 값을 계산하고
         L2  하루(demo 5분) 송금 합계·최대·건수
         L6  하루 송금 수, 장기 송금 수 → 평소 하루 평균
         L7  하루 안에 '처음' 보낸 상대 (장기 이력에 없던 상대)
    3) 판정 값(alert["evidence"])과 항목별로 맞대어
    4) 계산 범위·1분 내역·구성 거래와 함께 alert["recompute"]에 붙인다.
사건(fds_cases.recompute)에 저장되고, 사건 상세·STR 초안이 이 값을 글로 보여준다.

범위는 판정(queries.py)과 같은 정의로 잡는다.
    끝    경보 시각 alerted_at (= 판정의 now). 그 뒤에 들어온 거래는 세지 않고 late_tx_count로만 남긴다
    하루  max(now - PERIOD["day"], 해제 시각)    L2, L6 오늘, L7 '처음 보낸' 시각
    장기  max(now - PERIOD["long"], 해제 시각)   L6 평소
    이력  now - PERIOD["long"] (해제 시각 무관)   L7 '처음'인지 판단
해제 시각 = 직원이 동결 해제한 지갑의 해제 시각 (detector.py가 alert["history_from"]으로 넘김).

시각은 epoch ms(LongType)로 계산한다 (Spark·Python 시간대 변환을 타지 않게).
금액은 Decimal(38, 8)로 더한다 (Mongo Decimal128처럼 정확하게, float 오차 없이).
실패해도 예외를 밖으로 던지지 않는다. 경보는 그대로 나가고 recompute.status만 ERROR가 된다.

Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType, StructField, StructType

from config import (CROSSCHECK_MAX_TXS, CROSSCHECK_MONEY_TOLERANCE, CROSSCHECK_RULES, MAX_HASHES, PERIOD,
                    PROFILE_NAME)

logger = logging.getLogger("aegis.crosscheck")

ENGINE = "spark_recompute_v1"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
ONE_MS = timedelta(milliseconds=1)
CENT = Decimal("0.00000001")  # Decimal(38, 8)
WINDOW_MINUTES, SLIDE_MINUTES = 5, 1  # 5분 이동 윈도, 1분마다 이동
MAX_WINDOWS = 60                      # recompute에 담는 윈도 수 상한 (경보 시각에 가까운 것부터, 최댓값은 따로 남김)

TX_SCHEMA = StructType([
    StructField("wallet", StringType(), False),
    StructField("to", StringType(), True),
    StructField("tx_hash", StringType(), True),
    StructField("amount", DecimalType(38, 8), False),
    StructField("ts", LongType(), False),
])
ALERT_SCHEMA = StructType([
    StructField("k", IntegerType(), False),        # 배치 안 경보 번호
    StructField("wallet", StringType(), False),
    StructField("now", LongType(), False),         # 경보 시각 (ms)
    StructField("day_from", LongType(), False),
    StructField("long_from", LongType(), False),
    StructField("hist_from", LongType(), False),
])

# 규칙별로 맞대는 항목: (evidence 필드, 이름, 비교 방식)
CHECKS = {
    "L2": [("day_sum", "누적 송금액", "money"), ("day_max", "건당 최대 금액", "money"), ("day_count", "송금 건수", "exact")],
    "L6": [("today_count", "오늘 송금 수", "exact"), ("baseline_daily_avg", "평소 하루 평균", "avg")],
    "L7": [("new_counterparty_count", "처음 보낸 상대 수", "exact"), ("new_counterparties", "처음 보낸 상대 목록", "exact")],
}


# ---------- 시각·금액 ----------

def _ms(dt):
    """aware datetime → epoch ms (Mongo처럼 ms 아래는 버림)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - EPOCH) // ONE_MS


def _iso_ms(ms):
    return (EPOCH + timedelta(milliseconds=ms)).isoformat()


def _dec(value):
    """Mongo 금액(Decimal128, int, float, None) → Decimal(8자리)."""
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal128):
        d = value.to_decimal()
    elif isinstance(value, float):
        d = Decimal(repr(value))
    else:
        d = Decimal(value)
    return d.quantize(CENT)


def _num(d):
    return float(d) if d is not None else 0.0


# ---------- 경보 → 계산 범위 ----------

def _bounds(alert):
    """판정과 같은 범위 (queries.py _since와 같은 정의). 값은 모두 ms, days는 L6 평소 계산용."""
    now = datetime.fromisoformat(alert["alerted_at"])
    start = datetime.fromisoformat(alert["history_from"]) if alert.get("history_from") else None

    def since(span):
        t = now - span
        return max(t, start) if start else t

    history = (now - start) if start else PERIOD["long"]   # detector.py → rules.l6 history와 같음
    return {
        "now": _ms(now),
        "day_from": _ms(since(PERIOD["day"])),
        "long_from": _ms(since(PERIOD["long"])),
        "hist_from": _ms(now - PERIOD["long"]),
        "days": history / PERIOD["day"],
    }


# ---------- 원본 꺼내기 ----------

def _fetch(db, wallets, since_ms):
    """경보 지갑들이 보낸 승인 거래 (끝은 자르지 않는다. 경보 시각 뒤 거래는 late_tx_count로 센다)."""
    rows = []
    cursor = db.transfers.find(
        {"from": {"$in": sorted(wallets)}, "status": "ACCEPTED",
         "timestamp": {"$gte": EPOCH + timedelta(milliseconds=since_ms)}},
        {"_id": 0, "from": 1, "to": 1, "tx_hash": 1, "amount": 1, "timestamp": 1},
    )
    for t in cursor:
        if t.get("timestamp") is None:
            continue
        rows.append((t["from"], t.get("to"), t.get("tx_hash"), _dec(t.get("amount")), _ms(t["timestamp"])))
    return rows


# ---------- Spark 계산 ----------

def _compute(spark, keyed, tx_rows):
    """keyed: [(k, alert, bounds)]. 돌려주는 값: 경보 번호별 계산 결과 dict."""
    tx = spark.createDataFrame(tx_rows, TX_SCHEMA)
    al = spark.createDataFrame(
        [(k, a["violator_address"], b["now"], b["day_from"], b["long_from"], b["hist_from"]) for k, a, b in keyed],
        ALERT_SCHEMA,
    )
    # 경보마다 이력 시작 ~ 경보 시각 안의 거래 (장기·하루 범위는 모두 이 안에 있다)
    j = (tx.join(F.broadcast(al), "wallet")
         .where((F.col("ts") >= F.col("hist_from")) & (F.col("ts") <= F.col("now"))))
    in_day = F.col("ts") >= F.col("day_from")
    in_long = F.col("ts") >= F.col("long_from")

    totals = j.groupBy("k").agg(
        F.sum(F.when(in_day, 1).otherwise(0)).alias("day_count"),
        F.sum(F.when(in_day, F.col("amount"))).alias("day_sum"),
        F.max(F.when(in_day, F.col("amount"))).alias("day_max"),
        F.countDistinct(F.when(in_day, F.col("to"))).alias("day_receivers"),  # 범위 안 서로 다른 상대 주소 수
        F.sum(F.when(in_long, 1).otherwise(0)).alias("long_count"),
    ).collect()

    minutes = (j.where(in_day)
               .withColumn("minute", F.floor(F.col("ts") / 60000).cast("long") * 60000)
               .groupBy("k", "minute")
               .agg(F.count(F.lit(1)).alias("count"), F.sum("amount").alias("sum"),
                    F.max("amount").alias("max"), F.countDistinct("to").alias("receivers"))
               .orderBy("k", "minute").collect())

    # 5분 이동 윈도: Spark window()로 범위 안 송금을 5분 구간(1분마다 이동)으로 묶어 건수·금액·서로 다른 상대 수.
    # 시각은 ms → timestamp로 바꿔 쓰고, 구간 시작·끝은 다시 epoch ms로 꺼낸다 (시간대 변환을 타지 않게).
    windows = (j.where(in_day)
               .withColumn("t", F.timestamp_seconds(F.col("ts") / 1000))
               .groupBy("k", F.window("t", f"{WINDOW_MINUTES} minutes", f"{SLIDE_MINUTES} minute"))
               .agg(F.count(F.lit(1)).alias("count"), F.sum("amount").alias("sum"),
                    F.countDistinct("to").alias("receivers"))
               .select("k",
                       (F.col("window.start").cast("long") * 1000).alias("start"),
                       (F.col("window.end").cast("long") * 1000).alias("end"),
                       "count", "sum", "receivers")
               .orderBy("k", "start").collect())

    day_txs = j.where(in_day).select("k", "tx_hash", "to", "amount", "ts").orderBy("k", "ts").collect()

    # L7: 상대별로 이력 안에서 처음 보낸 시각. 그 시각이 하루 범위 안이면 '처음 보낸 상대'
    firsts = (j.groupBy("k", "to", "day_from")
              .agg(F.min("ts").alias("first"), F.min_by("tx_hash", "ts").alias("tx_hash"),
                   F.min_by("amount", "ts").alias("amount"))
              .where(F.col("first") >= F.col("day_from"))
              .orderBy("k", "first").collect())

    out = defaultdict(lambda: {"totals": None, "minutes": [], "windows": [], "day_txs": [], "firsts": []})
    for r in totals:
        out[r["k"]]["totals"] = r
    for r in minutes:
        out[r["k"]]["minutes"].append(r)
    for r in windows:
        out[r["k"]]["windows"].append(r)
    for r in day_txs:
        out[r["k"]]["day_txs"].append(r)
    for r in firsts:
        out[r["k"]]["firsts"].append(r)
    return out


# ---------- 결과 → recompute ----------

def _values(rule, res, b):
    """Spark 계산 → 규칙별 값 (evidence와 같은 필드 이름)과 계산 과정."""
    t = res["totals"]
    day_count = int(t["day_count"]) if t else 0
    long_count = int(t["long_count"]) if t else 0
    if rule == "L2":
        return {"day_sum": _num(t["day_sum"] if t else None), "day_max": _num(t["day_max"] if t else None),
                "day_count": day_count}, {}
    if rule == "L6":
        prior = long_count - day_count
        baseline = prior / max(b["days"] - 1, 1)
        return {"today_count": day_count, "baseline_daily_avg": round(baseline, 2)}, \
            {"long_count": long_count, "prior_count": prior, "days": round(b["days"], 4),
             "formula": "baseline_daily_avg = prior_count / max(days - 1, 1)"}
    if rule == "L7":
        cps = sorted(r["to"] for r in res["firsts"] if r["to"])
        return {"new_counterparty_count": len(res["firsts"]), "new_counterparties": cps[:MAX_HASHES]}, {}
    return {}, {}


def _summary(t):
    """범위 안 송금 횟수·합계·서로 다른 상대 주소 수 (모든 대상 규칙 공통)."""
    if not t:
        return {"send_count": 0, "send_sum": 0.0, "receiver_count": 0}
    return {"send_count": int(t["day_count"]), "send_sum": _num(t["day_sum"]), "receiver_count": int(t["day_receivers"])}


def _windows(rows):
    """5분 이동 윈도 결과. 거래가 있던 구간만 있고, 가장 많이 보낸 구간과 상대가 가장 많았던 구간을 따로 남긴다.
    범위 안 거래만 세므로 범위 처음·끝에 걸친 구간은 일부 시간만 담긴다."""
    items = [{"start": _iso_ms(r["start"]), "end": _iso_ms(r["end"]), "count": int(r["count"]),
              "sum": _num(r["sum"]), "receivers": int(r["receivers"])} for r in rows]
    return {
        "engine": "spark.sql.functions.window",
        "size_minutes": WINDOW_MINUTES,
        "slide_minutes": SLIDE_MINUTES,
        "window_count": len(items),
        # 같은 값이면 먼저 나온(이른) 구간
        "peak_count": max(items, key=lambda x: x["count"]) if items else None,
        "peak_receivers": max(items, key=lambda x: x["receivers"]) if items else None,
        "items": items[-MAX_WINDOWS:],
        "items_truncated": len(items) > MAX_WINDOWS,
    }


def _same(kind, a, b):
    if kind == "money":
        return a is not None and b is not None and abs(float(a) - float(b)) <= CROSSCHECK_MONEY_TOLERANCE
    if kind == "avg":
        return a is not None and b is not None and abs(float(a) - float(b)) <= 0.01  # 판정 값은 소수 둘째 자리 반올림
    if isinstance(a, list) and isinstance(b, list):
        return sorted(a)[:MAX_HASHES] == sorted(b)[:MAX_HASHES]
    return a == b


def _txs(rule, res):
    """구성 거래: L2·L6은 하루 범위에서 Spark가 센 거래 전부, L7은 새 상대마다 처음 보낸 거래."""
    rows = res["firsts"] if rule == "L7" else res["day_txs"]
    key = "first" if rule == "L7" else "ts"
    return [{"tx_hash": r["tx_hash"], "to": r["to"], "amount": _num(r["amount"]), "timestamp": _iso_ms(r[key])}
            for r in rows]


def _recompute(alert, res, b, late, batch_id):
    rule = alert["rule_id"]
    values, detail = _values(rule, res, b)
    ev = alert.get("evidence") or {}
    checks = []
    for field, label, kind in CHECKS[rule]:
        checks.append({"field": field, "label": label, "detector": ev.get(field), "spark": values.get(field),
                       "match": _same(kind, ev.get(field), values.get(field))})
    txs = _txs(rule, res)
    out = {
        "engine": ENGINE,
        "status": "MATCH" if all(c["match"] for c in checks) else "MISMATCH",
        "rule_id": rule,
        "period_profile": PROFILE_NAME,
        "spark_batch_id": batch_id,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "alerted_at": alert["alerted_at"],
        "source": "transfers (Mongo 원장, status=ACCEPTED, from=대상 지갑)",
        "window": {"from": _iso_ms(b["day_from"]), "to": _iso_ms(b["now"])},
        # 계산 범위(demo 5분, prod 24시간) 안 송금 요약. 비교할 판정 값이 없는 참고 값이라 checks에는 넣지 않는다
        "summary": _summary(res["totals"]),
        "values": values,
        "checks": checks,
        "minutes": [{"minute": _iso_ms(m["minute"]), "count": int(m["count"]), "sum": _num(m["sum"]),
                     "max": _num(m["max"]), "receivers": int(m["receivers"])} for m in res["minutes"]],
        # 5분 이동 윈도 (Spark window()). 판정에는 쓰지 않는 참고 근거
        "windows_5m": _windows(res["windows"]),
        "tx_count": len(txs),
        "txs": txs[:CROSSCHECK_MAX_TXS],
        "txs_truncated": len(txs) > CROSSCHECK_MAX_TXS,
        "late_tx_count": late,
    }
    if rule == "L6":
        out["baseline_window"] = {"from": _iso_ms(b["long_from"]), "to": _iso_ms(b["day_from"])}
    if rule == "L7":
        out["history_from"] = _iso_ms(b["hist_from"])
    if detail:
        out["detail"] = detail
    return out


def _error(alert, batch_id, err):
    return {"engine": ENGINE, "status": "ERROR", "rule_id": alert["rule_id"], "spark_batch_id": batch_id,
            "computed_at": datetime.now(timezone.utc).isoformat(), "alerted_at": alert.get("alerted_at"),
            "error": f"{type(err).__name__}: {err}"[:300]}


# ---------- 진입점 ----------

def attach(spark, db, alerts, batch_id=None):
    """대상 경보(L2·L6·L7)에 alert["recompute"]를 붙인다. 예외를 던지지 않는다. 돌려주는 값: 통계 dict."""
    targets = [a for a in alerts if a.get("rule_id") in CROSSCHECK_RULES]
    if not targets:
        return {}
    try:
        keyed = [(k, a, _bounds(a)) for k, a in enumerate(targets)]
        since = min(b["hist_from"] for _, _, b in keyed)
        tx_rows = _fetch(db, {a["violator_address"] for a in targets}, since)
        res = _compute(spark, keyed, tx_rows)
        late_by_wallet = defaultdict(list)
        for w, _, _, _, ts in tx_rows:
            late_by_wallet[w].append(ts)
        for k, a, b in keyed:
            late = sum(1 for ts in late_by_wallet[a["violator_address"]] if ts > b["now"])
            a["recompute"] = _recompute(a, res[k], b, late, batch_id)
    except Exception as e:  # 재계산은 증빙일 뿐이라 실패해도 경보는 그대로 나간다
        logger.exception(f"crosscheck 실패 batch={batch_id}")
        for a in targets:
            a["recompute"] = _error(a, batch_id, e)
    statuses = [a["recompute"]["status"] for a in targets]
    return {"crosscheck": len(targets), "crosscheck_mismatch": statuses.count("MISMATCH"),
            "crosscheck_error": statuses.count("ERROR")}
