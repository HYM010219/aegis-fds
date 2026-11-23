"""L1~L8 판단: queries.py의 조회 결과를 받아 경보 목록을 돌려준다.

Mongo·Redis를 직접 부르지 않아서, 가짜 거래 목록만 넣어도 결과를 확인할 수 있다.
(L8만 2·3번째 거래를 더 찾아야 해서 조회 함수를 인자로 받는다)

    L1 반복 보류        2주 안에 1단계 보류 3회 이상                       자동 동결
    L2 쪼개서 대량 송금 하루 누적 5만 이상, 모든 거래는 5만 미만            자동 동결
    L3 장기 쪼개기      2주 안에 900~990 송금 10회 이상                    자동 동결
    L4 경유 지갑        받은 금액 80% 이상을 1시간 안에 송금, 2주 3회 이상  직원 검토
    L5 위험 지갑 접촉   블랙리스트·동결 지갑과 거래 (CRITICAL/HIGH/MEDIUM)   직원 검토
    L6 활동 급증        오늘 거래 수가 평소 하루 평균의 10배 이상, 20건 이상 직원 검토
    L7 새 상대 집중     하루 안에 처음 보내는 상대 20개 이상                직원 검토
    L8 순환 거래        A→B→A 또는 A→B→C→A, 80% 이상 돌아옴              자동 동결
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import timezone

from config import (
    BIG_AMOUNT,
    L1_MIN_HOLDS,
    L2_MIN_DAY_SUM,
    L3_MIN_SPLITS,
    L4_FORWARD_RATIO,
    L4_MIN_TIMES,
    L5_MEDIUM_SUM,
    L6_MIN_COUNT,
    L6_RATIO,
    L7_MIN_NEW,
    L8_MAX_FANOUT,
    L8_RETURN_RATIO,
    MAX_HASHES,
    PERIOD,
    PROFILE_NAME,
    RULE_NAMES,
    SPLIT_MAX,
    SPLIT_MIN,
)


# ---------- 경보 형식 ----------

def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def _span(td):
    """기간을 사람이 읽는 글자로: 14d, 24h, 5m."""
    s = int(td.total_seconds())
    if s % 86400 == 0 and s >= 2 * 86400:   # 2일 이상만 일 단위 (24시간은 24h)
        return f"{s // 86400}d"
    for unit, size in (("h", 3600), ("m", 60)):
        if s % size == 0:
            return f"{s // size}{unit}"
    return f"{s}s"


def criteria(rule):
    """이 경보를 판정할 때 쓴 기준값과 기간 (config.py 값 그대로). 사건 상세·STR 초안이 '기준 대비 실측'을 이 값으로 보여준다."""
    long, day, hour = _span(PERIOD["long"]), _span(PERIOD["day"]), _span(PERIOD["hour"])
    return {
        "L1": {"window": long, "min_holds": L1_MIN_HOLDS},
        "L2": {"window": day, "min_day_sum": L2_MIN_DAY_SUM, "each_below": L2_MIN_DAY_SUM},
        "L3": {"window": long, "amount_min": SPLIT_MIN, "amount_max": SPLIT_MAX, "min_count": L3_MIN_SPLITS},
        "L4": {"window": long, "forward_within": hour, "min_received": BIG_AMOUNT,
               "forward_ratio": L4_FORWARD_RATIO, "min_times": L4_MIN_TIMES},
        "L5": {"window": long, "critical": "블랙리스트에게서 받음", "high": "블랙리스트에게 보냄",
               "medium_frozen_sum": L5_MEDIUM_SUM},
        "L6": {"today_window": day, "baseline_window": long, "ratio": L6_RATIO, "min_count": L6_MIN_COUNT},
        "L7": {"window": day, "history_window": long, "min_new": L7_MIN_NEW},
        "L8": {"window": day, "min_leg_amount": BIG_AMOUNT, "min_return_ratio": L8_RETURN_RATIO, "max_hops": 3},
    }[rule]


def make_alert(rule, wallet, hashes, first, last, evidence, now, risk=None, counterparty=None):
    """경보 소비자가 읽는 형식 (fds/app/alert_consumer.py).
    evidence = 실측값, criteria = 판정 기준값, alerted_at·period_profile(·batch_id는 detector가 붙임) = 계산 출처."""
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
        "criteria": criteria(rule),
        "period_profile": PROFILE_NAME,
        "alerted_at": _iso(now),
    }


# ---------- L1 반복 보류 ----------

def l1(hold_rows, now):
    """hold_rows: queries.hold_summary() 결과."""
    return [
        make_alert("L1", row["_id"], row["hashes"], row["first"], row["last"],
                   {"hold_count": row["holds"], "hold_rules": sorted(row["rules"])}, now)
        for row in hold_rows
        if row["holds"] >= L1_MIN_HOLDS
    ]


# ---------- L2·L3·L6 (보낸 거래 요약 하나를 같이 씀) ----------

def l2_l3_l6(sent_rows, now, history=None):
    """sent_rows: queries.sent_summary() 결과. 지갑마다 L2, L3, L6 순서로 본다.
    history: 조회한 기간 (동결 해제 지갑은 해제 이후라 2주보다 짧다. 없으면 PERIOD["long"])."""
    alerts = []
    for row in sent_rows:
        for alert in (l2(row, now), l3(row, now), l6(row, now, history)):
            if alert:
                alerts.append(alert)
    return alerts


def l2(row, now):
    """L2 쪼개서 대량 송금 (R05 회피): 하루 누적은 5만 이상인데 한 건도 5만을 넘지 않음."""
    if row["day_sum"] >= L2_MIN_DAY_SUM and row["day_max"] < L2_MIN_DAY_SUM:
        return make_alert("L2", row["_id"], row["day_hashes"], row["day_first"], row["day_last"], {
            "day_sum": row["day_sum"], "day_max": row["day_max"], "day_count": row["day_count"],
        }, now)
    return None


def l3(row, now):
    """L3 장기 쪼개기 (R02 회피): 2주 동안 900~990 송금이 많음."""
    if row["split_count"] >= L3_MIN_SPLITS:
        return make_alert("L3", row["_id"], row["split_hashes"], row["split_first"], row["split_last"], {
            "split_count": row["split_count"], "split_sum": row["split_sum"],
        }, now)
    return None


def l6(row, now, history=None):
    """L6 급격한 활동 증가: 오늘을 뺀 나머지 기간의 하루 평균과 비교."""
    days = (history or PERIOD["long"]) / PERIOD["day"]
    if days < 2:
        # 동결 해제 후 이틀이 안 됨: 비교할 '평소' 기록이 없어서 보지 않는다
        return None
    today = row["day_count"]
    if row["long_count"] - today <= 0:
        # 오늘 말고는 기록이 없는 새 지갑: '평소'가 없어 비교하지 않는다 (새 상대 집중은 L7이 본다)
        return None
    baseline = (row["long_count"] - today) / max(days - 1, 1)
    if today >= L6_MIN_COUNT and today >= L6_RATIO * baseline:
        return make_alert("L6", row["_id"], row["day_hashes"], row["day_first"], row["day_last"], {
            "today_count": today, "baseline_daily_avg": round(baseline, 2),
        }, now)
    return None


# ---------- L4 경유 지갑 ----------

def l4(wallets, big, now):
    """받은 금액의 80% 이상을 1시간 안에 다시 송금, 2주 안에 3회 이상 (1,000 이상 받은 거래만).
    big: queries.big_transfers() 결과."""
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
        hits = _pass_throughs(received.get(w, []), sent.get(w, []))
        if len(hits) >= L4_MIN_TIMES:
            hashes = [r["tx_hash"] for r, _, _ in hits] + [t["tx_hash"] for _, f, _ in hits for t in f]
            alerts.append(make_alert("L4", w, hashes, hits[0][0]["ts"], max(f[-1]["ts"] for _, f, _ in hits), {
                "pass_through_count": len(hits),
                "examples": [
                    {"received": r["amount"], "forwarded": round(total, 6),
                     "minutes": round((f[-1]["ts"] - r["ts"]).total_seconds() / 60, 1)}
                    for r, f, total in hits[:5]
                ],
            }, now))
    return alerts


def _pass_throughs(received, sent):
    """받은 거래마다 1시간 안에 보낸 거래를 모아, 80% 이상이면 (받은 거래, 보낸 거래들, 보낸 합계)."""
    ins = sorted(received, key=lambda t: t["ts"])
    outs = sorted(sent, key=lambda t: t["ts"])
    if len(ins) < L4_MIN_TIMES or not outs:
        return []
    out_times = [t["ts"] for t in outs]
    hits = []
    for r in ins:
        lo = bisect_right(out_times, r["ts"])
        hi = bisect_right(out_times, r["ts"] + PERIOD["hour"])
        forwarded = outs[lo:hi]
        total = sum(t["amount"] for t in forwarded)
        if forwarded and total >= L4_FORWARD_RATIO * r["amount"]:
            hits.append((r, forwarded, total))
    return hits


# ---------- L5 위험 지갑 접촉 (금액·방향 등급) ----------

def l5(inflow, outflow, black, now):
    """통과된 거래만 센다 (R07로 막힌 시도는 제외). 등급은 _l5_grade().
    inflow, outflow: queries.risky_flows() 결과 / black: 블랙리스트 (나머지 위험 지갑은 동결 지갑).
    동결 지갑은 기본적으로 직원이 확인한 동결만 들어온다 (detector._analyze_l5, config.L5_EXPOSURE)."""
    per_wallet = defaultdict(lambda: {"in_black": [], "out_black": [], "frozen_sum": 0.0,
                                      "hashes": [], "first": None, "last": None})
    for direction, rows in (("in", inflow), ("out", outflow)):
        for row in rows:
            info = per_wallet[row["_id"]["w"]]
            cp, amount = row["_id"]["cp"], row["amount"]
            if cp in black:
                info[f"{direction}_black"].append({"wallet": cp, "count": row["count"], "amount": amount})
            else:
                info["frozen_sum"] += amount
            info["hashes"] += row["hashes"]
            info["first"] = min(filter(None, [info["first"], row["first"]]))
            info["last"] = max(filter(None, [info["last"], row["last"]]))

    alerts = []
    for w, info in per_wallet.items():
        risk = _l5_grade(info)
        if not risk:
            continue
        alerts.append(make_alert("L5", w, info["hashes"], info["first"], info["last"], {
            "grade": risk,
            "inflow_from_blacklist": info["in_black"],
            "outflow_to_blacklist": info["out_black"],
            "frozen_exposure_sum": round(info["frozen_sum"], 6),
        }, now, risk=risk))
    return alerts


def _l5_grade(info):
    """CRITICAL: 블랙리스트에게서 직접 받음 / HIGH: 블랙리스트에게 보냄 /
    MEDIUM: 동결 지갑과 2주 누적 1만 이상 (주고받은 합계) / 해당 없으면 None."""
    if info["in_black"]:
        return "CRITICAL"
    if info["out_black"]:
        return "HIGH"
    if info["frozen_sum"] >= L5_MEDIUM_SUM:
        return "MEDIUM"
    return None


# ---------- L7 새 상대 집중 ----------

def l7(new_rows, now):
    """new_rows: queries.new_counterparties() 결과."""
    return [
        make_alert("L7", row["_id"], [], row["first"], row["last"], {
            "new_counterparty_count": row["new_count"],
            "new_counterparties": sorted(row["counterparties"])[:MAX_HASHES],
        }, now)
        for row in new_rows
        if row["new_count"] >= L7_MIN_NEW
    ]


# ---------- L8 순환 거래 ----------

def l8(wallets, big, white, now, transfers_from):
    """A→B→A 또는 A→B→C→A를 하루 안에 시간 순서대로 돌고, 각 거래 1,000 이상,
    처음 보낸 금액의 80% 이상이 돌아옴. 화이트리스트 지갑을 거치면 제외.
    big: queries.big_transfers() 결과 / transfers_from: queries.transfers_from(senders, since, to=None)."""
    t_day = now - PERIOD["day"]
    targets = set(wallets)

    # 1번째 거래: A가 하루 안에 보낸 1,000 이상 거래
    first_legs = defaultdict(list)
    for t in big:
        if (t["from"] in targets and t["ts"] >= t_day and t["amount"] >= BIG_AMOUNT
                and t["to"] != t["from"] and t["to"] not in white):
            first_legs[t["from"]].append(t)
    if not first_legs:
        return []

    # 2번째 거래: B가 보낸 거래 (상대가 너무 많은 B는 넓혀 가지 않음)
    bs = sorted({t["to"] for legs in first_legs.values() for t in legs})
    second = transfers_from(bs, t_day)
    out_degree = defaultdict(set)
    for t in second:
        out_degree[t["from"]].add(t["to"])
    hubs = {b for b, cps in out_degree.items() if len(cps) > L8_MAX_FANOUT}
    by_from = defaultdict(list)
    for t in second:
        if t["from"] not in hubs:
            by_from[t["from"]].append(t)

    # 3번째 거래: C가 분석 지갑(A 후보)에게 돌려준 거래
    cs = sorted({t["to"] for t in second if t["from"] not in hubs and t["to"] not in white})
    third = transfers_from(cs, t_day, to=sorted(targets)) if cs else []
    back = defaultdict(list)
    for t in third:
        back[(t["from"], t["to"])].append(t)

    alerts = []
    for a, legs in first_legs.items():
        found = _find_cycle(a, legs, by_from, back, white)
        if found:
            path = [found[0]["from"]] + [t["to"] for t in found]
            alerts.append(make_alert("L8", a, [t["tx_hash"] for t in found], found[0]["ts"], found[-1]["ts"], {
                "path": path,
                "amounts": [t["amount"] for t in found],
                "return_ratio": round(found[-1]["amount"] / found[0]["amount"], 4),
                "hops": len(found),
            }, now, counterparty=path[1]))
    return alerts


def _find_cycle(a, legs, by_from, back, white):
    """A의 1번째 거래들 중 처음 찾은 순환 [A→B, B→A] 또는 [A→B, B→C, C→A]. 없으면 None."""
    for l1_ in sorted(legs, key=lambda t: t["ts"]):
        need = L8_RETURN_RATIO * l1_["amount"]
        for l2_ in by_from.get(l1_["to"], []):
            if l2_["ts"] <= l1_["ts"]:
                continue
            c = l2_["to"]
            # A→B→A
            if c == a and l2_["amount"] >= need:
                return [l1_, l2_]
            # A→B→C→A (C가 A·B 자신이거나 화이트리스트면 건너뜀)
            if c in (a, l1_["to"]) or c in white:
                continue
            l3_ = _return_leg(back.get((c, a), []), after=l2_["ts"], need=need)
            if l3_:
                return [l1_, l2_, l3_]
    return None


def _return_leg(candidates, after, need):
    """C→A 거래 중 2번째 거래 뒤에 있고 금액이 충분한 첫 거래."""
    for t in candidates:
        if t["ts"] > after and t["amount"] >= need:
            return t
    return None
