"""STR(의심거래보고) 작성 근거 초안: 케이스 증거로 보고서 초안을 만들고, 분석관 의견을 저장한다.

    GET   /api/v1/admin/cases/{id}/str-draft   초안 만들기·보기 (저장하지 않음, 볼 때마다 지금 케이스 값으로 다시 만든다)
    PATCH /api/v1/admin/cases/{id}/str-draft   분석관 최종 의견 저장 → str_status DRAFTED + 감사 원장 한 줄

숫자를 새로 계산하지 않는다. 초안의 모든 숫자는 케이스에 저장된 값을 그대로 쓰고, 어디서 온 값인지 provenance에 적는다.
    실측값   fds_cases.evidence.*   Spark가 판정할 때 계산한 값 (spark/rules.py)
    기준값   fds_cases.criteria.*   판정에 쓴 기준 (spark/config.py)
    계산 출처 fds_cases.source       엔진·기간 설정(prod/demo)·Spark 배치 번호·계산 시각
    재계산   fds_cases.recompute    L2·L6·L7: 원본 거래를 Spark로 다시 계산한 값·일치 여부·1분 내역·구성 거래 (spark/crosscheck.py)
    거래     transfers              케이스 거래 해시로 찾은 원장 원본
그래서 사건 상세(GET /cases/{id})와 STR 초안에는 항상 같은 값이 보인다.

최종 판단·제출은 고객사 보고책임자가 한다 (이 초안은 '보고 검토가 필요해 보임'까지만 쓴다). 제출 상태 관리(SUBMITTED)는 To-Be.
"""
import os
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Annotated

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, FastAPI
from pydantic import BaseModel, Field, StringConstraints
from pymongo.errors import PyMongoError

from bson.decimal128 import Decimal128
from redis.exceptions import RedisError

from app.admin import AdminError, _decision, _transaction, require_admin_key
from app.mongo_client import db
from app.redis_client import redis_client
from app.wallet_status import frozen_reason
from common.keys import BLACKLIST_KEY, FROZEN_KEY, FROZEN_REASONS_KEY

router = APIRouter(prefix="/api/v1/admin", dependencies=[Depends(require_admin_key)])

INSTITUTION = os.getenv("STR_INSTITUTION_NAME", "{고객사 거래소명}")
KST = timezone(timedelta(hours=9))
LAW = "특정금융정보법 제4조 (의심거래보고 검토)"
EXTRA_BASIS = {
    "L5": "범죄수익은닉규제법상 불법재산 관련 가능성 (참고)",
    "L8": "가장·통정매매 유사 패턴 (이용자보호법 제10조 참고)",
}


def _plain(value):
    """Mongo 값을 JSON으로 보낼 수 있게 바꾼다 (ObjectId → 문자열, 시각 → ISO, Decimal128 → 소수)."""
    if isinstance(value, dict):
        return {("id" if k == "_id" else k): _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return value


async def _wallet_flags(wallets):
    """지갑별 동결·사유·블랙리스트 (Redis)."""
    ws = list(wallets)
    if not ws:
        return {}
    try:
        pipe = redis_client.pipeline(transaction=False)
        pipe.smismember(FROZEN_KEY, ws)
        pipe.hmget(FROZEN_REASONS_KEY, ws)
        pipe.smismember(BLACKLIST_KEY, ws)
        frozen, reasons, black = await pipe.execute()
    except RedisError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    return {w: {"frozen": bool(f), "frozen_reason": frozen_reason(r) if f else None, "blacklisted": bool(b)}
            for w, f, r, b in zip(ws, frozen, reasons, black)}


def install(app: FastAPI):
    """main.py에서 부른다."""
    app.include_router(router)


class StrOpinion(BaseModel):
    operator: str = Field(min_length=1, max_length=64)  # 분석관 ID
    final_opinion: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=5000)]


# ---------- 숫자·기간 표기 ----------

def _usdt(x):
    return f"{x:,.2f} USDT" if isinstance(x, (int, float)) else str(x)


def _span(code):
    """criteria의 기간 표기(14d, 24h, 5m)를 한글로."""
    if not code:
        return "기간"
    unit = {"d": "일", "h": "시간", "m": "분", "s": "초"}.get(code[-1], "")
    return f"{code[:-1]}{unit}"


def _n(x):
    """기준값 숫자 (예전 케이스처럼 기준값이 없으면 ?)."""
    return f"{x:,}" if isinstance(x, (int, float)) else "?"


def _pct(x):
    return f"{x * 100:.1f}%" if isinstance(x, (int, float)) else "?"


def _kst(iso):
    if not iso:
        return ""
    return datetime.fromisoformat(iso).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")


# ---------- 룰별: 기준 대비 실측 + 의심 사유 문장 ----------

def _row(item, key, value, threshold=None, threshold_key=None, shown=None):
    return {
        "item": item,
        "measured": value,
        "measured_display": shown if shown is not None else value,
        "threshold": threshold,
        "measured_from": f"fds_cases.evidence.{key}",
        "threshold_from": f"fds_cases.criteria.{threshold_key}" if threshold_key else None,
    }


def _findings(rule, ev, cr):
    """(기준 대비 실측 목록, 의심 사유 문장)."""
    w = _span(cr.get("window"))
    if rule == "L1":
        return [_row("보류 횟수", "hold_count", ev.get("hold_count"), f">= {cr.get('min_holds')}", "min_holds"),
                _row("보류된 룰", "hold_rules", ev.get("hold_rules"))], \
            f"{w} 동안 1단계 보류가 {ev.get('hold_count')}회({', '.join(ev.get('hold_rules') or [])}) 걸린 뒤에도 출금을 반복 시도함"
    if rule == "L2":
        return [_row("누적 송금액", "day_sum", ev.get("day_sum"), f">= {_n(cr.get('min_day_sum'))}", "min_day_sum", _usdt(ev.get("day_sum"))),
                _row("건당 최대 금액", "day_max", ev.get("day_max"), f"< {_n(cr.get('each_below'))}", "each_below", _usdt(ev.get("day_max"))),
                _row("송금 건수", "day_count", ev.get("day_count"))], \
            (f"{w} 동안 {ev.get('day_count')}건으로 나눠 총 {_usdt(ev.get('day_sum'))}를 송금함 "
             f"(건당 최대 {_usdt(ev.get('day_max'))}로, 단건 대량 송금 기준 {_n(cr.get('each_below'))} USDT를 피한 분할 송금으로 보임)")
    if rule == "L3":
        rng = f"{_n(cr.get('amount_min'))}~{_n(cr.get('amount_max'))} USDT"
        return [_row(f"{rng} 송금 횟수", "split_count", ev.get("split_count"), f">= {cr.get('min_count')}", "min_count"),
                _row("합계", "split_sum", ev.get("split_sum"), shown=_usdt(ev.get("split_sum")))], \
            f"{w} 동안 {rng} 구간 송금을 {ev.get('split_count')}회(합계 {_usdt(ev.get('split_sum'))}) 반복함 (금액 경계 아래로 나눈 장기 쪼개기)"
    if rule == "L4":
        return [_row("경유 횟수", "pass_through_count", ev.get("pass_through_count"), f">= {cr.get('min_times')}", "min_times"),
                _row("경유 예시", "examples", ev.get("examples"))], \
            (f"{w} 동안 {_n(cr.get('min_received'))} USDT 이상을 받은 뒤 {_span(cr.get('forward_within'))} 안에 "
             f"{_pct(cr.get('forward_ratio'))} 이상을 다시 보낸 경유 거래가 {ev.get('pass_through_count')}회 있음")
    if rule == "L5":
        got = sum(x.get("amount", 0) for x in ev.get("inflow_from_blacklist") or [])
        sent = sum(x.get("amount", 0) for x in ev.get("outflow_to_blacklist") or [])
        return [_row("등급", "grade", ev.get("grade")),
                _row("블랙리스트에게서 받은 금액", "inflow_from_blacklist", got, shown=_usdt(got)),
                _row("블랙리스트에게 보낸 금액", "outflow_to_blacklist", sent, shown=_usdt(sent)),
                _row("동결 지갑과 주고받은 합계", "frozen_exposure_sum", ev.get("frozen_exposure_sum"),
                     f">= {_n(cr.get('medium_frozen_sum'))} (MEDIUM)", "medium_frozen_sum", _usdt(ev.get("frozen_exposure_sum")))], \
            (f"{w} 안에 위험 지갑과 거래함 (등급 {ev.get('grade')}: 블랙리스트에게서 {_usdt(got)} 받음, "
             f"블랙리스트에게 {_usdt(sent)} 보냄, 동결 지갑과 {_usdt(ev.get('frozen_exposure_sum'))} 주고받음). "
             f"거래 당시에는 상대가 위험 지갑이 아니었을 수 있음")
    if rule == "L6":
        return [_row("오늘 송금 수", "today_count", ev.get("today_count"),
                     f">= {cr.get('min_count')} 그리고 >= {cr.get('ratio')} × 평소", "min_count"),
                _row("평소 하루 평균", "baseline_daily_avg", ev.get("baseline_daily_avg"))], \
            (f"최근 {_span(cr.get('today_window'))} 송금이 {ev.get('today_count')}건으로, "
             f"평소 하루 평균 {ev.get('baseline_daily_avg')}건의 {cr.get('ratio')}배 이상으로 급증함")
    if rule == "L7":
        return [_row("처음 보낸 상대 수", "new_counterparty_count", ev.get("new_counterparty_count"),
                     f">= {cr.get('min_new')}", "min_new"),
                _row("새 상대 (일부)", "new_counterparties", (ev.get("new_counterparties") or [])[:10])], \
            (f"{w} 동안 {_span(cr.get('history_window'))} 기록에 없던 상대 {ev.get('new_counterparty_count')}곳으로 송금함 "
             f"(처음 보는 여러 지갑으로 한꺼번에 분산)")
    if rule == "L8":
        path = ev.get("flow_path") or []
        amounts = ev.get("amounts") or []
        return [_row("귀환 비율", "return_ratio", ev.get("return_ratio"), f">= {_pct(cr.get('min_return_ratio'))}",
                     "min_return_ratio", _pct(ev.get("return_ratio"))),
                _row("경로", "flow_path", path, shown=" → ".join(path)),
                _row("레그별 금액", "amounts", amounts, f"각 >= {_n(cr.get('min_leg_amount'))}", "min_leg_amount",
                     " / ".join(_usdt(a) for a in amounts)),
                _row("경유 단계", "hops", ev.get("hops"), f"<= {cr.get('max_hops')}", "max_hops")], \
            (f"{w} 안에 복수 지갑 경로({' → '.join(path)})를 거쳐 최초 송금액 {_usdt(amounts[0] if amounts else None)}의 "
             f"{_pct(ev.get('return_ratio'))}가 출발 지갑으로 돌아온 순환 거래")
    return [], None


# ---------- Spark 재계산 (fds_cases.recompute) ----------

RC_WORD = {"MATCH": "일치", "MISMATCH": "불일치", "ERROR": "실패"}
MONEY_FIELDS = {"day_sum", "day_max"}


def _attach_recompute(findings, rc):
    """기준 대비 실측 줄마다 Spark 재계산 값과 일치 여부를 붙인다 (같은 evidence 필드끼리)."""
    checks = {x.get("field"): x for x in rc.get("checks") or []}
    for f in findings:
        key = f["measured_from"].rsplit(".", 1)[-1]
        x = checks.get(key)
        if x:
            f["recomputed"] = x.get("spark")
            f["recompute_match"] = x.get("match")
            f["recomputed_from"] = f"fds_cases.recompute.values.{key}"


def _rc_value(field, v):
    if v is None:
        return "-"
    if isinstance(v, list):
        return f"{len(v)}곳"
    if field in MONEY_FIELDS:
        return _usdt(v)
    return f"{v:,}" if isinstance(v, int) else str(v)


def _width(s):
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in s)


def _pad(s, n):
    """한글이 섞여도 고정폭 글꼴에서 줄이 맞게 채운다."""
    s = str(s)
    return s + " " * max(n - _width(s), 0)


def _hms(iso):
    return datetime.fromisoformat(iso).astimezone(KST).strftime("%H:%M:%S") if iso else ""


def _window_label(w):
    """계산 범위 길이를 글로 (5분, 24시간). 동결 해제 지갑은 더 짧을 수 있다."""
    try:
        s = int((datetime.fromisoformat(w["to"]) - datetime.fromisoformat(w["from"])).total_seconds())
    except (KeyError, TypeError, ValueError):
        return "범위 안"
    if s >= 3600 and s % 3600 == 0:
        return f"{s // 3600}시간"
    return f"{s // 60}분" if s % 60 == 0 else f"{s}초"


def _render_recompute(rc):
    """초안 본문 4-1. Spark 재계산 (판정 값과 나란히, 1분 내역, 구성 거래)."""
    if not rc:
        return []
    rows = ["4-1. Spark 재계산 (출처: fds_cases.recompute · spark/crosscheck.py, 원본 transfers 승인 송금)"]
    rows.append(f"   결과        {RC_WORD.get(rc.get('status'), rc.get('status'))} · 배치 {rc.get('spark_batch_id')} · "
                f"{_kst(rc.get('computed_at'))}")
    if rc.get("status") == "ERROR":
        rows.append(f"   오류        {rc.get('error')} (판정 값만 사용)")
        return rows
    w = rc.get("window") or {}
    rows.append(f"   계산 범위   {_kst(w.get('from'))} ~ {_kst(w.get('to'))} (경보 시각까지)")
    sm = rc.get("summary")
    if sm:
        rows.append(f"   범위 요약   {_window_label(w)} 송금 {sm.get('send_count')}건 · {_usdt(sm.get('send_sum'))} · "
                    f"상대 주소 {sm.get('receiver_count')}곳")
    if rc.get("baseline_window"):
        b, d = rc["baseline_window"], rc.get("detail") or {}
        rows.append(f"   평소 범위   {_kst(b.get('from'))} ~ {_kst(b.get('to'))} · 이전 송금 {d.get('prior_count')}건 "
                    f"÷ max({d.get('days')} - 1, 1)일")
    if rc.get("history_from"):
        rows.append(f"   '처음' 기준  {_kst(rc['history_from'])} 이후 기록에 없던 상대")
    rows.append(f"   {_pad('항목', 22)}{_pad('판정 값', 22)}{_pad('Spark 값', 22)}결과")
    for x in rc.get("checks") or []:
        rows.append(f"   {_pad(x.get('label'), 22)}{_pad(_rc_value(x.get('field'), x.get('detector')), 22)}"
                    f"{_pad(_rc_value(x.get('field'), x.get('spark')), 22)}{'일치' if x.get('match') else '불일치'}")
    if rc.get("status") == "MISMATCH" and rc.get("late_tx_count"):
        rows.append(f"   참고        경보 시각 뒤에 들어온 거래 {rc['late_tx_count']}건은 재계산에서 뺐음 (차이 원인일 수 있음)")
    minutes = rc.get("minutes") or []
    rows.append(f"   1분 내역 (KST, 거래가 있던 분만 {len(minutes)}개)")
    rows.append(f"     {_pad('시각', 10)}{_pad('건수', 6)}{_pad('합계', 22)}{_pad('최대', 22)}상대")
    for m in minutes[:30]:
        rows.append(f"     {_pad(_hms(m.get('minute'))[:5], 10)}{_pad(m.get('count'), 6)}{_pad(_usdt(m.get('sum')), 22)}"
                    f"{_pad(_usdt(m.get('max')), 22)}{m.get('receivers')}")
    if len(minutes) > 30:
        rows.append(f"     … 외 {len(minutes) - 30}분")
    txs = rc.get("txs") or []
    label = "새 상대마다 처음 보낸 거래" if rc.get("rule_id") == "L7" else "Spark가 합산한 거래"
    rows.append(f"   구성 거래 {rc.get('tx_count', len(txs))}건 ({label})"
                + (" · 일부만 저장됨" if rc.get("txs_truncated") else ""))
    for t in txs[:20]:
        rows.append(f"     {_hms(t.get('timestamp'))}  {_usdt(t.get('amount'))}  → {t.get('to')}  {t.get('tx_hash')}")
    if len(txs) > 20:
        rows.append(f"     … 외 {len(txs) - 20}건")
    return rows


# ---------- 초안 만들기 ----------

async def _case(case_id):
    try:
        oid = ObjectId(case_id)
    except InvalidId:
        raise AdminError(404, "CASE_NOT_FOUND")
    try:
        case = await db.fds_cases.find_one({"_id": oid})
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    if not case:
        raise AdminError(404, "CASE_NOT_FOUND")
    return case


async def build_draft(case):
    c = _plain(case)
    rule, wallet = c["rule_id"], c["violator_address"]
    ev, cr, src = c.get("evidence") or {}, c.get("criteria") or {}, c.get("source") or {}
    try:
        txs = await (db.transfers.find({"tx_hash": {"$in": case.get("tx_hashes") or []}, "status": "ACCEPTED"},
                                       {"_id": 0, "tx_hash": 1, "from": 1, "to": 1, "amount": 1, "timestamp": 1})
                     .sort("timestamp", 1).limit(200).to_list())
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    txs = _plain(txs)
    flags = (await _wallet_flags([wallet]))[wallet]

    if rule == "MANUAL":
        findings, reason = [], f"직원 직접 동결: {c.get('review_rationale')}"
    else:
        findings, reason = _findings(rule, ev, cr)
    rc = c.get("recompute") or {}
    _attach_recompute(findings, rc)
    status_text = flags["frozen_reason"] or ("BLACKLISTED" if flags["blacklisted"] else "NOT_FROZEN")
    draft_text = (
        f"{reason}. 이에 따라 특정금융정보법 제4조에 따른 의심거래보고 검토가 필요한 것으로 보임. "
        f"대상 지갑({wallet})의 현재 상태는 {status_text}임."
    ) if reason else None

    opinion = c.get("analyst_opinion") or {}
    created = datetime.now(timezone.utc)
    report = {
        "report_id": f"STR-DRAFT-{created.astimezone(KST):%Y%m%d}-{rule}-{c['id'][-6:]}",
        "created_at": created.isoformat(),
        "str_status": c.get("str_status") or "NOT_GENERATED",
        "reporter": {
            "institution_name": INSTITUTION,
            "analyst_name": opinion.get("analyst_id"),
            "compliance_officer_sign": "PENDING_REVIEW",
        },
        "subject": {
            "wallet_address": wallet,
            "client_ip": None,           # 고객사 출금 세션에서 제공 (계정 비의존 설계)
            "customer_id": None,         # 고객사 코어 DB 연동
            "status": status_text,
        },
        "suspicion_details": {
            "case_id": c["id"],
            "rule_id": rule,
            "rule_name": c.get("rule_name"),
            "risk_level": c.get("risk_level"),
            "legal_basis": " · ".join(x for x in (LAW, EXTRA_BASIS.get(rule)) if x),
            "detected_at": c.get("detected_at"),
            "first_tx_at": c.get("first_tx_at"),
            "last_tx_at": c.get("last_tx_at"),
            "findings": findings,          # 기준 대비 실측 (값과 출처)
            "evidence": ev,                # 케이스 증거 원본 그대로
            "criteria": cr,
            "related_tx_hashes": c.get("tx_hashes") or [],
        },
        "transactions": [
            {**t, "time_kst": _kst(t.get("timestamp")),
             "explorer_url": (f"https://etherscan.io/tx/{t['tx_hash']}"
                              if str(t.get("tx_hash", "")).startswith("0x") and len(t["tx_hash"]) == 66 else None)}
            for t in txs
        ],
        "analyst_opinion": {
            "is_customized": bool(opinion.get("final_opinion")),
            "generated_draft": draft_text,
            "final_opinion": opinion.get("final_opinion"),
            "updated_at": opinion.get("updated_at"),
        },
        "spark_recompute": rc or None,     # 케이스의 재계산 근거 묶음 그대로 (없으면 None)
        "provenance": {
            "note": "초안의 숫자는 새로 계산하지 않고 케이스에 저장된 값을 그대로 쓴다 (사건 상세와 같은 값)",
            "case": f"fds_cases/{c['id']}",
            "measured_values": "fds_cases.evidence (Spark 판정 시 계산, spark/rules.py)",
            "thresholds": "fds_cases.criteria (판정 기준, spark/config.py)",
            "recomputed_values": "fds_cases.recompute (원본 거래 Spark 재계산, spark/crosscheck.py)" if rc else None,
            "recompute_status": rc.get("status"),
            "recompute_batch_id": rc.get("spark_batch_id"),
            "recompute_at": rc.get("computed_at"),
            "computed_by": f"{src.get('engine')} + Spark 재계산 {RC_WORD.get(rc.get('status'), rc.get('status'))}"
                           if rc else src.get("engine"),
            "period_profile": src.get("period_profile"),
            "spark_batch_id": src.get("batch_id"),
            "computed_at": src.get("alerted_at"),
            "transactions": "transfers (Mongo 원장, 케이스 tx_hashes로 조회)",
            "review_history": f"policy_decisions (case_id={c['id']})",
        },
    }
    report["text"] = _render(report)
    return report


def _render(r):
    """화면 미리보기·인쇄용 글 (문서 6.2 양식)."""
    s, sub, op = r["suspicion_details"], r["subject"], r["analyst_opinion"]
    line = "=" * 78
    rows = [
        line, "  Suspicious Transaction Report (STR) Reference Draft — 작성 근거 초안", line,
        f"Case ID: {r['report_id']} | Detected: {_kst(s['detected_at'])} | Rule: {s['rule_id']} {s['rule_name']} ({s['risk_level']})",
        "-" * 78,
        f"1. 보고 기관   {r['reporter']['institution_name']} · 준법감시실 자금세탁방지팀 · 보고책임자: 준법감시인",
        f"2. 보고 대상   지갑 {sub['wallet_address']} · 현재 상태 {sub['status']} · 고객 식별자: (고객사 연동)",
        "3. 거래 내역",
    ]
    for t in r["transactions"][:20]:
        rows.append(f"   {t['time_kst']}  {_usdt(t.get('amount'))}  {t['from']} → {t['to']}  {t['tx_hash']}")
    if len(r["transactions"]) > 20:
        rows.append(f"   … 외 {len(r['transactions']) - 20}건")
    rows.append("4. 기준 대비 실측 (출처: 케이스 증거)")
    for f in s["findings"]:
        th = f" (기준 {f['threshold']})" if f.get("threshold") else ""
        rc_note = f" · Spark 재계산 {'일치' if f['recompute_match'] else '불일치'}" if "recompute_match" in f else ""
        rows.append(f"   - {f['item']}: {f['measured_display']}{th}{rc_note}")
    rows += _render_recompute(r.get("spark_recompute"))
    rows += [
        "5. 의심 사유 (자동 생성 → 분석관 편집 가능)",
        f"   {op['final_opinion'] or op['generated_draft'] or ''}",
        f"6. 근거        {s['legal_basis']}",
        f"   계산 출처   {r['provenance']['computed_by']} · 설정 {r['provenance']['period_profile']} · "
        f"배치 {r['provenance']['spark_batch_id']} · {_kst(r['provenance']['computed_at'])}",
        "7. 검토        본 문서는 Aegis-FDS 탐지 기록에 기반한 작성 근거 초안이며, 보고책임자의 검토·승인을 거쳐 KoFIU에 제출됨.",
        f"               작성자: {r['reporter']['analyst_name'] or '(미지정)'} · 승인: {r['reporter']['compliance_officer_sign']}",
        line,
    ]
    return "\n".join(rows)


# ---------- API ----------

@router.get("/cases/{case_id}/str-draft")
async def get_draft(case_id: str):
    return {"status": "OK", "str_report": await build_draft(await _case(case_id))}


@router.patch("/cases/{case_id}/str-draft")
async def save_opinion(case_id: str, body: StrOpinion):
    """분석관이 고친 의심 사유(최종 의견)를 저장한다. 케이스의 증거·숫자는 바꾸지 않는다."""
    case = await _case(case_id)
    now = datetime.now(timezone.utc)
    opinion = {"final_opinion": body.final_opinion, "analyst_id": body.operator, "updated_at": now}

    class _Who:  # 감사 원장 형식(_decision)에 맞춘 직원·사유
        operator, rationale = body.operator, "STR 초안 분석관 의견 저장"

    async def callback(session):
        await db.fds_cases.update_one({"_id": case["_id"]},
                                      {"$set": {"analyst_opinion": opinion, "str_status": "DRAFTED", "updated_at": now}},
                                      session=session)
        await db.policy_decisions.insert_one(
            _decision("STR_DRAFT_SAVE", case["violator_address"], _Who, now, "STR_DRAFTED", case["_id"]),
            session=session)
    await _transaction(callback)
    case.update({"analyst_opinion": opinion, "str_status": "DRAFTED"})
    return {"status": "OK", "str_report": await build_draft(case)}
