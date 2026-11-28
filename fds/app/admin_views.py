"""관제 화면 조회 API (Step 7)

FE web/src/admin/data/api.js 가 부르는 경로와 응답 형식에 맞춘다.
조치 API(동결·해제·명단)는 admin.py, 여기는 읽기 전용 조회만 둔다.

    GET /api/v1/admin/transfers?limit=300    실시간 거래 (판정 결과 포함)
    GET /api/v1/admin/health                 시스템 상태 + 데이터 신뢰성 신호
    GET /api/v1/admin/cases?limit=200        경보(사건) 목록
    GET /api/v1/admin/cases/{case_id}        사건 상세 (근거·관련 거래·자금 요약)
    GET /api/v1/admin/stats?range=24h        대시보드 집계 (KPI·시계열·룰별·위험 지갑 상위)
    POST /api/v1/admin/cases/{id}/decision   FE 조치 버튼 어댑터 → admin.py 조치 API를 그대로 호출

원칙
- 송금 API와 같은 프로세스에서 돌기 때문에 조회는 짧게 끊는다 (Mongo maxTimeMS, 신호당 상한).
- 읽기만 한다. 예외: decision 어댑터는 FE 요청 형식을 admin.py 조치 함수로 바꿔 넘기기만 한다
  (동결·종결·해제 로직과 검증은 admin.py 한 곳에만 있다).
- 응답 형식은 pydantic 모델로 선언한다 → Swagger(/docs)에 필드·허용값이 그대로 나오는 계약서가 된다.
"""
import asyncio
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Literal

from bson import Decimal128, ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, FastAPI, Query
from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.admin import AdminError, require_admin_key
from app.mongo_client import db, mongo_client
from app.redis_client import redis_client
from common import entities
from common.keys import (ALERT_TOPIC, BLACKLIST_KEY, FREEZE_TOPIC, FROZEN_KEY, HOLD_TOPIC,
                         REJECTED_TOPIC, TRANSFER_TOPIC, WHALE_LIMIT_USDT, whitelist_key)

router = APIRouter(prefix="/api/v1/admin", dependencies=[Depends(require_admin_key)])

QUERY_MS = 2000                       # Mongo 조회 한 번의 상한. 넘으면 503 (송금 처리를 붙잡지 않게)


def install(app: FastAPI):
    """main.py에서 admin.install(app) 다음에 부른다 (AdminError 처리기는 admin.install이 등록)."""
    app.include_router(router)


# =====================================================================
# 응답 형식 (Swagger 계약)
# =====================================================================

Origin = Literal["MAINNET", "EXCHANGE", "SDN", "SIM", "APP"]


class TransferRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    tx_hash: str | None
    log_index: int | None
    from_: str | None = Field(default=None, alias="from")   # JSON 키는 "from"
    to: str | None
    amount: float | None
    ts: int | None                                    # epoch ms (게이트웨이 기록 시각)
    block_number: int | None
    status: Literal["ACCEPTED", "REJECTED"] | None
    http: int | None
    reason: str | None
    result: Literal["PASS", "PASS_HOLD_OTHER", "HOLD", "BLOCK", "FROZEN"]
    rule: str | None
    sender_hold: bool                                 # R01 5번째처럼 통과했지만 보내는 지갑이 보류됨
    fromOrigin: Origin
    toOrigin: Origin
    source: str
    fromLabel: str | None = None                      # 알려진 주소 이름 (common/entities.json). 없으면 None
    toLabel: str | None = None


class Service(BaseModel):
    id: str
    name: str
    role: str
    status: Literal["UP", "WARN", "DOWN", "MOCK"]
    metric: str
    latency: int | None
    last: int | None                                  # epoch ms


class Topic(BaseModel):
    topic: str
    desc: str
    lag: int                                          # outbox 발행 대기 건수
    rate: float                                       # 분당 발행 건수 (최근 5분 평균, 소수 1자리)


class Health(BaseModel):
    mock: bool
    checkedAt: int
    services: list[Service]
    topics: list[Topic]
    signals: dict[str, float | int | None]            # SLI 실측용 숫자 (침지 테스트 때 5분마다 기록)


class Evidence(BaseModel):
    label: str
    value: float
    threshold: float
    unit: str
    op: Literal[">=", ">", "<", "<="]


class Decided(BaseModel):
    status: str
    actor: str | None
    ts: int | None
    memo: str | None


CaseStatus = Literal["PENDING", "AUTO_FROZEN", "FROZEN", "DISMISSED", "RELEASED"]


class CaseItem(BaseModel):
    case_id: str                                      # Mongo _id 문자열. 동결·이상 없음 API에 그대로 쓴다
    rule_id: str
    rule_name: str | None
    risk_level: Literal["CRITICAL", "HIGH", "MEDIUM"] | None
    violator_address: str
    origin: Origin
    isNew: bool
    detected_at: int | None
    status: CaseStatus
    period_profile: str
    tx_hashes: list[str]
    evidence: list[Evidence]
    evidence_raw: dict                                # Spark 원본 근거 (STR 초안·감사용)
    headline: str
    counterpart: str | None
    amount: float
    decided: Decided | None
    label: str | None = None                          # 대상 지갑이 알려진 주소면 그 이름 (common/entities.json)


class Flow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    ts: int | None
    in_: float = Field(default=0, alias="in")
    out: float = 0


class Summary(BaseModel):
    received: float
    sent: float
    counterparties: int
    holds: list[str]
    firstSeen: int | None


class Counterparty(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    address: str | None
    in_: float = Field(alias="in")
    out: float
    n: int
    origin: Origin
    label: str | None = None


class AuditRow(BaseModel):
    seq: int
    ts: int
    actor: str
    action: str
    case_id: str | None = None
    wallet: str | None = None
    note: str | None = None
    prev: str
    hash: str


class CaseDetail(CaseItem):
    related: list[TransferRow]
    flows: list[Flow]
    summary: Summary
    counterparties: list[Counterparty]
    otherCases: list[CaseItem]
    audit: list[AuditRow]                             # policy_decisions 해시 체인
    sdn: list[str]
    recompute: dict | None = None                     # Spark 재계산 근거 묶음 (L2·L6·L7, spark/crosscheck.py). 목록 API에는 넣지 않음


class GraphNode(BaseModel):
    address: str
    level: int
    origin: Origin
    label: str | None = None
    frozen: bool
    sdn: bool
    center: bool
    onPath: bool


class GraphEdge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    amount: float
    n: int
    txs: list[str]
    hit: bool
    ts: int | None


class GraphWindow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: int = Field(alias="from")
    to: int


class GraphResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    case: CaseDetail
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    window: GraphWindow
    hitTimes: list[int]


class SearchItem(BaseModel):
    kind: Literal["사건", "지갑", "거래"]
    id: str
    text: str
    to: str


# =====================================================================
# 공통 변환
# =====================================================================

TRANSFER_FIELDS = {
    "_id": 0, "tx_hash": 1, "log_index": 1, "from": 1, "to": 1, "amount": 1, "block": 1,
    "http_code": 1, "status": 1, "reason": 1, "created_at": 1,
}
# FE ORIGINS 의 source 값 (meta.js).
ORIGIN_SOURCE = {"MAINNET": "온체인", "EXCHANGE": "온체인", "SDN": "온체인", "SIM": "모의", "APP": "앱"}
RULE_IN_REASON = re.compile(r"\b(?:TEMP_HOLD_|FROZEN_)?(R0[1-7]|L[1-8])\b")


def _number(v):
    if isinstance(v, Decimal128):
        return float(v.to_decimal())
    try:
        return float(v)
    except (TypeError, ValueError):
        return None                   # 금액 형식 오류로 거절된 기록


def _epoch_ms(dt):
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _short(a):
    return f"{a[:6]}…{a[-4:]}" if a and len(a) > 12 else (a or "?")


def _rule_from_reason(reason):
    if not reason:
        return None
    if reason.startswith("BLACKLISTED"):
        return "R07"
    m = RULE_IN_REASON.search(reason.replace("-", "_"))
    return m.group(1) if m else None   # FROZEN_MANUAL 등은 룰 없음


def _result(doc, holds):
    """FE RESULTS 키: PASS · PASS_HOLD_OTHER · HOLD · BLOCK · FROZEN"""
    if doc.get("status") == "ACCEPTED":
        # R03·R06: 거래는 통과, 받는 지갑 보류. R01 5번째(보내는 지갑 보류)는 거래 자체는 통과라 PASS로 둔다.
        return "PASS_HOLD_OTHER" if any(h.get("side") == "RECEIVER" for h in holds) else "PASS"
    if doc.get("http_code") == 429:
        return "HOLD"
    if (doc.get("reason") or "").startswith("FROZEN_"):
        return "FROZEN"
    return "BLOCK"


async def _origins(addresses):
    """시뮬레이터(BOT-, wal_) → SIM, 거래소(화이트리스트 L) → EXCHANGE, 블랙리스트 → SDN, 나머지 → MAINNET."""
    addresses = list(addresses)
    out = {}
    for a in addresses:
        if a and (a.startswith("BOT-") or a.startswith("bot-") or a.startswith("wal_")):
            out[a] = "SIM"
        else:
            out[a] = "MAINNET"
    if not addresses:
        return out
    try:
        exchange = await redis_client.smismember(whitelist_key("L"), addresses)
        blocked = await redis_client.smismember(BLACKLIST_KEY, addresses)
    except RedisError:
        return out                    # 출처 표시는 부가 정보 → 실패해도 목록은 보여 준다
    for a, ex, bl in zip(addresses, exchange, blocked):
        if bl:
            out[a] = "SDN"
        elif ex:
            out[a] = "EXCHANGE"
    return out


async def _transfer_rows(docs):
    """원장 문서 → TransferRow. 보류 기록과 지갑 출처를 붙인다."""
    accepted = [d["tx_hash"] for d in docs if d.get("status") == "ACCEPTED" and d.get("tx_hash")]
    hold_docs = []
    if accepted:
        hold_docs = await (db.fds_holds.find({"tx_hash": {"$in": accepted}},
                                             {"_id": 0, "tx_hash": 1, "log_index": 1, "rule": 1, "side": 1})
                           .max_time_ms(QUERY_MS).to_list())
    holds_by_tx = {}
    for h in hold_docs:
        holds_by_tx.setdefault((h.get("tx_hash"), h.get("log_index")), []).append(h)

    origins = await _origins(sorted({a for d in docs for a in (d.get("from"), d.get("to")) if a}))

    rows = []
    for d in docs:
        holds = holds_by_tx.get((d.get("tx_hash"), d.get("log_index")), [])
        from_origin = origins.get(d.get("from"), "MAINNET")
        rows.append(TransferRow(
            tx_hash=d.get("tx_hash"),
            log_index=d.get("log_index"),
            from_=d.get("from"),
            to=d.get("to"),
            amount=_number(d.get("amount")),
            ts=_epoch_ms(d.get("created_at")),
            block_number=int(d["block"]) if d.get("block") is not None else None,
            status=d.get("status"),
            http=d.get("http_code"),
            reason=d.get("reason"),
            result=_result(d, holds),
            rule=holds[0].get("rule") if holds else _rule_from_reason(d.get("reason")),
            sender_hold=any(h.get("side") == "SENDER" for h in holds),
            fromOrigin=from_origin,
            toOrigin=origins.get(d.get("to"), "MAINNET"),
            source=ORIGIN_SOURCE.get(from_origin, "온체인"),
            fromLabel=entities.label(d.get("from")),
            toLabel=entities.label(d.get("to")),
        ))
    return rows


# =====================================================================
# GET /transfers — 실시간 거래
# =====================================================================

@router.get("/transfers", response_model=list[TransferRow])
async def list_transfers(limit: int = Query(default=300, ge=1, le=1000)):
    """최근 통과 거래 + 최근 보류·차단·동결 거절 거래를 섞어 최신순으로 돌려준다.

    (FE 문구 "표본: 최근 거래 일부 + 모든 보류·차단·거절 거래"와 같은 방식)
    401·409·422·503 같은 입력·인증 오류는 판정이 아니므로 넣지 않는다.
    """
    try:
        recent = await (db.transfers.find({"status": "ACCEPTED"}, TRANSFER_FIELDS)
                        .sort("created_at", -1).limit(limit).max_time_ms(QUERY_MS).to_list())
        flagged = await (db.transfers.find({"status": "REJECTED", "http_code": {"$in": [403, 429]}}, TRANSFER_FIELDS)
                         .sort("created_at", -1).limit(limit).max_time_ms(QUERY_MS).to_list())
        docs = sorted(recent + flagged, key=lambda d: _epoch_ms(d.get("created_at")) or 0, reverse=True)[:limit]
        return await _transfer_rows(docs)
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")


# =====================================================================
# GET /health — 시스템 상태 · 데이터 신뢰성
# =====================================================================
#
# "컨테이너는 모두 살아 있는데 데이터가 틀린" 상황을 한 화면에서 잡기 위한 신호.
#   인프라: Redis·Mongo·ClickHouse 응답
#   데이터: 원장 신선도, outbox 발행 대기, Mongo ↔ ClickHouse 적재 완결성(SQL 대조), Spark 하트비트

PROBE_S = 2.0
FRESH_WARN_S = int(os.getenv("HEALTH_FRESH_WARN_S", "60"))
FRESH_DOWN_S = int(os.getenv("HEALTH_FRESH_DOWN_S", "600"))
OUTBOX_WARN_S = int(os.getenv("HEALTH_OUTBOX_WARN_S", "30"))
OUTBOX_DOWN_S = int(os.getenv("HEALTH_OUTBOX_DOWN_S", "300"))
RECON_WINDOW = timedelta(minutes=int(os.getenv("HEALTH_RECON_WINDOW_MIN", "60")))
RECON_DELAY = timedelta(minutes=int(os.getenv("HEALTH_RECON_DELAY_MIN", "5")))  # 적재 지연을 빼고 비교
RECON_WARN, RECON_DOWN = 0.99, 0.95
SPARK_HEARTBEAT_KEY = "fds:heartbeat:spark"     # Spark가 배치마다 epoch ms 를 남기면 자동으로 읽힘 (DE 예정)
SPARK_WARN_S, SPARK_DOWN_S = 60, 300

CH_URL = os.getenv("CLICKHOUSE_URL", "").rstrip("/")             # 예: http://clickhouse:8123
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "aegis")

TOPIC_DESC = {
    TRANSFER_TOPIC: "통과 거래",
    REJECTED_TOPIC: "거절 거래",
    HOLD_TOPIC: "1단계 보류·차단 → Spark",
    ALERT_TOPIC: "2단계 탐지 사건",
    FREEZE_TOPIC: "동결·해제 → Spark",
}


def _now_ms():
    return int(time.time() * 1000)


def _grade(value, warn, down):
    """값이 클수록 나쁜 신호 (경과 초 등)."""
    if value is None:
        return "WARN"
    return "UP" if value < warn else ("WARN" if value < down else "DOWN")


async def _timed(coro):
    """(결과, 응답 ms, 오류) — 상한을 넘거나 실패해도 예외를 밖으로 내보내지 않는다."""
    t0 = time.perf_counter()
    try:
        out = await asyncio.wait_for(coro, PROBE_S)
        return out, round((time.perf_counter() - t0) * 1000), None
    except Exception as e:  # 상태 화면은 어떤 실패도 "DOWN 신호"로 바꿔 보여 준다
        return None, None, type(e).__name__


def _ch_query(sql):
    req = urllib.request.Request(
        f"{CH_URL}/?{urllib.parse.urlencode({'database': CH_DB})}",
        data=sql.encode(),
        headers={"X-ClickHouse-User": CH_USER, "X-ClickHouse-Key": CH_PASSWORD},
    )
    with urllib.request.urlopen(req, timeout=PROBE_S) as r:
        return r.read().decode().strip()


async def _redis_probe():
    await redis_client.ping()
    return await redis_client.dbsize()


async def _mongo_probe():
    await mongo_client.admin.command("ping")
    latest = None
    for status in ("ACCEPTED", "REJECTED"):     # (status, created_at) 인덱스를 타도록 상태별로 조회
        d = await (db.transfers.find({"status": status}, {"_id": 0, "created_at": 1})
                   .sort("created_at", -1).limit(1).max_time_ms(QUERY_MS).to_list())
        ms = _epoch_ms(d[0]["created_at"]) if d else None
        if ms and (latest is None or ms > latest):
            latest = ms
    return latest


async def _outbox_probe():
    since = datetime.now(timezone.utc) - timedelta(minutes=5)
    # pymongo 비동기: aggregate()는 await 해서 커서를 받은 뒤 to_list()
    pending = await (await db.outbox_events.aggregate([
        {"$match": {"publish_state": "PENDING"}},
        {"$group": {"_id": "$topic", "n": {"$sum": 1}, "oldest": {"$min": "$created_at"}}},
    ], maxTimeMS=QUERY_MS)).to_list()
    published = await (await db.outbox_events.aggregate([
        {"$match": {"publish_state": "PUBLISHED", "published_at": {"$gte": since}}},
        {"$group": {"_id": "$topic", "n": {"$sum": 1}}},
    ], maxTimeMS=QUERY_MS)).to_list()
    return pending, published


async def _recon_probe():
    """최근 1시간(5분 전까지) 통과 거래: Mongo 원장 건수 vs ClickHouse 고유 건수."""
    end = datetime.now(timezone.utc) - RECON_DELAY
    start = end - RECON_WINDOW
    mongo_n = await db.transfers.count_documents(
        {"status": "ACCEPTED", "created_at": {"$gte": start, "$lt": end}}, maxTimeMS=QUERY_MS)
    # ReplacingMergeTree는 병합 전 중복이 남을 수 있어 FINAL 대신 고유 키로 센다.
    # ClickHouse created_at(통과 거래)은 거래 시각 → 게이트웨이 시각과 최대 ±30초 차이 (경계 오차)
    sql = (
        "SELECT uniqExact(tx_hash, log_index) FROM transfers "
        f"WHERE status = 'ACCEPTED' AND created_at >= fromUnixTimestamp64Milli({_epoch_ms(start)}) "
        f"AND created_at < fromUnixTimestamp64Milli({_epoch_ms(end)}) FORMAT TabSeparated"
    )
    ch_n = int(await asyncio.to_thread(_ch_query, sql) or 0)
    return mongo_n, ch_n


async def _spark_probe():
    v = await redis_client.get(SPARK_HEARTBEAT_KEY)
    return int(v) if v else None


@router.get("/health", response_model=Health)
async def health():
    now = _now_ms()
    probes = [_redis_probe(), _mongo_probe(), _outbox_probe(), _spark_probe()]
    if CH_URL:
        probes += [_recon_probe(), asyncio.to_thread(_ch_query, "SELECT 1")]
    results = await asyncio.gather(*(_timed(p) for p in probes))
    (r_keys, r_ms, r_err), (m_last, m_ms, m_err), (ob, ob_ms, ob_err), (sp_last, _, _) = results[:4]
    recon, ch_ping = (results[4], results[5]) if CH_URL else ((None, None, "NOT_CONFIGURED"), (None, None, "NOT_CONFIGURED"))

    services, signals = [], {}

    services.append({"id": "gateway", "name": "게이트웨이 (FastAPI)", "role": "R01~R07 입구 검사", "status": "UP",
                     "metric": "이 응답을 보내는 중", "latency": None, "last": now})

    services.append({"id": "redis", "name": "Redis", "role": "5분 카운터 · 보류 · 동결 목록",
                     "status": "DOWN" if r_err else "UP",
                     "metric": f"키 {r_keys:,}개" if r_keys is not None else f"응답 없음 ({r_err})",
                     "latency": r_ms, "last": now if not r_err else None})

    fresh_s = round((now - m_last) / 1000) if m_last else None
    signals["ledger_freshness_s"] = fresh_s
    services.append({"id": "mongo", "name": "MongoDB", "role": "원장 · outbox · 사건 · 감사 기록",
                     "status": "DOWN" if m_err else "UP",
                     "metric": "응답 정상" if not m_err else f"응답 없음 ({m_err})",
                     "latency": m_ms, "last": now if not m_err else None})
    services.append({"id": "freshness", "name": "원장 신선도 (데이터)", "role": "마지막 거래 기록 이후 경과",
                     "status": "DOWN" if m_err else _grade(fresh_s, FRESH_WARN_S, FRESH_DOWN_S),
                     "metric": f"{fresh_s:,}초 전 기록" if fresh_s is not None else "기록 없음",
                     "latency": None, "last": m_last})

    topics = []
    if ob_err:
        signals.update(outbox_pending=None, outbox_oldest_s=None)
        services.append({"id": "outbox", "name": "발행 대기 (데이터)", "role": "outbox → Kafka", "status": "DOWN",
                         "metric": f"조회 실패 ({ob_err})", "latency": None, "last": None})
    else:
        pending, published = ob
        p_by = {p["_id"]: p for p in pending}
        r_by = {p["_id"]: p["n"] for p in published}
        total = sum(p["n"] for p in pending)
        oldest = min((_epoch_ms(p["oldest"]) for p in pending if p.get("oldest")), default=None)
        oldest_s = round((now - oldest) / 1000) if oldest else 0
        signals.update(outbox_pending=total, outbox_oldest_s=oldest_s)
        services.append({"id": "outbox", "name": "발행 대기 (데이터)", "role": "outbox → Kafka (publisher)",
                         "status": _grade(oldest_s, OUTBOX_WARN_S, OUTBOX_DOWN_S),
                         "metric": f"대기 {total:,}건 · 가장 오래된 {oldest_s:,}초", "latency": ob_ms, "last": now})
        for t, desc in TOPIC_DESC.items():
            topics.append({"topic": t, "desc": f"{desc} (lag = 발행 대기)",
                           "lag": p_by.get(t, {}).get("n", 0), "rate": round(r_by.get(t, 0) / 5, 1)})   # 분당

    _, ch_ms, ch_err = ch_ping
    services.append({"id": "clickhouse", "name": "ClickHouse", "role": "장기 보관 · 분석",
                     "status": "WARN" if ch_err == "NOT_CONFIGURED" else ("DOWN" if ch_err else "UP"),
                     "metric": "접속 정보 미설정" if ch_err == "NOT_CONFIGURED" else (f"응답 없음 ({ch_err})" if ch_err else "응답 정상"),
                     "latency": ch_ms, "last": now if not ch_err else None})

    rc, rc_ms, rc_err = recon
    if rc_err:
        signals["recon_ratio"] = None
        rc_status, rc_metric = ("WARN", "ClickHouse 미설정") if rc_err == "NOT_CONFIGURED" else ("DOWN", f"대조 실패 ({rc_err})")
    else:
        mongo_n, ch_n = rc
        ratio = (ch_n / mongo_n) if mongo_n else None
        signals.update(recon_mongo=mongo_n, recon_clickhouse=ch_n, recon_ratio=ratio)
        if ratio is None:
            rc_status, rc_metric = "UP", "비교할 통과 거래 없음 (지난 1시간)"
        else:
            rc_status = "UP" if ratio >= RECON_WARN else ("WARN" if ratio >= RECON_DOWN else "DOWN")
            rc_metric = f"{min(ratio, 1) * 100:.1f}% · Mongo {mongo_n:,} / ClickHouse {ch_n:,}"
    services.append({"id": "reconciliation", "name": "적재 완결성 (데이터)",
                     "role": f"통과 거래 Mongo ↔ ClickHouse · 최근 {int(RECON_WINDOW.total_seconds() // 60)}분 (적재 지연 {int(RECON_DELAY.total_seconds() // 60)}분 제외)",
                     "status": rc_status, "metric": rc_metric, "latency": rc_ms, "last": now if not rc_err else None})

    sp_s = round((now - sp_last) / 1000) if sp_last else None
    signals["spark_heartbeat_s"] = sp_s
    services.append({"id": "spark", "name": "Spark (L1~L8)", "role": "2단계 분석",
                     "status": _grade(sp_s, SPARK_WARN_S, SPARK_DOWN_S),
                     "metric": f"마지막 배치 {sp_s:,}초 전" if sp_s is not None else "하트비트 미연결 (DE 예정)",
                     "latency": None, "last": sp_last})

    services.append({"id": "api", "name": "관제 API", "role": "이 화면에 데이터 제공", "status": "UP",
                     "metric": "연결됨", "latency": None, "last": now})

    return {"mock": False, "checkedAt": now, "services": services, "topics": topics, "signals": signals}


# =====================================================================
# GET /cases, /cases/{id} — 경보(사건)
# =====================================================================
#
# fds_cases 는 alert_consumer.py 가 만든다. 근거(evidence)는 Spark가 남긴 원본 dict이고,
# FE는 [{label, value, threshold, unit, op}] 막대 목록을 그린다 → 여기서 룰별로 바꾼다.
# 기준값은 spark/config.py 와 같은 값이다. (To-Be: common/ 으로 옮겨 한 곳에서 관리)

L_RULES = ("L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8")
RULE_NAMES = {
    "R01": "초고빈도 송금",
    "R02": "쪼개기 송금",
    "R03": "자금 집금",
    "R04": "자금 분산",
    "R05": "대량 송금",
    "R06": "핑퐁 거래",
    "R07": "블랙리스트",
    "L1": "반복 보류",
    "L2": "쪼개서 대량 송금",
    "L3": "장기 쪼개기",
    "L4": "경유 지갑",
    "L5": "위험 지갑 접촉",
    "L6": "급격한 활동 증가",
    "L7": "새 상대 집중",
    "L8": "순환 거래",
}
L1_MIN_HOLDS, L3_MIN_SPLITS, L4_MIN_TIMES = 3, 10, 3
L5_MEDIUM_SUM, L6_RATIO, L6_MIN_COUNT, L7_MIN_NEW = 10_000, 10, 20, 20
L8_RETURN_RATIO, BIG_AMOUNT = 0.8, 1_000

PROFILE = os.getenv("PERIOD_PROFILE", "demo")
PERIOD = {
    "demo": {"long": "20분", "day": "5분", "long_ms": 20 * 60_000, "day_min": 5},
    "prod": {"long": "2주", "day": "24시간", "long_ms": 14 * 86_400_000, "day_min": 1440},
}.get(PROFILE, {"long": "2주", "day": "24시간", "long_ms": 14 * 86_400_000, "day_min": 1440})


def _case_status(c):
    rs = c.get("review_status")
    if rs in ("PENDING", "FROZEN", "DISMISSED"):
        return rs
    if rs == "UNFROZEN":
        return "RELEASED"
    return "AUTO_FROZEN" if c.get("action_taken") == "WALLET_FROZEN" else "PENDING"


def _ev(label, value, threshold, unit, op=">="):
    return {"label": label, "value": float(value or 0), "threshold": float(threshold), "unit": unit, "op": op}


def _evidence(rule, e, c):
    """Spark 근거 dict → (FE 막대 목록, 한 줄 요약, 상대 설명).
    기준값은 케이스에 저장된 criteria(Spark가 판정할 때 쓴 값, STR 초안과 같은 값)를 먼저 쓰고,
    criteria가 없는 예전 케이스만 아래 상수로 대신한다."""
    p = PERIOD
    cr = c.get("criteria") or {}

    def th(key, default):
        return cr.get(key, default)
    if rule == "L1":
        rules = e.get("hold_rules") or []
        return ([_ev("1단계 보류 횟수", e.get("hold_count"), th("min_holds", L1_MIN_HOLDS), "회")],
                f"{p['long']} 안에 1단계 보류가 {e.get('hold_count', 0)}번 걸렸습니다 ({' → '.join(rules)}).",
                f"{rules[0]} 등 보류 {e.get('hold_count', 0)}회" if rules else None)
    if rule == "L2":
        return ([_ev(f"{p['day']} 누적 송금액", e.get("day_sum"), th("min_day_sum", WHALE_LIMIT_USDT), "USDT"),
                 _ev(f"가장 큰 1건 ({th('each_below', WHALE_LIMIT_USDT):,.0f} USDT 미만이어야 해당)", e.get("day_max"), th("each_below", WHALE_LIMIT_USDT), "USDT", "<")],
                f"{p['day']} 동안 {e.get('day_count', 0)}건으로 나눠 {e.get('day_sum', 0):,.0f} USDT를 보냈고, 각 건은 {th('each_below', WHALE_LIMIT_USDT):,.0f} USDT 미만입니다.",
                f"{e.get('day_count', 0)}건 분할")
    if rule == "L3":
        return ([_ev("900~990 USDT 송금 횟수", e.get("split_count"), th("min_count", L3_MIN_SPLITS), "회")],
                f"{p['long']} 동안 900~990 USDT 송금이 {e.get('split_count', 0)}번 있었습니다 (합계 {e.get('split_sum', 0):,.0f} USDT).",
                None)
    if rule == "L4":
        return ([_ev("받은 뒤 1시간 안에 80% 이상 재송금", e.get("pass_through_count"), th("min_times", L4_MIN_TIMES), "회")],
                f"받은 돈의 80% 이상을 1시간 안에 다시 보낸 일이 {e.get('pass_through_count', 0)}번 있었습니다.",
                None)
    if rule == "L5":
        ins, outs = e.get("inflow_from_blacklist") or [], e.get("outflow_to_blacklist") or []
        return ([_ev("블랙리스트에서 받은 상대", len(ins), 0, "곳", ">"),
                 _ev("블랙리스트로 보낸 상대", len(outs), 0, "곳", ">"),
                 _ev("동결 지갑과 거래한 금액", e.get("frozen_exposure_sum"), th("medium_frozen_sum", L5_MEDIUM_SUM), "USDT")],
                f"위험 지갑과 거래했습니다: 블랙리스트에서 받음 {len(ins)}곳, 보냄 {len(outs)}곳, 동결 지갑 노출 {e.get('frozen_exposure_sum', 0):,.0f} USDT ({e.get('grade') or c.get('risk_level')}).",
                _short((ins or outs)[0].get("wallet")) if (ins or outs) else None)
    if rule == "L6":
        base = e.get("baseline_daily_avg") or 0
        ratio, min_count = th("ratio", L6_RATIO), th("min_count", L6_MIN_COUNT)
        need = max(min_count, ratio * base)
        return ([_ev(f"{p['day']} 거래 수 (평소의 {ratio}배·최소 {min_count}건)", e.get("today_count"), need, "건")],
                f"최근 {p['day']} 거래가 {e.get('today_count', 0)}건으로 평소 평균({base})보다 크게 늘었습니다.",
                None)
    if rule == "L7":
        return ([_ev(f"{p['day']} 안에 처음 거래한 상대", e.get("new_counterparty_count"), th("min_new", L7_MIN_NEW), "곳")],
                f"{p['day']} 안에 처음 거래하는 상대가 {e.get('new_counterparty_count', 0)}곳 생겼습니다.",
                None)
    if rule == "L8":
        path = e.get("path") or []
        amounts = e.get("amounts") or [0]
        first, last = c.get("first_tx_at"), c.get("last_tx_at")
        minutes = round((last - first).total_seconds() / 60, 1) if first and last else 0
        return ([_ev("돌아온 비율", (e.get("return_ratio") or 0) * 100, th("min_return_ratio", L8_RETURN_RATIO) * 100, "%"),
                 _ev("가장 작은 1건", min(amounts), th("min_leg_amount", BIG_AMOUNT), "USDT"),
                 _ev("한 바퀴 걸린 시간", minutes, p["day_min"], "분", "<=")],
                f"{' → '.join(_short(a) for a in path)} 로 {minutes}분 만에 돈이 한 바퀴 돌아 {(e.get('return_ratio') or 0) * 100:.0f}%가 돌아왔습니다.",
                _short(c.get("counterparty_address")))
    return [], f"{rule} 경보", None


KST = timezone(timedelta(hours=9))


def _kst_time(iso, with_date):
    try:
        t = datetime.fromisoformat(iso).astimezone(KST)
    except (TypeError, ValueError):
        return "?"
    return t.strftime("%m-%d %H:%M" if with_date else "%H:%M:%S")


def _recompute_note(c):
    """사건 한 줄 요약 끝에 붙이는 Spark 재계산 결과 (화면 수정 없이 글로 보여준다).
    예) · Spark 재계산 일치 (10:05:00~10:10:00 KST, 구성 거래 12건)"""
    rc = c.get("recompute") or {}
    status = rc.get("status")
    if not status:
        return ""
    if status == "ERROR":
        return " · Spark 재계산 실패 (판정 값만 표시)"
    w = rc.get("window") or {}
    try:
        long_span = datetime.fromisoformat(w["to"]) - datetime.fromisoformat(w["from"]) >= timedelta(hours=1)
    except (KeyError, TypeError, ValueError):
        long_span = False
    span = f"{_kst_time(w.get('from'), long_span)}~{_kst_time(w.get('to'), long_span)} KST"
    if status == "MATCH":
        sm = rc.get("summary") or {}
        cps = f", 상대 {sm['receiver_count']}곳" if "receiver_count" in sm else ""
        peak = (rc.get("windows_5m") or {}).get("peak_count")
        w5 = f", 5분 최대 {peak['count']}건" if peak else ""
        return f" · Spark 재계산 일치 ({span}, 구성 거래 {rc.get('tx_count', 0)}건{cps}{w5})"
    bad = [x for x in rc.get("checks") or [] if not x.get("match")]
    diff = ", ".join(f"{x.get('label')} 판정 {_short_value(x.get('detector'))} / Spark {_short_value(x.get('spark'))}"
                     for x in bad[:2])
    return f" · Spark 재계산 불일치 ({span}, {diff})"


def _short_value(v):
    if isinstance(v, list):
        return f"{len(v)}곳"
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def _evidence_amount(c):
    """근거 거래가 없는 사건(L5·L7)은 Spark 근거의 금액으로 대신한다."""
    e = c.get("evidence") or {}
    flows = (e.get("inflow_from_blacklist") or []) + (e.get("outflow_to_blacklist") or [])
    return float(sum((f.get("amount") or 0) for f in flows) or e.get("frozen_exposure_sum") or 0)


def _case_item(c, origins, amounts):
    rule = c.get("rule_id")
    evidence, headline, counterpart = _evidence(rule, c.get("evidence") or {}, c)
    decided = None
    if c.get("reviewed_by"):
        decided = {"status": _case_status(c), "actor": c.get("reviewed_by"),
                   "ts": _epoch_ms(c.get("reviewed_at")), "memo": c.get("review_rationale") or c.get("review_memo")}
    return {
        "case_id": str(c["_id"]),
        "rule_id": rule,
        "rule_name": c.get("rule_name"),
        "risk_level": c.get("risk_level"),
        "violator_address": c.get("violator_address"),
        "origin": origins.get(c.get("violator_address"), "MAINNET"),
        "isNew": False,                               # 지갑 첫 등장 시각은 아직 추적하지 않음
        "detected_at": _epoch_ms(c.get("detected_at")),
        "status": _case_status(c),
        "period_profile": PROFILE,
        "tx_hashes": c.get("tx_hashes") or [],
        "evidence": evidence,
        "evidence_raw": c.get("evidence") or {},
        "headline": headline + _recompute_note(c),
        "counterpart": counterpart,
        "amount": round(amounts.get(str(c["_id"])) or _evidence_amount(c), 6),
        "decided": decided,
        "label": entities.label(c.get("violator_address")),
    }


async def _case_amounts(cases):
    """사건별 관련 금액 = 근거 거래(tx_hashes)의 원장 금액 합 (같은 거래는 한 번만)."""
    hashes = sorted({h for c in cases for h in (c.get("tx_hashes") or [])})
    if not hashes:
        return {}
    docs = await (db.transfers.find({"tx_hash": {"$in": hashes}},
                                    {"_id": 0, "tx_hash": 1, "log_index": 1, "status": 1, "amount": 1})
                  .max_time_ms(QUERY_MS).to_list())
    by_hash = {}
    for d in docs:
        by_hash.setdefault(d["tx_hash"], {})[(d.get("log_index"), d.get("status"))] = _number(d.get("amount")) or 0
    out = {}
    for c in cases:
        total = 0.0
        for h in set(c.get("tx_hashes") or []):
            total += sum(by_hash.get(h, {}).values())
        out[str(c["_id"])] = total
    return out


async def _case_items(cases):
    origins = await _origins({c.get("violator_address") for c in cases if c.get("violator_address")})
    amounts = await _case_amounts(cases)
    return [_case_item(c, origins, amounts) for c in cases]


def _fnv1a_32(s: str) -> str:
    h = 0x811C9DC5
    for ch in s:
        h ^= ord(ch)
        h = (h * 0x01000193) & 0xFFFFFFFF
    return f"{h:08x}"


def _parse_expand(expand: str | list[str] | None) -> list[str]:
    if not expand:
        return []
    if isinstance(expand, str):
        return [x.strip() for x in expand.split(",") if x.strip()]
    out = []
    for item in expand:
        for x in item.split(","):
            if x.strip():
                out.append(x.strip())
    return out


async def _get_audit_chain(limit: int = 1000) -> list[AuditRow]:
    """감사 이력 = 케이스 생성(탐지) + 감사 원장 policy_decisions(자동 동결·직원 조치·목록 변경·STR).
    조치 기록은 admin.py·alert_consumer.py·str_draft.py가 policy_decisions에 한 줄씩 남긴 것을 그대로 쓴다."""
    events = []
    # policy_decisions action → 화면 action 이름
    action_name = {"UNFREEZE": "RELEASE", "STR_DRAFT_SAVE": "STR_DRAFT"}
    try:
        decisions = await db.policy_decisions.find().sort("created_at", 1).limit(5000).max_time_ms(QUERY_MS).to_list()
        system_frozen = {d.get("case_id") for d in decisions if d.get("actor_type") == "SYSTEM" and d.get("action") == "FREEZE"}
        for d in decisions:
            system = d.get("actor_type") == "SYSTEM"
            action = d.get("action") or "DECISION"
            note = d.get("rationale") or ""
            if d.get("reason_code") and d.get("reason_code") not in note:
                note = f"{d['reason_code']} · {note}" if note else d["reason_code"]
            events.append({
                "ts": _epoch_ms(d.get("created_at")),
                "actor": "SYSTEM (Spark)" if system else (d.get("actor_id") or ""),
                "action": "AUTO_FREEZE" if system and action == "FREEZE" else action_name.get(action, action),
                "case_id": d.get("case_id"),
                "wallet": d.get("wallet"),
                "note": note,
            })

        cases = await db.fds_cases.find().sort("detected_at", 1).limit(2000).max_time_ms(QUERY_MS).to_list()
        for c in cases:
            cid = str(c["_id"])
            w = c.get("violator_address")
            det_ts = _epoch_ms(c.get("detected_at"))
            if det_ts is None:
                continue
            events.append({
                "ts": det_ts,
                "actor": "SYSTEM (Spark)",
                "action": "CASE_CREATED",
                "case_id": cid,
                "wallet": w,
                "note": f"{c.get('rule_id')} {c.get('rule_name') or ''}".strip(),
            })
            # 감사 원장이 생기기 전에 자동 동결된 케이스는 케이스 기록으로 보충
            if c.get("action_taken") == "WALLET_FROZEN" and c.get("rule_id") != "MANUAL" and cid not in system_frozen                     and c.get("review_status") in (None, "UNFROZEN"):
                events.append({
                    "ts": det_ts + 1,
                    "actor": "SYSTEM (Spark)",
                    "action": "AUTO_FREEZE",
                    "case_id": cid,
                    "wallet": w,
                    "note": f"FROZEN_{c.get('rule_id')}",
                })
    except PyMongoError:
        pass

    events.sort(key=lambda x: x.get("ts") or 0)
    prev = "00000000"
    chained = []
    for i, r in enumerate(events):
        payload = json.dumps([r.get("ts"), r.get("actor"), r.get("action"), r.get("case_id"), r.get("note")],
                             ensure_ascii=False, separators=(",", ":"))
        h = _fnv1a_32(prev + payload)
        row = AuditRow(
            seq=i + 1,
            ts=r.get("ts") or 0,
            actor=r.get("actor") or "",
            action=r.get("action") or "",
            case_id=r.get("case_id"),
            wallet=r.get("wallet"),
            note=r.get("note"),
            prev=prev,
            hash=h,
        )
        prev = h
        chained.append(row)
    chained.reverse()
    return chained[:limit]


@router.get("/cases", response_model=list[CaseItem])
async def list_cases(limit: int = Query(default=200, ge=1, le=1000)):
    """L1~L8 사건 최신순. 직원 수동 동결(MANUAL)은 사건이 아니라 조치라 감사 로그에서 본다."""
    try:
        cases = await (db.fds_cases.find({"rule_id": {"$in": list(L_RULES)}})
                       .sort("detected_at", -1).limit(limit).max_time_ms(QUERY_MS).to_list())
        return await _case_items(cases)
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")


@router.get("/cases/{case_id}", response_model=CaseDetail)
async def get_case(case_id: str):
    try:
        oid = ObjectId(case_id)
    except (InvalidId, TypeError):
        raise AdminError(404, "CASE_NOT_FOUND")
    try:
        c = await db.fds_cases.find_one({"_id": oid}, max_time_ms=QUERY_MS)
        if not c or c.get("rule_id") not in L_RULES:
            raise AdminError(404, "CASE_NOT_FOUND")
        w = c["violator_address"]
        item = (await _case_items([c]))[0]

        # 근거 거래 (판정 결과 포함)
        related_docs = []
        if c.get("tx_hashes"):
            related_docs = await (db.transfers.find({"tx_hash": {"$in": c["tx_hashes"]}}, TRANSFER_FIELDS)
                                  .sort("created_at", -1).max_time_ms(QUERY_MS).to_list())
        related = await _transfer_rows(related_docs)

        # 분석 기간 안의 통과 거래 → 자금 흐름·거래 상대 (from/to + timestamp 인덱스)
        since = (c.get("detected_at") or datetime.now(timezone.utc)) - timedelta(milliseconds=PERIOD["long_ms"])
        fields = {"_id": 0, "from": 1, "to": 1, "amount": 1, "created_at": 1}
        sent_docs = await (db.transfers.find({"from": w, "status": "ACCEPTED", "timestamp": {"$gte": since}}, fields)
                           .limit(2000).max_time_ms(QUERY_MS).to_list())
        recv_docs = await (db.transfers.find({"to": w, "status": "ACCEPTED", "timestamp": {"$gte": since}}, fields)
                           .limit(2000).max_time_ms(QUERY_MS).to_list())

        holds = await (db.fds_holds.find({"wallet": w, "held_at": {"$gte": since}}, {"_id": 0, "rule": 1})
                       .sort("held_at", -1).limit(100).max_time_ms(QUERY_MS).to_list())
        others = await (db.fds_cases.find({"violator_address": w, "_id": {"$ne": oid}, "rule_id": {"$in": list(L_RULES)}})
                        .sort("detected_at", -1).limit(20).max_time_ms(QUERY_MS).to_list())
        other_items = await _case_items(others)
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")

    flows, cps = [], {}
    for d, direction in [(x, "out") for x in sent_docs] + [(x, "in") for x in recv_docs]:
        amt = _number(d.get("amount")) or 0
        flows.append({"ts": _epoch_ms(d.get("created_at")), "in": amt if direction == "in" else 0,
                      "out": amt if direction == "out" else 0})
        other = d.get("to") if direction == "out" else d.get("from")
        cp = cps.setdefault(other, {"address": other, "in": 0.0, "out": 0.0, "n": 0})
        cp[direction] += amt
        cp["n"] += 1
    flows.sort(key=lambda f: f["ts"] or 0)
    cp_origins = await _origins(cps.keys())
    top = sorted(cps.values(), key=lambda p: p["in"] + p["out"], reverse=True)[:5]
    for p in top:
        p["origin"], p["label"] = cp_origins.get(p["address"], "MAINNET"), entities.label(p["address"])

    try:
        sdn = sorted(await redis_client.smembers(BLACKLIST_KEY))[:1000]
    except RedisError:
        sdn = []

    chain = await _get_audit_chain(limit=500)
    case_audit = [r for r in chain if r.case_id == case_id]

    detail = {
        **item,
        "related": related,
        "flows": flows,
        "summary": {
            "received": round(sum(f["in"] for f in flows), 6),
            "sent": round(sum(f["out"] for f in flows), 6),
            "counterparties": len(cps),
            "holds": [h.get("rule") for h in holds if h.get("rule")],
            "firstSeen": None,
        },
        "counterparties": top,
        "otherCases": other_items,
        "audit": case_audit,
        "sdn": sdn,
        "recompute": c.get("recompute"),
    }
    return detail                                     # response_model 이 형식을 검증하고 별칭(from·in)으로 내보낸다


@router.get("/cases/{case_id}/graph", response_model=GraphResponse)
async def get_case_graph(
    case_id: str,
    depth: int = Query(default=1, ge=1, le=2),
    minAmount: float = Query(default=0, ge=0),
    from_: int | None = Query(default=None, alias="from"),
    to: int | None = Query(default=None),
    expand: str | None = Query(default=None),
):
    """자금 흐름 그래프 데이터 (FE Flow.jsx 및 store.js:getGraph 규격)."""
    try:
        oid = ObjectId(case_id)
    except (InvalidId, TypeError):
        raise AdminError(404, "CASE_NOT_FOUND")

    try:
        c_doc = await db.fds_cases.find_one({"_id": oid}, max_time_ms=QUERY_MS)
        if not c_doc or c_doc.get("rule_id") not in L_RULES:
            raise AdminError(404, "CASE_NOT_FOUND")
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")

    case_detail_dict = await get_case(case_id)
    center = c_doc["violator_address"]
    hit_hashes = set(c_doc.get("tx_hashes") or [])

    detected_ms = _epoch_ms(c_doc.get("detected_at")) or _now_ms()
    win_from_ms = from_ if from_ is not None else (detected_ms - PERIOD["long_ms"] - 30 * 60 * 1000)
    win_to_ms = to if to is not None else _now_ms()

    win_from_dt = datetime.fromtimestamp(win_from_ms / 1000, timezone.utc)
    win_to_dt = datetime.fromtimestamp(win_to_ms / 1000, timezone.utc)
    time_query = {"$gte": win_from_dt, "$lte": win_to_dt}
    fields = {"_id": 0, "tx_hash": 1, "from": 1, "to": 1, "amount": 1, "created_at": 1, "timestamp": 1}

    expand_wallets = _parse_expand(expand)
    nodes = {center: 0}
    for w in expand_wallets:
        nodes[w] = nodes.get(w, 1)

    frontier = [center] + [w for w in expand_wallets if w != center]
    edges = {}

    max_steps = depth + (1 if expand_wallets else 0)
    try:
        for _ in range(max_steps):
            next_frontier = []
            for a in frontier:
                sent_coro = db.transfers.find({"from": a, "status": "ACCEPTED", "timestamp": time_query}, fields).limit(1000).max_time_ms(QUERY_MS).to_list()
                recv_coro = db.transfers.find({"to": a, "status": "ACCEPTED", "timestamp": time_query}, fields).limit(1000).max_time_ms(QUERY_MS).to_list()
                sent, recv = await asyncio.gather(sent_coro, recv_coro)

                for t in sent + recv:
                    amt = _number(t.get("amount")) or 0.0
                    tx_h = t.get("tx_hash")
                    if amt < minAmount and tx_h not in hit_hashes:
                        continue
                    u, v = t.get("from"), t.get("to")
                    if not u or not v:
                        continue
                    other = v if u == a else u
                    key = (u, v)
                    ts = _epoch_ms(t.get("timestamp") or t.get("created_at"))
                    if key not in edges:
                        edges[key] = {
                            "from": u, "to": v, "amount": 0.0, "n": 0,
                            "txs": [], "hit": False, "ts": ts
                        }
                    e = edges[key]
                    if tx_h and tx_h not in e["txs"]:
                        e["amount"] += amt
                        e["n"] += 1
                        e["txs"].append(tx_h)
                        if ts is not None and (e["ts"] is None or ts < e["ts"]):
                            e["ts"] = ts
                        if tx_h in hit_hashes:
                            e["hit"] = True
                    if other not in nodes:
                        nodes[other] = nodes[a] + 1
                        next_frontier.append(other)
            frontier = next_frontier
            if len(nodes) > 30:
                break

        shown_wallets = list(nodes.keys())[:30]
        shown_set = set(shown_wallets)

        if len(shown_set) > 1:
            cross_docs = await db.transfers.find(
                {"from": {"$in": shown_wallets}, "to": {"$in": shown_wallets}, "status": "ACCEPTED", "timestamp": time_query},
                fields
            ).limit(1000).max_time_ms(QUERY_MS).to_list()
            for t in cross_docs:
                amt = _number(t.get("amount")) or 0.0
                tx_h = t.get("tx_hash")
                if amt < minAmount and tx_h not in hit_hashes:
                    continue
                u, v = t.get("from"), t.get("to")
                if not u or not v:
                    continue
                key = (u, v)
                ts = _epoch_ms(t.get("timestamp") or t.get("created_at"))
                if key not in edges:
                    edges[key] = {
                        "from": u, "to": v, "amount": 0.0, "n": 0,
                        "txs": [], "hit": False, "ts": ts
                    }
                e = edges[key]
                if tx_h and tx_h not in e["txs"]:
                    e["amount"] += amt
                    e["n"] += 1
                    e["txs"].append(tx_h)
                    if ts is not None and (e["ts"] is None or ts < e["ts"]):
                        e["ts"] = ts
                    if tx_h in hit_hashes:
                        e["hit"] = True
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")

    origins = await _origins(shown_wallets)
    frozen_set = set()
    try:
        is_frozen_flags = await redis_client.smismember(FROZEN_KEY, shown_wallets)
        for addr, is_f in zip(shown_wallets, is_frozen_flags):
            if is_f:
                frozen_set.add(addr)
    except RedisError:
        pass

    try:
        frozen_cases = await db.fds_cases.find(
            {"violator_address": {"$in": shown_wallets}, "action_taken": "WALLET_FROZEN",
             "review_status": {"$in": [None, "AUTO_FROZEN", "FROZEN", "PENDING"]}},
            {"_id": 0, "violator_address": 1}
        ).max_time_ms(QUERY_MS).to_list()
        for fc in frozen_cases:
            frozen_set.add(fc["violator_address"])
    except PyMongoError:
        pass

    edge_values = list(edges.values())
    node_list = []
    for addr in shown_wallets:
        level = nodes.get(addr, 0)
        origin = origins.get(addr, "MAINNET")
        node_list.append(GraphNode(
            address=addr,
            level=level,
            origin=origin,
            label=entities.label(addr),
            frozen=(addr in frozen_set),
            sdn=(origin == "SDN"),
            center=(addr == center),
            onPath=any(e["hit"] and (e["from"] == addr or e["to"] == addr) for e in edge_values),
        ))

    edge_list = []
    for e in edge_values:
        if e["from"] in shown_set and e["to"] in shown_set:
            edge_list.append(GraphEdge(
                from_=e["from"],
                to=e["to"],
                amount=round(e["amount"], 6),
                n=e["n"],
                txs=e["txs"],
                hit=e["hit"],
                ts=e["ts"],
            ))

    hit_times = sorted([
        t.ts for t in case_detail_dict.get("related", [])
        if getattr(t, "status", None) == "ACCEPTED" and getattr(t, "ts", None) is not None
        and getattr(t, "tx_hash", None) in hit_hashes
    ])

    return GraphResponse(
        case=case_detail_dict,
        nodes=node_list,
        edges=edge_list,
        window=GraphWindow(from_=win_from_ms, to=win_to_ms),
        hitTimes=hit_times,
    )


@router.get("/audit", response_model=list[AuditRow])
async def get_audit(limit: int = Query(default=1000, ge=1, le=5000)):
    """감사 로그 (policy_decisions) — 시간순 해시 체인으로 무결성 확인."""
    return await _get_audit_chain(limit=limit)


# =====================================================================
# GET /stats — 대시보드 집계 (FE store.js getStats 형식)
# =====================================================================

class Kpi(BaseModel):
    pending: int                  # 검토 대기: 기간과 무관하게 "지금" 남은 수
    pendingCritical: int
    autoFrozen: int               # 이하 기간 안에 탐지된 사건의 현재 상태 기준
    staffFrozen: int
    dismissed: int
    holds: int                    # 1단계 보류 (fds_holds)
    blocks: int                   # R07 블랙리스트 차단 (403)
    frozenTries: int              # 동결 지갑의 송금 시도 (403 FROZEN_*)
    inflowPerMin: int             # 최근 1분 게이트웨이 유입
    inflowTotal: int


class Grade(BaseModel):
    grade: Literal["CRITICAL", "HIGH", "MEDIUM"]
    n: int


class Point(BaseModel):
    t: int                        # 버킷 시작 epoch ms
    inflow: int
    hold: int
    block: int
    freeze: int


class SparkPoint(BaseModel):
    v: int


class TopCase(CaseItem):
    rules: list[str]              # 사건 룰 + 이 지갑이 걸린 1단계 룰
    spark: list[SparkPoint]       # 최근 24시간 활동 (2시간 × 12)


class Sparks(BaseModel):
    pending: list[SparkPoint]
    auto: list[SparkPoint]
    holds: list[SparkPoint]
    blocks: list[SparkPoint]
    dismissed: list[SparkPoint]


class Stats(BaseModel):
    range: Literal["1h", "24h", "14d"]
    kpi: Kpi
    grades: list[Grade]
    series: list[Point]
    ruleCounts: dict[str, int]
    top: list[TopCase]
    oldNote: str | None
    sparks: Sparks


RANGE_SPAN = {"1h": timedelta(hours=1), "24h": timedelta(hours=24), "14d": timedelta(days=14)}
RANGE_BUCKET = {"1h": ("minute", 1), "24h": ("minute", 30), "14d": ("day", 1)}
BUCKET_MS = {"1h": 60_000, "24h": 1_800_000, "14d": 86_400_000}
STATS_TZ = os.getenv("STATS_TZ", "Asia/Seoul")      # 일 단위 버킷을 한국 날짜로 자른다
SPARK_N, SPARK_MS = 12, 2 * 3_600_000               # 작은 추이선: 2시간 × 12 = 24시간
PENDING_Q = {"$or": [{"review_status": "PENDING"},
                     {"review_status": None, "action_taken": {"$ne": "WALLET_FROZEN"}}]}  # _case_status 와 같은 규칙
AUTO_Q = {"review_status": None, "action_taken": "WALLET_FROZEN"}
GRADE_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}


async def _agg(coll, pipeline):
    """aggregate 한 번. 2초 상한 (송금 처리를 붙잡지 않게)."""
    cur = await coll.aggregate(pipeline, maxTimeMS=QUERY_MS)
    return await cur.to_list()


def _bucket_expr(field, unit, size):
    return {"$dateTrunc": {"date": f"${field}", "unit": unit, "binSize": size, "timezone": STATS_TZ}}


async def _bucketed(coll, match, field, unit, size):
    """{버킷 epoch ms: 건수}"""
    rows = await _agg(coll, [{"$match": match},
                             {"$group": {"_id": _bucket_expr(field, unit, size), "n": {"$sum": 1}}}])
    return {_epoch_ms(r["_id"]): r["n"] for r in rows if r.get("_id")}


async def _spark(coll, match, field, now_ms):
    """최근 24시간을 2시간 칸 12개로 센다 (지금 기준으로 칸을 나눔)."""
    start = now_ms - SPARK_N * SPARK_MS
    rows = await _agg(coll, [
        {"$match": {**match, field: {"$gte": datetime.fromtimestamp(start / 1000, timezone.utc)}}},
        {"$group": {"_id": {"$floor": {"$divide": [{"$subtract": [{"$toLong": f"${field}"}, start]}, SPARK_MS]}},
                    "n": {"$sum": 1}}},
    ])
    by = {int(r["_id"]): r["n"] for r in rows if r.get("_id") is not None}
    return [{"v": by.get(i, 0)} for i in range(SPARK_N)]


async def _wallet_activity(wallet, now_ms):
    start = now_ms - SPARK_N * SPARK_MS
    since = datetime.fromtimestamp(start / 1000, timezone.utc)
    docs = []
    for side in ("from", "to"):           # from·to 인덱스를 각각 타도록 두 번 조회
        docs += await (db.transfers.find({side: wallet, "created_at": {"$gte": since}}, {"_id": 0, "created_at": 1})
                       .limit(5000).max_time_ms(QUERY_MS).to_list())
    pts = [0] * SPARK_N
    for d in docs:
        ms = _epoch_ms(d.get("created_at"))
        if ms is not None and ms >= start:
            pts[min(SPARK_N - 1, (ms - start) // SPARK_MS)] += 1
    return [{"v": v} for v in pts]


@router.get("/stats", response_model=Stats)
async def stats(range: Literal["1h", "24h", "14d"] = Query(default="24h")):
    """대시보드 첫 화면. 모든 숫자는 Mongo에서 집계 (aggregate 1회당 2초 상한).

    - 유입 = 게이트웨이가 원장(transfers)에 남긴 모든 요청 (통과 + 거절)
    - 보류 = fds_holds, 차단 = 거절 중 BLACKLISTED*, 동결 시도 = 거절 중 FROZEN_*
    - 사건 상태는 "지금" 상태 기준 (탐지 후 해제·종결되면 그쪽으로 센다)
    """
    now = datetime.now(timezone.utc)
    now_ms = _epoch_ms(now)
    since = now - RANGE_SPAN[range]
    unit, size = RANGE_BUCKET[range]
    in_range = lambda f: {f: {"$gte": since}}
    blocks_q = {"status": "REJECTED", "reason": {"$regex": "^BLACKLISTED"}}
    tries_q = {"status": "REJECTED", "reason": {"$regex": "^FROZEN_"}}
    case_q = {"rule_id": {"$in": list(L_RULES)}}

    try:
        inflow_b, hold_b, block_b, freeze_b, tries_n, last_min = await asyncio.gather(
            _bucketed(db.transfers, in_range("created_at"), "created_at", unit, size),
            _bucketed(db.fds_holds, in_range("held_at"), "held_at", unit, size),
            _bucketed(db.transfers, {**blocks_q, **in_range("created_at")}, "created_at", unit, size),
            _bucketed(db.fds_cases, {**case_q, **AUTO_Q, **in_range("detected_at")}, "detected_at", unit, size),
            db.transfers.count_documents({**tries_q, **in_range("created_at")}, maxTimeMS=QUERY_MS),
            db.transfers.count_documents({"created_at": {"$gte": now - timedelta(minutes=1)}}, maxTimeMS=QUERY_MS),
        )
        hold_rules = await _agg(db.fds_holds, [{"$match": in_range("held_at")},
                                               {"$group": {"_id": "$rule", "n": {"$sum": 1}}}])
        case_groups = await _agg(db.fds_cases, [
            {"$match": {**case_q, **in_range("detected_at")}},
            {"$group": {"_id": {"rule": "$rule_id", "rs": "$review_status", "act": "$action_taken"}, "n": {"$sum": 1}}},
        ])
        pending_groups = await _agg(db.fds_cases, [
            {"$match": {**case_q, **PENDING_Q}},
            {"$group": {"_id": "$risk_level", "n": {"$sum": 1}}},
        ])
        sparks = dict(zip(("pending", "auto", "holds", "blocks", "dismissed"), await asyncio.gather(
            _spark(db.fds_cases, {**case_q, **PENDING_Q}, "detected_at", now_ms),
            _spark(db.fds_cases, {**case_q, **AUTO_Q}, "detected_at", now_ms),
            _spark(db.fds_holds, {}, "held_at", now_ms),
            _spark(db.transfers, blocks_q, "created_at", now_ms),
            _spark(db.fds_cases, {**case_q, "review_status": "DISMISSED"}, "detected_at", now_ms),
        )))

        # 위험 지갑 상위: 기간 안 사건 중 검토 대기 → 등급 → 금액 순
        recent = await (db.fds_cases.find({**case_q, **in_range("detected_at")})
                        .sort("detected_at", -1).limit(300).max_time_ms(QUERY_MS).to_list())
        items = await _case_items(recent)
        items.sort(key=lambda c: (c["status"] != "PENDING", GRADE_ORDER.get(c["risk_level"], 9), -c["amount"]))
        top = []
        for c in items[:6]:
            w = c["violator_address"]
            wrules = await (db.fds_holds.distinct("rule", {"wallet": w, "held_at": {"$gte": since}}))
            top.append({**c, "rules": list(dict.fromkeys([c["rule_id"], *sorted(r for r in wrules if r)])),
                        "spark": await _wallet_activity(w, now_ms)})
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")

    # 시계열: 빈 칸도 0으로 채운다 (차트 x축이 끊기지 않게)
    keys = set(inflow_b) | set(hold_b) | set(block_b) | set(freeze_b)
    if range == "14d":
        series_keys = keys                # 일 단위는 한국 날짜 경계라 데이터가 있는 날만
    else:
        step = BUCKET_MS[range]           # 분 단위 버킷은 UTC/KST 경계가 같다 → 빈 칸을 0으로 채움
        series_keys = keys | set(_steps((_epoch_ms(since) // step) * step, now_ms, step))
    series = [{"t": k, "inflow": inflow_b.get(k, 0), "hold": hold_b.get(k, 0),
               "block": block_b.get(k, 0), "freeze": freeze_b.get(k, 0)} for k in sorted(series_keys)]

    st = {"AUTO_FROZEN": 0, "FROZEN": 0, "DISMISSED": 0}
    rule_counts = {r["_id"]: r["n"] for r in hold_rules if r.get("_id")}
    rule_counts["R07"] = sum(block_b.values())
    for g in case_groups:
        k = g["_id"]
        status = _case_status({"review_status": k.get("rs"), "action_taken": k.get("act")})
        if status in st:
            st[status] += g["n"]
        rule_counts[k["rule"]] = rule_counts.get(k["rule"], 0) + g["n"]
    pend = {g["_id"]: g["n"] for g in pending_groups}

    return {
        "range": range,
        "kpi": {
            "pending": sum(pend.values()),
            "pendingCritical": pend.get("CRITICAL", 0),
            "autoFrozen": st["AUTO_FROZEN"],
            "staffFrozen": st["FROZEN"],
            "dismissed": st["DISMISSED"],
            "holds": sum(hold_b.values()),
            "blocks": sum(block_b.values()),
            "frozenTries": tries_n,
            "inflowPerMin": last_min,
            "inflowTotal": sum(inflow_b.values()),
        },
        "grades": [{"grade": g, "n": pend.get(g, 0)} for g in ("CRITICAL", "HIGH", "MEDIUM")],
        "series": series,
        "ruleCounts": rule_counts,
        "top": top,
        "oldNote": None,                  # 원장 보관 기간(TTL) 안이면 2주 전체가 실제 기록
        "sparks": sparks,
    }


def _steps(first, last, step):
    t = first
    while t <= last:
        yield t
        t += step


# =====================================================================
# POST /cases/{id}/decision — FE 조치 버튼 어댑터
# =====================================================================

class Decision(BaseModel):
    action: Literal["FREEZE", "DISMISS", "RELEASE", "STR_DRAFT"]
    actor: str = Field(min_length=1, max_length=64)       # FE 로그인 이름 → admin.py operator
    note: str | None = Field(default=None, max_length=1000)


class DecisionResult(BaseModel):
    ok: bool
    status: CaseStatus | None                             # 조치 후 화면 상태 (STR_DRAFT는 상태 변화 없음)
    result: str | None                                    # admin.py 결과 (FROZEN · ALREADY_FROZEN · DISMISSED · UNFROZEN)


DECISION_STATUS = {"FREEZE": "FROZEN", "DISMISS": "DISMISSED", "RELEASE": "RELEASED"}


@router.post("/cases/{case_id}/decision", response_model=DecisionResult)
async def decide_case(case_id: str, body: Decision):
    """FE api.decide(id, action, {actor, note}) 형식을 admin.py 조치로 연결한다.

    - FREEZE  → POST /cases/{id}/freeze   (검토 대기 사건 → 직원 동결)
    - DISMISS → POST /cases/{id}/dismiss  (검토 대기만 가능. 동결된 사건은 409 NOT_PENDING → RELEASE 사용)
    - RELEASE → POST /wallets/{지갑}/unfreeze (지갑 단위: 그 지갑의 동결 사건이 모두 해제됨)
    - STR_DRAFT → 상태 변화 없음. 기록만 (policy_decisions 연결 전이라 로그)
    오류(404·409·503)는 admin.py 가 낸 그대로 돌려준다.
    """
    from app import admin as A                            # 순환 import 방지
    # 직원 API는 사유(rationale)가 필수다. 화면에서 비워 보내면 기본 문구로 남긴다 (감사 원장에 그대로 기록)
    review = A.Review(operator=body.actor, rationale=(body.note or "").strip() or "관제 화면 조치 (사유 미입력)")
    if body.action == "FREEZE":
        r = await A.freeze_case(case_id, review)
    elif body.action == "DISMISS":
        r = await A.dismiss_case(case_id, review)
    elif body.action == "RELEASE":
        case = await A._find_case(case_id)
        r = await A.unfreeze_wallet(case["violator_address"], review)
    else:
        case = await A._find_case(case_id)                # 없는 사건이면 404
        A._audit("STR_DRAFT", case_id=case_id, operator=body.actor)
        try:
            # 감사 원장 형식(decision_id 포함)은 admin._decision과 같게
            await db.policy_decisions.insert_one(
                A._decision("STR_DRAFT_SAVE", case["violator_address"], review, datetime.now(timezone.utc),
                            "STR_DRAFTED", case["_id"]))
        except PyMongoError:
            pass
        return {"ok": True, "status": None, "result": None}
    return {"ok": True, "status": DECISION_STATUS[body.action], "result": r.get("result")}


# =====================================================================
# GET /search — 전역 검색 (지갑 주소 · 사건 ID · tx_hash)
# =====================================================================

@router.get("/search", response_model=list[SearchItem])
async def search_admin(q: str = Query(default="", max_length=128)):
    """전역 검색: 지갑 주소 · 사건 ID · tx_hash.

    - 3자 미만: 빈 리스트 [] 반환 (과도한 전체 스캔 방지)
    - 최대 8건 반환
    - 정규식 특수문자 이스케이프(re.escape) 및 prefix(^) 검색 적용
    """
    s = q.strip()
    if len(s) < 3:
        return []

    s_lower = s.lower()
    escaped_prefix = f"^{re.escape(s_lower)}"
    out: list[SearchItem] = []
    seen: set[tuple[str, str]] = set()

    def add_item(kind: Literal["사건", "지갑", "거래"], item_id: str, text: str, to: str):
        key = (kind, item_id)
        if key not in seen and len(out) < 8:
            seen.add(key)
            out.append(SearchItem(kind=kind, id=item_id, text=text, to=to))

    try:
        # 1. 사건 검색 (Case ID)
        # 1-1) 24자리 ObjectId 직접 조회
        if len(s) == 24:
            try:
                oid = ObjectId(s)
                c = await db.fds_cases.find_one({"_id": oid}, max_time_ms=QUERY_MS)
                if c:
                    cid = str(c["_id"])
                    r_id = c.get("rule_id", "")
                    r_name = c.get("rule_name") or RULE_NAMES.get(r_id, "")
                    txt = f"{cid} · {r_id} {r_name}".strip()
                    add_item("사건", cid, txt, f"/cases/{cid}")
            except (InvalidId, TypeError):
                pass

        # 1-2) 최근 사건들 중 case_id 접두사 일치
        if len(out) < 8:
            recent_cases = await (
                db.fds_cases.find({"rule_id": {"$in": list(L_RULES)}})
                .sort("detected_at", -1)
                .limit(200)
                .max_time_ms(QUERY_MS)
                .to_list()
            )
            for c in recent_cases:
                cid = str(c["_id"])
                if cid.lower().startswith(s_lower):
                    r_id = c.get("rule_id", "")
                    r_name = c.get("rule_name") or RULE_NAMES.get(r_id, "")
                    txt = f"{cid} · {r_id} {r_name}".strip()
                    add_item("사건", cid, txt, f"/cases/{cid}")
                    if len(out) >= 8:
                        break

        # 2. 지갑 주소 검색 (violator_address prefix)
        if len(out) < 8:
            wallet_cases = await (
                db.fds_cases.find(
                    {"violator_address": {"$regex": escaped_prefix, "$options": "i"}},
                    max_time_ms=QUERY_MS
                )
                .sort("detected_at", -1)
                .limit(8)
                .to_list()
            )
            for c in wallet_cases:
                cid = str(c["_id"])
                w = c.get("violator_address", "")
                r_id = c.get("rule_id", "")
                txt = f"{w} · {r_id} 사건"
                add_item("지갑", cid, txt, f"/cases/{cid}")
                if len(out) >= 8:
                    break

        # 3. 거래 해시 검색 (tx_hash prefix)
        if len(out) < 8 and (s_lower.startswith("0x") or len(s) >= 8):
            txs = await (
                db.transfers.find(
                    {"tx_hash": {"$regex": escaped_prefix, "$options": "i"}},
                    {"_id": 0, "tx_hash": 1, "amount": 1},
                    max_time_ms=QUERY_MS
                )
                .sort("created_at", -1)
                .limit(4)
                .to_list()
            )
            for t in txs:
                tx_hash = t.get("tx_hash", "")
                amt = _number(t.get("amount")) or 0
                amt_str = f"{int(amt):,}" if amt == int(amt) else f"{amt:,.2f}"
                linked_case = await db.fds_cases.find_one(
                    {"tx_hashes": tx_hash},
                    {"_id": 1},
                    max_time_ms=QUERY_MS
                )
                if linked_case:
                    lc_id = str(linked_case["_id"])
                    txt = f"{tx_hash[:18]}… · {amt_str} USDT · {lc_id}"
                    to_url = f"/cases/{lc_id}"
                else:
                    txt = f"{tx_hash[:18]}… · {amt_str} USDT"
                    to_url = f"/transfers?q={tx_hash}"
                add_item("거래", tx_hash, txt, to_url)
                if len(out) >= 8:
                    break

    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")

    return out[:8]

