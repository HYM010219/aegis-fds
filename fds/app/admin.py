"""직원(관제 웹) API: 동결·해제, 케이스 처리, 블랙리스트·화이트리스트 관리, 지갑 상태 조회.

관제 웹의 백엔드 서버만 부른다 (브라우저에서 직접 부르지 않음). 인증은 x-admin-key 헤더 (ADMIN_API_KEY).
송금 API 키(GATEWAY_API_KEY)와는 다른 키를 쓴다. 주소는 전부 /api/v1/admin 으로 시작한다.

    케이스
    POST /cases/{case_id}/freeze          수동 동결: 케이스 지갑을 동결 (사유 = 케이스 룰, 예: FROZEN_L5)
    POST /cases/{case_id}/dismiss         이상 없음: 동결 없이 검토 대기 케이스만 닫기
    지갑
    POST /wallets/{wallet}/freeze         케이스 없이 직접 동결 (사유 FROZEN_MANUAL, MANUAL 케이스를 하나 만든다)
    POST /wallets/{wallet}/unfreeze       동결 해제: 해제 시각 이후 거래만 다시 분석 (해제 직후 재동결 방지)
    GET  /wallets/{wallet}                지갑 상태: 동결·사유·블랙리스트·화이트리스트·보류·해제 시각
    블랙리스트 (R07 차단, L5 기준)
    POST /blacklist                       여러 주소 한 번에 등록
    POST /blacklist/{wallet}/remove       등록 해제
    GET  /blacklist                       목록 (검색, 페이지)
    화이트리스트 (R01~R06 보류 면제, L = 2단계 분석 면제)
    POST /whitelist                       지갑 하나에 면제 룰 등록 (메모 필수)
    POST /whitelist/{wallet}/remove       고른 룰 면제만 해제
    GET  /whitelist                       목록 (검색, 룰, 페이지)

케이스 review_status
    PENDING 검토 대기 / FROZEN 직원이 동결 / DISMISSED 이상 없음 / UNFROZEN 동결 해제됨 / None 자동 동결(L1·L2·L3·L8)

처리 순서는 모두 ① Mongo(기록) → ② Redis(실제 검사에 쓰는 값). 게이트웨이·Spark는 Redis만 본다.
②에서 실패하면 503이 나가고, 같은 요청을 다시 보내면 남은 부분만 처리된다.
블랙리스트·화이트리스트 기록(fds_blacklist, fds_whitelist)은 지우지 않고 active=false로 남겨 이력이 된다.
"""
import json
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from typing import Annotated, Literal

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, StringConstraints
from pymongo.errors import BulkWriteError, PyMongoError
from redis.exceptions import RedisError

from app.holds import hold_key
from app.mongo_client import db, mongo_client
from app.redis_client import redis_client
from app.store import outbox_doc
from app.wallet_status import frozen_reason
from common.keys import (
    BLACKLIST_KEY,
    FREEZE_TOPIC,
    FROZEN_KEY,
    FROZEN_REASONS_KEY,
    UNFROZEN_AT_KEY,
    WHITELIST_RULES,
    whitelist_key,
)

logger = logging.getLogger("aegis.gateway")

# 케이스 상태
PENDING, FROZEN, DISMISSED, UNFROZEN = "PENDING", "FROZEN", "DISMISSED", "UNFROZEN"


# ---------- 오류 응답 ----------

class AdminError(Exception):
    def __init__(self, http_code, reason, **extra):
        self.http_code, self.reason, self.extra = http_code, reason, extra


async def _admin_error(request: Request, e: AdminError):
    return JSONResponse(status_code=e.http_code, content={"status": "ERROR", "reason": e.reason, **e.extra})


# ---------- 인증 ----------

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")


async def require_admin_key(x_admin_key: str | None = Header(default=None)):
    if not ADMIN_API_KEY:
        # 키를 설정하지 않으면 직원 API는 꺼진 상태 (송금 API는 그대로 동작)
        raise AdminError(503, "ADMIN_API_DISABLED")
    if x_admin_key is None or not secrets.compare_digest(x_admin_key.encode(), ADMIN_API_KEY.encode()):
        raise AdminError(401, "UNAUTHORIZED")


router = APIRouter(prefix="/api/v1/admin", dependencies=[Depends(require_admin_key)])


def install(app: FastAPI):
    """main.py에서 부른다."""
    app.include_router(router)
    app.add_exception_handler(AdminError, _admin_error)


# ---------- 요청 형식 ----------

Wallet = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
WhitelistRule = Literal[WHITELIST_RULES]


class Review(BaseModel):
    operator: str = Field(min_length=1, max_length=64)       # 처리한 직원 ID (웹 백엔드가 로그인 정보로 채움)
    memo: str | None = Field(default=None, max_length=1000)  # 판단 근거


class BlacklistAdd(Review):
    wallets: list[Wallet] = Field(min_length=1, max_length=1000)


class WhitelistAdd(Review):
    wallet: Wallet
    rules: list[WhitelistRule] = Field(min_length=1)
    memo: str = Field(min_length=1, max_length=1000)  # 보안을 약하게 하는 작업이라 사유 필수


class WhitelistRemove(Review):
    rules: list[WhitelistRule] = Field(min_length=1)


# ---------- 공통 ----------

def _now():
    return datetime.now(timezone.utc)


def _audit(action, **fields):
    logger.info(json.dumps({"time": _now().isoformat(timespec="milliseconds"), "admin": action, **fields},
                           ensure_ascii=False))


async def _find_case(case_id):
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


async def _transaction(callback):
    """Mongo 트랜잭션 하나로 실행하고 callback의 반환값을 돌려준다."""
    try:
        async with mongo_client.start_session() as session:
            return await session.with_transaction(callback)
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")


async def _redis(pipe):
    try:
        return await pipe.execute()
    except RedisError:
        raise AdminError(503, "FDS_UNAVAILABLE")


def _freeze_event(event, wallet, rule, operator, at):
    """fds.freezes.v1 이벤트. 자동 동결(alert_consumer.py)과 같은 형식이라 ClickHouse freezes에 그대로 쌓인다."""
    time_field = "frozen_at" if event == "FREEZE" else "unfrozen_at"
    return outbox_doc(FREEZE_TOPIC, wallet, {
        "event": event,
        "wallet": wallet,
        "rule": rule,
        "source": "MANUAL",
        "operator": operator,
        time_field: at.isoformat(),
    }, at)


# ---------- 수동 동결 ----------

@router.post("/cases/{case_id}/freeze")
async def freeze_case(case_id: str, body: Review):
    case = await _find_case(case_id)
    wallet, rule, status = case["violator_address"], case["rule_id"], case.get("review_status")
    now = _now()
    review = {
        "review_status": FROZEN,
        "action_taken": "WALLET_FROZEN",
        "reviewed_by": body.operator,
        "reviewed_at": now,
        "review_memo": body.memo,
        "updated_at": now,
    }

    pipe = redis_client.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, wallet)
    pipe.hget(FROZEN_REASONS_KEY, wallet)
    already, current_rule = await _redis(pipe)

    # 1) 다른 룰로 이미 동결된 지갑: 직원 판단만 기록 (사유와 이벤트는 처음 동결 그대로)
    if already:
        if status != FROZEN:
            try:
                await db.fds_cases.update_one({"_id": case["_id"]}, {"$set": review})
            except PyMongoError:
                raise AdminError(503, "FDS_UNAVAILABLE")
        _audit("FREEZE", case_id=case_id, wallet=wallet, rule=rule, operator=body.operator, result="ALREADY_FROZEN")
        return {"status": "OK", "result": "ALREADY_FROZEN", "wallet": wallet, "reason": frozen_reason(current_rule)}

    # 2) Mongo: 케이스 상태 + 동결 이벤트. 이미 FROZEN이면 앞선 요청이 여기까지 끝낸 것이라 건너뛴다.
    if status != FROZEN:
        async def callback(session):
            # 그사이 다른 직원이 처리했으면 바꾸지 않는다
            result = await db.fds_cases.update_one({"_id": case["_id"], "review_status": status},
                                                   {"$set": review}, session=session)
            if result.matched_count == 0:
                raise AdminError(409, "CONFLICT")
            await db.outbox_events.insert_one(_freeze_event("FREEZE", wallet, rule, body.operator, now),
                                              session=session)
        await _transaction(callback)

    # 3) Redis: 이 순간부터 게이트웨이가 403 FROZEN_{룰}로 막는다
    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(FROZEN_REASONS_KEY, wallet, rule)
    pipe.sadd(FROZEN_KEY, wallet)
    await _redis(pipe)

    _audit("FREEZE", case_id=case_id, wallet=wallet, rule=rule, operator=body.operator, result="FROZEN")
    return {"status": "OK", "result": "FROZEN", "wallet": wallet, "reason": frozen_reason(rule)}


# ---------- 이상 없음 ----------

@router.post("/cases/{case_id}/dismiss")
async def dismiss_case(case_id: str, body: Review):
    case = await _find_case(case_id)
    if case.get("review_status") != PENDING:
        # 자동 동결·직원 동결 케이스는 '동결 해제'로 처리한다
        raise AdminError(409, "NOT_PENDING", review_status=case.get("review_status"))
    now = _now()
    try:
        result = await db.fds_cases.update_one(
            {"_id": case["_id"], "review_status": PENDING},
            {"$set": {
                "review_status": DISMISSED,
                "action_taken": "NO_ACTION",
                "reviewed_by": body.operator,
                "reviewed_at": now,
                "review_memo": body.memo,
                "updated_at": now,
            }},
        )
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    if result.matched_count == 0:
        raise AdminError(409, "CONFLICT")

    _audit("DISMISS", case_id=case_id, wallet=case["violator_address"], rule=case["rule_id"], operator=body.operator)
    return {"status": "OK", "result": DISMISSED, "case_id": case_id}


# ---------- 동결 해제 ----------

@router.post("/wallets/{wallet}/unfreeze")
async def unfreeze_wallet(wallet: str, body: Review):
    pipe = redis_client.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, wallet)
    pipe.hget(FROZEN_REASONS_KEY, wallet)
    frozen, rule = await _redis(pipe)
    if not frozen:
        raise AdminError(409, "NOT_FROZEN")
    now = _now()

    # 1) Mongo: 이 지갑의 동결 케이스를 '해제됨'으로 + 해제 이벤트
    async def callback(session):
        result = await db.fds_cases.update_many(
            {"violator_address": wallet, "action_taken": "WALLET_FROZEN", "review_status": {"$ne": UNFROZEN}},
            {"$set": {
                "review_status": UNFROZEN,
                "unfrozen_by": body.operator,
                "unfrozen_at": now,
                "unfreeze_memo": body.memo,
                "updated_at": now,
            }},
            session=session,
        )
        await db.outbox_events.insert_one(_freeze_event("UNFREEZE", wallet, rule or "", body.operator, now),
                                          session=session)
        return result.modified_count
    cases_updated = await _transaction(callback)

    # 2) Redis: 해제 시각을 먼저 남기고(같은 트랜잭션) 동결 목록에서 뺀다.
    #    Spark는 해제 시각 이후 거래만 보므로, 해제 전 거래로 다시 동결되지 않는다.
    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(UNFROZEN_AT_KEY, wallet, int(now.timestamp() * 1000))
    pipe.srem(FROZEN_KEY, wallet)
    pipe.hdel(FROZEN_REASONS_KEY, wallet)
    await _redis(pipe)

    _audit("UNFREEZE", wallet=wallet, rule=rule, operator=body.operator, cases_updated=cases_updated)
    return {"status": "OK", "result": UNFROZEN, "wallet": wallet, "cases_updated": cases_updated}


# ---------- 케이스 없이 직접 동결 ----------

MANUAL_RULE = "MANUAL"  # 사유 FROZEN_MANUAL. 케이스 rule_id와 동결 이벤트 rule에 쓴다.


@router.post("/wallets/{wallet}/freeze")
async def freeze_wallet(wallet: str, body: Review):
    """신고·수사 요청처럼 케이스 없이 직원이 바로 동결. 기록을 남기려고 MANUAL 케이스를 하나 만든다
    (동결 해제하면 다른 동결 케이스처럼 UNFROZEN이 된다)."""
    pipe = redis_client.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, wallet)
    pipe.hget(FROZEN_REASONS_KEY, wallet)
    already, current_rule = await _redis(pipe)
    if already:
        return {"status": "OK", "result": "ALREADY_FROZEN", "wallet": wallet, "reason": frozen_reason(current_rule)}
    now = _now()

    # 1) Mongo: MANUAL 케이스 + 동결 이벤트. 앞선 요청이 여기까지 끝냈으면(동결 중인 MANUAL 케이스가 있으면) 건너뛴다.
    try:
        case = await db.fds_cases.find_one({"violator_address": wallet, "rule_id": MANUAL_RULE, "review_status": FROZEN})
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    if not case:
        case = {
            "case_dedup_key": f"{MANUAL_RULE}:{wallet}:{int(now.timestamp() * 1000)}",
            "rule_id": MANUAL_RULE,
            "rule_name": "MANUAL_FREEZE",
            "risk_level": "HIGH",
            "violator_address": wallet,
            "counterparty_address": None,
            "tx_hashes": [],
            "evidence": {},
            "action_taken": "WALLET_FROZEN",
            "review_status": FROZEN,
            "reviewed_by": body.operator,
            "reviewed_at": now,
            "review_memo": body.memo,
            "first_tx_at": None,
            "last_tx_at": None,
            "detected_at": now,
            "updated_at": now,
        }

        async def callback(session):
            await db.fds_cases.insert_one(case, session=session)
            await db.outbox_events.insert_one(_freeze_event("FREEZE", wallet, MANUAL_RULE, body.operator, now),
                                              session=session)
        await _transaction(callback)

    # 2) Redis: 이 순간부터 게이트웨이가 403 FROZEN_MANUAL로 막는다
    pipe = redis_client.pipeline(transaction=True)
    pipe.hset(FROZEN_REASONS_KEY, wallet, MANUAL_RULE)
    pipe.sadd(FROZEN_KEY, wallet)
    await _redis(pipe)

    _audit("FREEZE_WALLET", wallet=wallet, operator=body.operator, case_id=str(case["_id"]))
    return {"status": "OK", "result": "FROZEN", "wallet": wallet, "reason": frozen_reason(MANUAL_RULE),
            "case_id": str(case["_id"])}


# ---------- 지갑 상태 조회 ----------

@router.get("/wallets/{wallet}")
async def wallet_status(wallet: str):
    """관제 웹이 Redis에 직접 붙지 않고 동결 상태를 보는 용도."""
    pipe = redis_client.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, wallet)
    pipe.hget(FROZEN_REASONS_KEY, wallet)
    pipe.sismember(BLACKLIST_KEY, wallet)
    pipe.get(hold_key(wallet))
    pipe.ttl(hold_key(wallet))
    pipe.hget(UNFROZEN_AT_KEY, wallet)
    for rule in WHITELIST_RULES:
        pipe.sismember(whitelist_key(rule), wallet)
    frozen, rule, black, hold_rule, hold_ttl, unfrozen_ms, *white = await _redis(pipe)
    return {
        "wallet": wallet,
        "frozen": bool(frozen),
        "frozen_reason": frozen_reason(rule) if frozen else None,
        "blacklisted": bool(black),
        "whitelist_rules": [r for r, on in zip(WHITELIST_RULES, white) if on],
        "hold": {"rule": hold_rule, "seconds_left": max(hold_ttl, 0)} if hold_rule else None,
        "unfrozen_at": (datetime.fromtimestamp(int(unfrozen_ms) / 1000, timezone.utc).isoformat()
                        if unfrozen_ms else None),
    }


# ---------- 블랙리스트·화이트리스트 공통 ----------
# Mongo 문서 하나 = 등록 한 번. 해제하면 지우지 않고 active=false + removed_* 를 채운다.
# 같은 주소(화이트리스트는 주소+룰)의 active 문서는 하나만 있도록 고유 인덱스를 둔다 (store.ensure_indexes).

def _list_doc(wallet, body, now, **extra):
    return {
        "wallet": wallet,
        **extra,
        "active": True,
        "memo": body.memo,
        "added_by": body.operator,
        "added_at": now,
        "removed_by": None,
        "removed_at": None,
        "remove_memo": None,
    }


async def _insert_new(collection, docs):
    """문서를 넣고, 이미 등록돼 있어서 건너뛴 문서의 순번을 돌려준다."""
    try:
        await collection.insert_many(docs, ordered=False)
        return set()
    except BulkWriteError as e:
        errors = e.details.get("writeErrors", [])
        if any(err.get("code") != 11000 for err in errors):
            raise AdminError(503, "FDS_UNAVAILABLE")
        return {err["index"] for err in errors}
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")


async def _deactivate(collection, query, body, now):
    """등록 해제 기록. 해제한 문서 수를 돌려준다."""
    try:
        result = await collection.update_many({**query, "active": True}, {"$set": {
            "active": False,
            "removed_by": body.operator,
            "removed_at": now,
            "remove_memo": body.memo,
        }})
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    return result.modified_count


def _row(doc):
    out = {k: v for k, v in doc.items() if k != "_id"}
    for k, v in out.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
    return out


async def _page(collection, query, page, size):
    try:
        total = await collection.count_documents(query)
        docs = await collection.find(query).sort("added_at", -1).skip((page - 1) * size).limit(size).to_list()
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    return {"status": "OK", "total": total, "page": page, "size": size, "items": [_row(d) for d in docs]}


def _search(q, history):
    query = {} if history else {"active": True}
    if q:
        query["wallet"] = {"$regex": re.escape(q.strip()), "$options": "i"}
    return query


# ---------- 블랙리스트 ----------

@router.post("/blacklist")
async def add_blacklist(body: BlacklistAdd):
    """등록 즉시 R07로 송금이 막힌다. Spark는 다음 배치에서 새 주소를 찾아 상대 지갑 L5를 다시 계산한다 (계기 ③)."""
    wallets = list(dict.fromkeys(body.wallets))  # 같은 주소가 두 번 오면 하나만
    now = _now()
    skipped = await _insert_new(db.fds_blacklist, [_list_doc(w, body, now) for w in wallets])
    pipe = redis_client.pipeline(transaction=True)
    pipe.sadd(BLACKLIST_KEY, *wallets)
    await _redis(pipe)

    added = [w for i, w in enumerate(wallets) if i not in skipped]
    already = [w for i, w in enumerate(wallets) if i in skipped]
    _audit("BLACKLIST_ADD", wallets=added, already=len(already), operator=body.operator)
    return {"status": "OK", "added": added, "already": already}


@router.post("/blacklist/{wallet}/remove")
async def remove_blacklist(wallet: str, body: Review):
    """R07 차단을 푼다. 이 주소 때문에 만들어진 L5 케이스는 그대로 둔다."""
    pipe = redis_client.pipeline(transaction=False)
    pipe.sismember(BLACKLIST_KEY, wallet)
    (listed,) = await _redis(pipe)
    removed = await _deactivate(db.fds_blacklist, {"wallet": wallet}, body, _now())
    if not listed and not removed:
        raise AdminError(409, "NOT_LISTED")
    pipe = redis_client.pipeline(transaction=True)
    pipe.srem(BLACKLIST_KEY, wallet)
    await _redis(pipe)

    _audit("BLACKLIST_REMOVE", wallet=wallet, operator=body.operator)
    return {"status": "OK", "result": "REMOVED", "wallet": wallet}


@router.get("/blacklist")
async def list_blacklist(
    q: str | None = Query(default=None, max_length=128),  # 주소 일부로 검색
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
    history: bool = False,                                 # true면 해제된 기록까지
):
    return await _page(db.fds_blacklist, _search(q, history), page, size)


# ---------- 화이트리스트 ----------

@router.post("/whitelist")
async def add_whitelist(body: WhitelistAdd):
    """고른 룰만 면제. R01~R06은 게이트웨이 보류 면제, L은 Spark 2단계 분석 면제 (R07은 면제 없음)."""
    rules = list(dict.fromkeys(body.rules))
    now = _now()
    skipped = await _insert_new(db.fds_whitelist, [_list_doc(body.wallet, body, now, rule=r) for r in rules])
    pipe = redis_client.pipeline(transaction=True)
    for rule in rules:
        pipe.sadd(whitelist_key(rule), body.wallet)
    await _redis(pipe)

    added = [r for i, r in enumerate(rules) if i not in skipped]
    already = [r for i, r in enumerate(rules) if i in skipped]
    _audit("WHITELIST_ADD", wallet=body.wallet, rules=added, operator=body.operator)
    return {"status": "OK", "wallet": body.wallet, "added": added, "already": already}


@router.post("/whitelist/{wallet}/remove")
async def remove_whitelist(wallet: str, body: WhitelistRemove):
    rules = list(dict.fromkeys(body.rules))
    pipe = redis_client.pipeline(transaction=False)
    for rule in rules:
        pipe.sismember(whitelist_key(rule), wallet)
    listed = await _redis(pipe)
    now = _now()

    removed, not_listed = [], []
    for rule, in_redis in zip(rules, listed):
        changed = await _deactivate(db.fds_whitelist, {"wallet": wallet, "rule": rule}, body, now)
        (removed if in_redis or changed else not_listed).append(rule)
    if not removed:
        raise AdminError(409, "NOT_LISTED")
    pipe = redis_client.pipeline(transaction=True)
    for rule in removed:
        pipe.srem(whitelist_key(rule), wallet)
    await _redis(pipe)

    _audit("WHITELIST_REMOVE", wallet=wallet, rules=removed, operator=body.operator)
    return {"status": "OK", "wallet": wallet, "removed": removed, "not_listed": not_listed}


@router.get("/whitelist")
async def list_whitelist(
    q: str | None = Query(default=None, max_length=128),
    rule: WhitelistRule | None = None,                     # 룰 하나만 보기
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
    history: bool = False,
):
    query = _search(q, history)
    if rule:
        query["rule"] = rule
    return await _page(db.fds_whitelist, query, page, size)
