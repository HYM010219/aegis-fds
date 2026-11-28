import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from bson.int64 import Int64
from pymongo.errors import PyMongoError

from app.holds import HOLD_MS
from app.mongo_client import db, mongo_client
from common.keys import HOLD_TOPIC, REJECTED_TOPIC, TRANSFER_TOPIC

logger = logging.getLogger("aegis.gateway")

HOLDS_KEEP_DAYS = 14            # fds_holds 보관 기간 (L1 2주 계산용)
# transfers 자동 삭제 기간(일). 0이면 지우지 않는다. ClickHouse를 연결한 뒤에 14로 켠다.
TRANSFERS_TTL_DAYS = int(os.getenv("TRANSFERS_TTL_DAYS", "0"))
TRANSFERS_TTL_INDEX = "created_at_1"


def parse_amount(value):
    """0보다 크고 소수점 6자리 이하인 금액만 Decimal로 돌려준다. 아니면 None."""
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount <= 0:
            return None
        rounded = amount.quantize(Decimal("0.000001"))
    except ArithmeticError:
        return None
    return rounded if rounded == amount else None


async def ensure_transfers_ttl():
    """TRANSFERS_TTL_DAYS에 맞춰 transfers 자동 삭제를 켜거나, 기간을 바꾸거나, 끈다."""
    current = (await db.transfers.index_information()).get(TRANSFERS_TTL_INDEX)
    current_seconds = current.get("expireAfterSeconds") if current else None

    if TRANSFERS_TTL_DAYS <= 0:
        if current_seconds is not None:
            await db.transfers.drop_index(TRANSFERS_TTL_INDEX)
            logger.info('{"event": "transfers_ttl", "state": "off"}')
        return

    seconds = TRANSFERS_TTL_DAYS * 86400
    if current is None:
        await db.transfers.create_index("created_at", expireAfterSeconds=seconds)
    elif current_seconds != seconds:
        # 이미 있는 TTL 인덱스는 다시 만들지 않고 기간만 바꾼다
        await db.command("collMod", "transfers",
                         index={"keyPattern": {"created_at": 1}, "expireAfterSeconds": seconds})
    else:
        return
    logger.info(f'{{"event": "transfers_ttl", "state": "on", "days": {TRANSFERS_TTL_DAYS}}}')


async def ensure_indexes():
    holds_seconds = HOLDS_KEEP_DAYS * 86400

    # 원천 데이터
    await db.transfers.create_index("tx_hash")
    await db.transfers.create_index([("from", 1), ("timestamp", -1)])
    await db.transfers.create_index([("to", 1), ("timestamp", -1)])
    await db.transfers.create_index([("status", 1), ("created_at", -1)])
    # 성공 건만 같은 tx_hash + log_index를 금지 (409 기록은 같은 값으로 저장돼야 하므로)
    await db.transfers.create_index(
        [("tx_hash", 1), ("log_index", 1), ("status", 1)],
        unique=True,
        partialFilterExpression={"status": "ACCEPTED"},
    )
    # 오래된 원본 자동 삭제 (설정으로 켬. 장기 보관은 ClickHouse)
    await ensure_transfers_ttl()

    # 보류 기록: 새로 걸린 보류만 한 줄씩. L1(2주 보류 횟수) 계산용
    await db.fds_holds.create_index([("wallet", 1), ("held_at", -1)])
    await db.fds_holds.create_index("held_at", expireAfterSeconds=holds_seconds)

    # Kafka 전송 대기함
    await db.outbox_events.create_index([("publish_state", 1), ("created_at", 1)])
    # 발행 후 24시간 뒤 삭제. published_at이 없는 PENDING 건은 삭제되지 않는다.
    await db.outbox_events.create_index("published_at", expireAfterSeconds=86400)

    # 탐지 결과
    await db.fds_cases.create_index("case_dedup_key", unique=True)
    await db.fds_cases.create_index([("violator_address", 1), ("detected_at", -1)])
    await db.fds_cases.create_index([("rule_id", 1), ("detected_at", -1)])
    await db.fds_cases.create_index("alert_ids")  # Spark 재발행 경보 중복 확인 (alert_consumer.py)

    # 감사 원장: 동결·해제·이상 없음·목록 변경을 한 줄씩 쌓기만 한다 (자동 동결은 경보 소비자가 같은 형식으로)
    await db.policy_decisions.create_index("decision_id", unique=True)
    await db.policy_decisions.create_index([("wallet", 1), ("created_at", -1)])
    await db.policy_decisions.create_index([("case_id", 1), ("created_at", -1)])
    await db.policy_decisions.create_index([("action", 1), ("created_at", -1)])

    # 블랙리스트·화이트리스트 기록 (직원 API). 해제해도 지우지 않으므로 active인 것만 고유하게.
    await db.fds_blacklist.create_index("wallet", unique=True, partialFilterExpression={"active": True},
                                        name="active_wallet")
    await db.fds_blacklist.create_index([("active", 1), ("added_at", -1)])
    await db.fds_whitelist.create_index([("wallet", 1), ("rule", 1)], unique=True,
                                        partialFilterExpression={"active": True}, name="active_wallet_rule")
    await db.fds_whitelist.create_index([("active", 1), ("rule", 1), ("added_at", -1)])


def make_record(req, http_code, body, now):
    amount = parse_amount(req.amount)
    return {
        "tx_hash": req.tx_hash,
        "log_index": req.log_index,
        "from": req.from_address,
        "to": req.to_address,
        # 잘못된 금액은 거절 기록이므로 받은 값을 그대로 남긴다
        "amount": Decimal128(amount) if amount is not None else req.amount,
        "block": Int64(req.block),
        "timestamp": req.timestamp,
        "http_code": http_code,
        "status": body["status"],
        "reason": body["reason"],
        "created_at": now,
        "exchange_name": getattr(req, "exchange_name", None),
    }


def make_outbox(req, topic, key, payload, now):
    return outbox_doc(topic, key, payload, now, req.tx_hash, req.log_index)


def outbox_doc(topic, key, payload, now, tx_hash=None, log_index=None):
    """Kafka 전송 대기함 문서. 거래가 없는 이벤트(직원 동결·해제)는 tx_hash 없이 만든다."""
    return {
        "tx_hash": tx_hash,
        "log_index": log_index,
        "topic": topic,
        "partition_key": key,
        "payload": payload,
        "publish_state": "PENDING",
        "created_at": now,
        "published_at": None,
    }


def tx_payload(req):
    """Kafka로 보낼 거래 필드 (인입 규격 그대로)."""
    return req.model_dump(mode="json", by_alias=True)


def hold_docs(req, hold, now):
    """보류 하나를 fds_holds 문서와 fds.holds.v1 이벤트로 만든다."""
    until = now + timedelta(milliseconds=HOLD_MS)
    record = {
        "wallet": hold.wallet,
        "rule": hold.rule,
        "rules": hold.rules,
        "side": hold.side,
        "tx_hash": req.tx_hash,
        "log_index": req.log_index,
        "from": req.from_address,
        "to": req.to_address,
        "amount": Decimal128(parse_amount(req.amount)),
        "tx_timestamp": req.timestamp,
        "evidence": hold.evidence,
        "held_at": now,
        "hold_until": until,
    }
    event = make_outbox(req, HOLD_TOPIC, hold.wallet, {
        "event": "HOLD",
        "wallet": hold.wallet,
        "rule": hold.rule,
        "rules": hold.rules,
        "side": hold.side,
        "held_at": now.isoformat(),
        "hold_until": until.isoformat(),
        "evidence": hold.evidence,
        **tx_payload(req),
    }, now)
    return record, event


def rejected_event(req, http_code, body, now):
    """거절 기록과 같은 내용을 transfer.rejected.v1로 보낸다 (transfers TTL로 지워져도 ClickHouse에 남게)."""
    return make_outbox(req, REJECTED_TOPIC, req.from_address, {
        **tx_payload(req),
        "http_code": http_code,
        "status": body["status"],
        "reason": body["reason"],
        "created_at": now.isoformat(),
    }, now)


def block_event(req, reason, now):
    """R07 차단 이벤트. 블랙리스트에 보내려 한 지갑을 Spark가 분석하게 한다."""
    return make_outbox(req, HOLD_TOPIC, req.from_address, {
        "event": "BLOCK",
        "wallet": req.from_address,
        "rule": "R07",
        "reason": reason,
        **tx_payload(req),
    }, now)


async def execute_transfer(req, body, receiver_hold=None, sender_hold=None):
    """원천 저장과 outbox 저장을 트랜잭션 하나로 처리한다. 보류도 같이 기록한다."""
    now = datetime.now(timezone.utc)

    async def callback(session):
        # 1) 원천 데이터
        await db.transfers.insert_one(make_record(req, 200, body, now), session=session)
        # 2) Kafka로 보낼 거래 이벤트
        await db.outbox_events.insert_one(
            make_outbox(req, TRANSFER_TOPIC, req.from_address, tx_payload(req), now), session=session
        )
        # 3) R01 5번째: 거래는 통과, 보내는 지갑 보류 기록
        if sender_hold:
            record, event = hold_docs(req, sender_hold, now)
            await db.fds_holds.insert_one(record, session=session)
            await db.outbox_events.insert_one(event, session=session)
        # 4) R03, R06: 거래는 통과, 받는 지갑 보류 기록
        if receiver_hold:
            record, event = hold_docs(req, receiver_hold, now)
            await db.fds_holds.insert_one(record, session=session)
            await db.outbox_events.insert_one(event, session=session)

    async with mongo_client.start_session() as session:
        await session.with_transaction(callback)


async def save_sender_hold(req, http_code, body, hold):
    """보내는 지갑 보류: 거절 기록, 보류 기록, 보류 이벤트를 트랜잭션 하나로 저장한다."""
    now = datetime.now(timezone.utc)

    async def callback(session):
        await db.transfers.insert_one(make_record(req, http_code, body, now), session=session)
        record, event = hold_docs(req, hold, now)
        await db.fds_holds.insert_one(record, session=session)
        await db.outbox_events.insert_many(
            [rejected_event(req, http_code, body, now), event], session=session
        )

    try:
        async with mongo_client.start_session() as session:
            await session.with_transaction(callback)
    except PyMongoError as e:
        # 보류 자체는 Redis에 이미 걸려 있다. 기록만 실패한 것이므로 로그를 남긴다.
        logger.error(f"보류 기록 저장 실패 {type(e).__name__} wallet={hold.wallet} rule={hold.rule} tx_hash={req.tx_hash}")


async def save_rejected(req, http_code, body, block_reason=None):
    """거절 기록 + transfer.rejected.v1 이벤트를 트랜잭션 하나로 저장한다. R07 차단이면 차단 이벤트도 함께."""
    now = datetime.now(timezone.utc)
    events = [rejected_event(req, http_code, body, now)]
    if block_reason:
        events.append(block_event(req, block_reason, now))

    async def callback(session):
        await db.transfers.insert_one(make_record(req, http_code, body, now), session=session)
        await db.outbox_events.insert_many(events, session=session)

    try:
        async with mongo_client.start_session() as session:
            await session.with_transaction(callback)
    except PyMongoError as e:
        logger.error(f"차단 기록 저장 실패 {type(e).__name__} tx_hash={req.tx_hash} log_index={req.log_index}")
