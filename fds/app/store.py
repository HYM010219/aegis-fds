import logging
from datetime import datetime, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from bson.int64 import Int64
from pymongo.errors import PyMongoError

from app.mongo_client import db, mongo_client

logger = logging.getLogger("aegis.gateway")

TOPIC = "transfer.events.v1"
WHALE_LIMIT = Decimal("50000")  # R05 차단 기준 (USDT)

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


async def ensure_indexes():
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

    # Kafka 전송 대기함
    await db.outbox_events.create_index([("publish_state", 1), ("created_at", 1)])
    # 발행 후 24시간 뒤 삭제. published_at이 없는 PENDING 건은 삭제되지 않는다.
    await db.outbox_events.create_index("published_at", expireAfterSeconds=86400)

    # 탐지 결과
    await db.fds_cases.create_index("case_dedup_key", unique=True)
    await db.fds_cases.create_index([("violator_address", 1), ("detected_at", -1)])
    await db.fds_cases.create_index([("rule_id", 1), ("detected_at", -1)])


def make_record(req, http_code, body):
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
        "created_at": datetime.now(timezone.utc),
    }


async def execute_transfer(req, body):
    """원천 저장과 outbox 저장을 트랜잭션 하나로 처리한다."""

    async def callback(session):
        # 1) 원천 데이터
        record = make_record(req, 200, body)
        await db.transfers.insert_one(record, session=session)

        # 2) Kafka로 보낼 이벤트 (인입 규격 필드 그대로)
        await db.outbox_events.insert_one({
            "tx_hash": req.tx_hash,
            "log_index": req.log_index,
            "topic": TOPIC,
            "partition_key": req.from_address,
            "payload": req.model_dump(mode="json", by_alias=True),
            "publish_state": "PENDING",
            "created_at": record["created_at"],
            "published_at": None,
        }, session=session)

    async with mongo_client.start_session() as session:
        await session.with_transaction(callback)


async def save_rejected(req, http_code, body):
    try:
        await db.transfers.insert_one(make_record(req, http_code, body))
    except PyMongoError as e:
        logger.error(f"차단 기록 저장 실패 {type(e).__name__} tx_hash={req.tx_hash} log_index={req.log_index}")
