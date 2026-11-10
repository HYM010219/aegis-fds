import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import redis
from confluent_kafka import Consumer, KafkaException, Producer
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.blacklist import FROZEN_KEY, FROZEN_REASONS_KEY, LONG_RULES
from app.whitelist import whitelist_key

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "aegis")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "fds.alerts.v1")
FREEZE_TOPIC = os.getenv("FREEZE_TOPIC", "fds.freezes.v1")
ALERT_GROUP = os.getenv("ALERT_GROUP", "aegis-alert-consumer")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

BATCH_SIZE = 500                  # 한 번에 가져올 경보 수
RETRY_INTERVAL = 3                # 오류가 났을 때 쉬는 시간(초)
CASE_GAP = timedelta(minutes=10)  # 거래 시각 기준으로 이 시간 넘게 끊기면 새 사건

# 자동 동결할 룰. 나머지(L4~L7)는 케이스만 만들고 관제 직원이 검토 후 동결한다.
AUTO_FREEZE_RULES = {"L1", "L2", "L3", "L8"}
# 위험도 기본값. L5는 Spark가 보낸 등급(CRITICAL, HIGH, MEDIUM)을 쓴다.
RISK_LEVELS = {
    "L1": "HIGH", "L2": "HIGH", "L3": "HIGH", "L8": "HIGH",
    "L4": "MEDIUM", "L5": "MEDIUM", "L6": "MEDIUM", "L7": "MEDIUM",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aegis.alert_consumer")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
cases = mongo_client[MONGO_DB].fds_cases
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True, socket_timeout=3)
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "delivery.timeout.ms": 10000,
})


def merge_evidence(old, new):
    """같은 사건의 증거를 합친다. 숫자는 최댓값, 목록은 합집합, 나머지는 새 값."""
    merged = dict(old)
    for name, value in new.items():
        current = merged.get(name)
        if current is None:
            merged[name] = value
        elif (isinstance(value, list) and isinstance(current, list)
              and all(isinstance(v, (str, int, float)) for v in value + current)):
            merged[name] = sorted(set(current) | set(value))
        elif isinstance(value, (int, float)) and isinstance(current, (int, float)):
            merged[name] = max(current, value)
        else:
            merged[name] = value
    return merged


def save_case(alert, first_tx_at, last_tx_at, auto_freeze):
    """같은 룰과 지갑의 이어지는 사건이 있으면 합치고, 없으면 새 케이스를 만든다."""
    rule_id = alert["rule_id"]
    wallet = alert["violator_address"]
    risk_level = alert.get("risk_level") or RISK_LEVELS[rule_id]
    now = datetime.now(timezone.utc)

    existing = cases.find_one(
        {
            "rule_id": rule_id,
            "violator_address": wallet,
            "first_tx_at": {"$lte": last_tx_at + CASE_GAP},
            "last_tx_at": {"$gte": first_tx_at - CASE_GAP},
        },
        sort=[("last_tx_at", -1)],
    )

    if existing:
        update = {
            "$set": {
                "evidence": merge_evidence(existing["evidence"], alert["evidence"]),
                "risk_level": risk_level,
                "updated_at": now,
            },
            "$min": {"first_tx_at": first_tx_at},
            "$max": {"last_tx_at": last_tx_at},
            "$addToSet": {"tx_hashes": {"$each": alert["tx_hashes"]}},
        }
        cases.update_one({"_id": existing["_id"]}, update)
        return

    cases.insert_one({
        "case_dedup_key": f"{rule_id}:{wallet}:{int(first_tx_at.timestamp() * 1000)}",
        "rule_id": rule_id,
        "rule_name": alert["rule_name"],
        "risk_level": risk_level,
        "violator_address": wallet,
        "counterparty_address": alert.get("counterparty_address"),
        "tx_hashes": alert["tx_hashes"],
        "evidence": alert["evidence"],
        # 자동 동결이면 WALLET_FROZEN, 아니면 관제 직원 검토 대기
        "action_taken": "WALLET_FROZEN" if auto_freeze else "REVIEW_PENDING",
        "review_status": None if auto_freeze else "PENDING",
        "first_tx_at": first_tx_at,
        "last_tx_at": last_tx_at,
        "detected_at": now,
        "updated_at": now,
    })
    logger.info(f"새 케이스 {rule_id} {wallet} {'자동 동결' if auto_freeze else '검토 대기'}")


def send_freeze_event(wallet, rule_id):
    """새로 동결된 지갑을 Spark에 알린다 (계기 ③: 상대 지갑 L5 재계산)."""
    errors = []
    producer.produce(
        FREEZE_TOPIC,
        key=wallet,
        value=json.dumps({
            "event": "FREEZE",
            "wallet": wallet,
            "rule": rule_id,
            "source": "AUTO",
            "frozen_at": datetime.now(timezone.utc).isoformat(),
        }),
        on_delivery=lambda err, msg: err and errors.append(err),
    )
    producer.flush(15)
    if errors or len(producer) > 0:
        raise KafkaException(errors[0] if errors else "동결 이벤트 전송 시간 초과")


def freeze_wallet(wallet, rule_id):
    """동결 이벤트를 먼저 보내고 Redis에 동결한다. 다시 처리해도 이벤트가 빠지지 않는다."""
    if redis_client.sismember(FROZEN_KEY, wallet):
        return
    send_freeze_event(wallet, rule_id)
    # 사유와 동결을 함께 저장한다. 이미 사유가 있으면 처음 동결한 룰을 유지한다.
    pipe = redis_client.pipeline(transaction=True)
    pipe.hsetnx(FROZEN_REASONS_KEY, wallet, rule_id)
    pipe.sadd(FROZEN_KEY, wallet)
    pipe.execute()
    logger.info(f"지갑 동결 {wallet} ({rule_id})")


def handle_alert(alert):
    rule_id = alert["rule_id"]
    wallet = alert["violator_address"]
    first_tx_at = datetime.fromisoformat(alert["first_tx_at"])
    last_tx_at = datetime.fromisoformat(alert["last_tx_at"])
    if rule_id not in LONG_RULES:
        raise ValueError(f"알 수 없는 룰: {rule_id}")

    # 2단계 분석에서 면제된 지갑은 저장도 동결도 하지 않는다
    if redis_client.sismember(whitelist_key("L"), wallet):
        return

    auto_freeze = rule_id in AUTO_FREEZE_RULES
    # 저장 먼저, 동결은 그다음. 어느 쪽이 실패해도 다시 처리하면 같은 결과가 된다.
    save_case(alert, first_tx_at, last_tx_at, auto_freeze)
    if auto_freeze:
        freeze_wallet(wallet, rule_id)


def handle_message(msg):
    try:
        alert = json.loads(msg.value())
        # 형식 확인을 위해 필수 필드를 먼저 읽는다
        alert["evidence"], alert["tx_hashes"]
        while True:
            try:
                handle_alert(alert)
                return
            except (PyMongoError, RedisError, KafkaException) as e:
                # 저장소가 복구될 때까지 다음 경보로 넘어가지 않는다
                logger.error(f"처리 실패 {type(e).__name__}: {e}")
                time.sleep(RETRY_INTERVAL)
    except (ValueError, KeyError, TypeError) as e:
        # 고쳐지지 않는 메시지는 기록만 남기고 건너뛴다
        logger.error(f"잘못된 경보 건너뜀 {type(e).__name__}: {e}")


def main():
    cases.create_index([("rule_id", 1), ("violator_address", 1), ("last_tx_at", -1)])
    # 관제 화면의 검토 대기 목록용
    cases.create_index([("review_status", 1), ("risk_level", 1), ("detected_at", -1)])

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": ALERT_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # 처리가 끝난 뒤에 직접 읽은 위치를 저장
    })
    consumer.subscribe([ALERT_TOPIC])
    logger.info(f"alert consumer 시작 kafka={KAFKA_BOOTSTRAP} topic={ALERT_TOPIC}")

    while True:
        messages = consumer.consume(BATCH_SIZE, timeout=1)
        handled = False
        for msg in messages:
            if msg.error():
                logger.error(f"Kafka 오류: {msg.error()}")
                continue
            handle_message(msg)
            handled = True

        if handled:
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as e:
                # 위치 저장에 실패하면 다시 읽게 되지만 같은 결과로 합쳐진다
                logger.error(f"읽은 위치 저장 실패: {e}")


if __name__ == "__main__":
    main()
