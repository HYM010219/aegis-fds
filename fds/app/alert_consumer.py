import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import redis
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.blacklist import FROZEN_KEY, FROZEN_REASONS_KEY
from app.whitelist import whitelist_key

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "aegis")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "fds.alerts.v1")
ALERT_GROUP = os.getenv("ALERT_GROUP", "aegis-alert-consumer")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

BATCH_SIZE = 500                  # 한 번에 가져올 경보 수
RETRY_INTERVAL = 3                # 오류가 났을 때 쉬는 시간(초)
CASE_GAP = timedelta(minutes=10)  # 거래 시각 기준으로 이 시간 넘게 끊기면 새 사건

# 동결할 룰. 여기서 빼면 그 룰은 fds_cases에만 저장한다.
FREEZE_RULES = {"R01", "R02", "R03", "R04", "R06"}
# 위험도 임시값. 팀 기준이 정해지면 이 표만 고친다.
RISK_LEVELS = {"R01": "HIGH", "R02": "HIGH", "R03": "HIGH", "R04": "HIGH", "R06": "HIGH"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aegis.alert_consumer")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
cases = mongo_client[MONGO_DB].fds_cases
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True, socket_timeout=3)


def merge_evidence(old, new):
    """같은 사건의 증거를 합친다. 횟수는 최댓값, 목록은 합집합, 왕복 간격은 최솟값."""
    merged = dict(old)
    for name, value in new.items():
        current = merged.get(name)
        if current is None:
            merged[name] = value
        elif name == "round_trip_gap_sec":
            merged[name] = min(current, value)
        elif isinstance(value, list):
            merged[name] = sorted(set(current) | set(value))
        elif isinstance(value, (int, float)):
            merged[name] = max(current, value)
        else:
            merged[name] = value
    return merged


def save_case(alert, first_tx_at, last_tx_at, action):
    """같은 룰과 지갑의 이어지는 사건이 있으면 합치고, 없으면 새 케이스를 만든다."""
    rule_id = alert["rule_id"]
    wallet = alert["violator_address"]
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
        cases.update_one(
            {"_id": existing["_id"]},
            {
                "$set": {
                    "evidence": merge_evidence(existing["evidence"], alert["evidence"]),
                    "action_taken": action,
                    "updated_at": now,
                },
                "$min": {"first_tx_at": first_tx_at},
                "$max": {"last_tx_at": last_tx_at},
                "$addToSet": {"tx_hashes": {"$each": alert["tx_hashes"]}},
            },
        )
        return

    cases.insert_one({
        "case_dedup_key": f"{rule_id}:{wallet}:{int(first_tx_at.timestamp() * 1000)}",
        "rule_id": rule_id,
        "rule_name": alert["rule_name"],
        "risk_level": RISK_LEVELS[rule_id],
        "violator_address": wallet,
        "counterparty_address": alert.get("counterparty_address"),
        "tx_hashes": alert["tx_hashes"],
        "evidence": alert["evidence"],
        "action_taken": action,
        "first_tx_at": first_tx_at,
        "last_tx_at": last_tx_at,
        "detected_at": now,
        "updated_at": now,
    })
    logger.info(f"새 케이스 {rule_id} {wallet}")


def handle_alert(alert):
    rule_id = alert["rule_id"]
    wallet = alert["violator_address"]
    first_tx_at = datetime.fromisoformat(alert["first_tx_at"])
    last_tx_at = datetime.fromisoformat(alert["last_tx_at"])
    if rule_id not in RISK_LEVELS:
        raise ValueError(f"알 수 없는 룰: {rule_id}")

    # 이 룰에서 면제된 지갑은 저장도 동결도 하지 않는다
    if redis_client.sismember(whitelist_key(rule_id), wallet):
        return

    freeze = rule_id in FREEZE_RULES
    # 저장 먼저, 동결은 그다음. 어느 쪽이 실패해도 다시 처리하면 같은 결과가 된다.
    save_case(alert, first_tx_at, last_tx_at, "WALLET_FROZEN" if freeze else "CASE_ONLY")
    if freeze:
        # 사유와 동결을 함께 저장한다. 이미 사유가 있으면 처음 동결한 룰을 유지한다.
        pipe = redis_client.pipeline(transaction=True)
        pipe.hsetnx(FROZEN_REASONS_KEY, wallet, rule_id)
        pipe.sadd(FROZEN_KEY, wallet)
        _, newly_frozen = pipe.execute()
        if newly_frozen:
            logger.info(f"지갑 동결 {wallet} ({rule_id})")


def handle_message(msg):
    try:
        alert = json.loads(msg.value())
        # 형식 확인을 위해 필수 필드를 먼저 읽는다
        alert["evidence"], alert["tx_hashes"]
        while True:
            try:
                handle_alert(alert)
                return
            except (PyMongoError, RedisError) as e:
                # 저장소가 복구될 때까지 다음 경보로 넘어가지 않는다
                logger.error(f"처리 실패 {type(e).__name__}: {e}")
                time.sleep(RETRY_INTERVAL)
    except (ValueError, KeyError, TypeError) as e:
        # 고쳐지지 않는 메시지는 기록만 남기고 건너뛴다
        logger.error(f"잘못된 경보 건너뜀 {type(e).__name__}: {e}")


def main():
    cases.create_index([("rule_id", 1), ("violator_address", 1), ("last_tx_at", -1)])

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
