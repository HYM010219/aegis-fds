import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import redis
from confluent_kafka import Consumer, KafkaException, Producer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError
from redis.exceptions import RedisError

from app.wallet_status import LONG_RULES, RISK_LEVELS
from common.keys import ALERT_TOPIC, FREEZE_TOPIC, FROZEN_KEY, FROZEN_REASONS_KEY, UNFROZEN_AT_KEY, whitelist_key

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "aegis")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ALERT_GROUP = os.getenv("ALERT_GROUP", "aegis-alert-consumer")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

BATCH_SIZE = 500                  # 한 번에 가져올 경보 수
RETRY_INTERVAL = 3                # 오류가 났을 때 쉬는 시간(초)
CASE_GAP = timedelta(minutes=10)  # 거래 시각 기준으로 이 시간 넘게 끊기면 새 사건

# 자동 동결할 룰. 나머지(L4~L7)는 케이스만 만들고 관제 직원이 검토 후 동결한다.
# 실험 스위치 L1_ACTION: review 이면 L1을 자동 동결하지 않고 검토 대기로 (기본 freeze = 지금 동작)
L1_ACTION = os.getenv("L1_ACTION", "freeze").lower()
AUTO_FREEZE_RULES = {"L2", "L3", "L8"} if L1_ACTION == "review" else {"L1", "L2", "L3", "L8"}
# 위험도 기본값 RISK_LEVELS는 app/wallet_status.py (관제 통계도 같이 씀)
# 등급 순서. 케이스를 합칠 때 더 높은 등급을 유지한다.
RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def higher_risk(a, b):
    """두 등급 중 높은 쪽 (모르는 값이나 None은 가장 낮게 본다)."""
    return max(a, b, key=lambda level: RISK_ORDER.get(level, -1))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aegis.alert_consumer")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
cases = mongo_client[MONGO_DB].fds_cases
decisions = mongo_client[MONGO_DB].policy_decisions  # 감사 원장 (쌓기만 함)
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
    """같은 룰과 지갑의 이어지는 사건이 있으면 합치고, 없으면 새 케이스를 만든다. 케이스 _id를 돌려준다."""
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
            # 동결 해제된 케이스는 끝난 사건이다. 해제 뒤 새로 잡히면 새 케이스로 만든다.
            "review_status": {"$ne": "UNFROZEN"},
        },
        sort=[("last_tx_at", -1)],
    )

    # 계산 출처: 어느 엔진·기간 설정·배치에서 언제 계산했는지 (사건 상세·STR 초안이 같이 보여줌)
    source = {
        "engine": "spark_engine_v1",
        "period_profile": alert.get("period_profile"),
        "batch_id": alert.get("batch_id"),
        "alerted_at": alert.get("alerted_at"),
    }

    if existing:
        update = {
            "$set": {
                "evidence": merge_evidence(existing["evidence"], alert["evidence"]),
                "criteria": alert.get("criteria") or existing.get("criteria"),
                "source": source,  # 합친 뒤에는 마지막으로 계산한 배치
                # 마지막 경보로 덮어쓰지 않고 더 높은 등급을 유지 (CRITICAL 뒤에 MEDIUM이 와도 CRITICAL)
                "risk_level": higher_risk(existing.get("risk_level"), risk_level),
                "updated_at": now,
            },
            "$min": {"first_tx_at": first_tx_at},
            "$max": {"last_tx_at": last_tx_at},
            "$addToSet": {"tx_hashes": {"$each": alert["tx_hashes"]}},
        }
        cases.update_one({"_id": existing["_id"]}, update)
        return existing["_id"]

    result = cases.insert_one({
        "case_dedup_key": f"{rule_id}:{wallet}:{int(first_tx_at.timestamp() * 1000)}",
        "rule_id": rule_id,
        "rule_name": alert["rule_name"],
        "risk_level": risk_level,
        "violator_address": wallet,
        "counterparty_address": alert.get("counterparty_address"),
        "tx_hashes": alert["tx_hashes"],
        "evidence": alert["evidence"],       # 실측값 (Spark 계산)
        "criteria": alert.get("criteria"),   # 판정 기준값 (spark/config.py)
        "source": source,
        # 자동 동결이면 WALLET_FROZEN, 아니면 관제 직원 검토 대기
        "action_taken": "WALLET_FROZEN" if auto_freeze else "REVIEW_PENDING",
        "review_status": None if auto_freeze else "PENDING",
        "first_tx_at": first_tx_at,
        "last_tx_at": last_tx_at,
        "detected_at": now,
        "updated_at": now,
    })
    logger.info(f"새 케이스 {rule_id} {wallet} {'자동 동결' if auto_freeze else '검토 대기'}")
    return result.inserted_id


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


def record_decision(wallet, rule_id, rule_name, case_id):
    """감사 원장에 자동 동결 한 줄 (actor SYSTEM). 직원 조치는 admin.py가 같은 형식으로 남긴다.
    decision_id를 케이스로 정해 두어, 다시 처리해도 한 줄만 남는다."""
    try:
        decisions.insert_one({
            "decision_id": f"SYSTEM:FREEZE:{case_id}",
            "case_id": str(case_id),
            "wallet": wallet,
            "action": "FREEZE",
            "actor_type": "SYSTEM",
            "actor_id": "spark_engine_v1",
            "reason_code": f"FROZEN_{rule_id}",
            "rationale": f"{rule_id} {rule_name} 자동 동결",
            "created_at": datetime.now(timezone.utc),
        })
    except DuplicateKeyError:
        pass


def freeze_wallet(wallet, rule_id, rule_name, case_id):
    """동결 이벤트 → 감사 원장 → Redis 동결 순서. 중간에 실패해 다시 처리해도 빠지는 기록이 없다."""
    if redis_client.sismember(FROZEN_KEY, wallet):
        return
    send_freeze_event(wallet, rule_id)
    record_decision(wallet, rule_id, rule_name, case_id)
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
    # 직원이 동결 해제한 지갑: 해제 전 거래가 들어간 경보(해제 직전에 만들어져 늦게 온 것)는 버린다
    unfrozen_ms = redis_client.hget(UNFROZEN_AT_KEY, wallet)
    if unfrozen_ms and first_tx_at.timestamp() * 1000 < int(unfrozen_ms):
        logger.info(f"해제 전 거래 경보 건너뜀 {rule_id} {wallet}")
        return

    auto_freeze = rule_id in AUTO_FREEZE_RULES
    # 저장 먼저, 동결은 그다음. 어느 쪽이 실패해도 다시 처리하면 같은 결과가 된다.
    case_id = save_case(alert, first_tx_at, last_tx_at, auto_freeze)
    if auto_freeze:
        freeze_wallet(wallet, rule_id, alert["rule_name"], case_id)


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
    # 감사 원장: 같은 자동 동결이 두 번 들어가지 않게 (게이트웨이도 같은 인덱스를 만든다)
    decisions.create_index("decision_id", unique=True)

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
