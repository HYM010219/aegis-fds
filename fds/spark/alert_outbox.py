"""Spark 경보 전송 대기함 (Mongo fds_alert_outbox). 게이트웨이 outbox_events와 같은 원리.

    경보를 Kafka로 보내기 전에 먼저 여기에 적어 두고, 보낸 뒤에 PUBLISHED로 바꾼다.
    stream.py 순서: 계산 → save() → Redis 처리 표시 → pending()을 Kafka로 → mark_published()

    - Kafka 전송이 실패해 배치가 다시 돌면, 이미 적어 둔 경보(PENDING)를 그대로 다시 보낸다.
      재분석 간격 때문에 같은 경보가 다시 계산되지 않아도 빠지지 않는다.
    - alert_id = "배치 번호:룰:지갑". Spark는 실패한 배치를 같은 배치 번호로 다시 돌리므로
      다시 계산해도 같은 번호가 나와 두 줄로 저장되지 않는다 (_id가 alert_id).
    - Kafka로 보낸 뒤 PUBLISHED로 바꾸기 전에 죽으면 같은 경보가 한 번 더 나간다.
      경보 소비자가 alert_id로 한 번만 반영한다 (app/alert_consumer.py).
    - 보낸 경보는 7일 뒤 자동 삭제. PENDING은 published_at이 없어서 지워지지 않는다.

Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone

from pymongo.errors import BulkWriteError

COLLECTION = "fds_alert_outbox"
KEEP_SECONDS = 7 * 86400   # 보낸 경보 보관 기간
PENDING_LIMIT = 5000       # 한 배치에 다시 보낼 최대 경보 수 (남으면 다음 배치)


def ensure_indexes(db):
    c = db[COLLECTION]
    c.create_index([("status", 1), ("created_at", 1)])
    c.create_index("published_at", expireAfterSeconds=KEEP_SECONDS)


def assign_ids(alerts, batch_id):
    """경보마다 alert_id를 붙인다. 한 배치에서 같은 룰·지갑이 두 번 나오면 뒤에 :1, :2를 붙인다."""
    count = defaultdict(int)
    for a in alerts:
        base = f"{batch_id}:{a['rule_id']}:{a['violator_address']}"
        n = count[base]
        count[base] += 1
        a["alert_id"] = base if n == 0 else f"{base}:{n}"


def save(db, alerts, batch_id):
    """경보를 PENDING으로 저장한다. 이미 있는 alert_id(같은 배치를 다시 돌린 경우)는 처음 저장한 것을 그대로 둔다.
    돌려주는 값: 새로 저장한 경보 수."""
    if not alerts:
        return 0
    assign_ids(alerts, batch_id)
    now = datetime.now(timezone.utc)
    docs = [{
        "_id": a["alert_id"],
        "key": a["violator_address"],   # Kafka key (같은 지갑 경보는 순서대로)
        "rule_id": a["rule_id"],
        "payload": json.dumps(a, ensure_ascii=False),
        "status": "PENDING",
        "batch_id": batch_id,
        "created_at": now,
        "published_at": None,
    } for a in alerts]
    try:
        return len(db[COLLECTION].insert_many(docs, ordered=False).inserted_ids)
    except BulkWriteError as e:
        errors = e.details.get("writeErrors") or []
        if e.details.get("writeConcernErrors") or any(err.get("code") != 11000 for err in errors):
            raise
        return e.details.get("nInserted", 0)


def pending(db):
    """아직 보내지 못한 경보 (이번 배치 + 지난번 실패분), 저장한 순서대로."""
    return list(db[COLLECTION].find({"status": "PENDING"}, {"key": 1, "payload": 1})
                .sort([("created_at", 1), ("_id", 1)]).limit(PENDING_LIMIT))


def mark_published(db, ids):
    if ids:
        db[COLLECTION].update_many({"_id": {"$in": ids}, "status": "PENDING"},
                                   {"$set": {"status": "PUBLISHED", "published_at": datetime.now(timezone.utc)}})
