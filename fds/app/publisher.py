import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "aegis")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

BATCH_SIZE = 500      # 한 번에 가져올 문서 수
POLL_INTERVAL = 0.2   # 보낼 게 없을 때 쉬는 시간(초)
RETRY_INTERVAL = 3    # 오류가 났을 때 쉬는 시간(초)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aegis.publisher")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
outbox = mongo_client[MONGO_DB].outbox_events

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",                 # Kafka가 안전하게 저장한 뒤에 ACK
    "enable.idempotence": True,    # 재시도해도 Kafka 안에서 중복과 순서 꼬임 방지
    "delivery.timeout.ms": 10000,  # 10초 안에 못 보내면 실패로 처리
})


def publish_batch():
    docs = list(
        outbox.find({"publish_state": "PENDING"})
        .sort("created_at", 1)
        .limit(BATCH_SIZE)
    )

    acked_ids = []
    errors = []

    for doc in docs:
        def on_delivery(err, msg, doc_id=doc["_id"]):
            if err:
                errors.append(err)
            else:
                acked_ids.append(doc_id)

        producer.produce(
            doc["topic"],
            key=doc["partition_key"],
            value=json.dumps(doc["payload"], ensure_ascii=False, default=str),
            on_delivery=on_delivery,
        )

    # 보낸 메시지의 ACK 또는 실패 결과가 올 때까지 기다린다
    producer.flush(15)

    if errors:
        logger.error(f"발행 실패 {len(errors)}건: {errors[0]}")

    # ACK를 받은 문서만 PUBLISHED로 변경
    if acked_ids:
        outbox.update_many(
            {"_id": {"$in": acked_ids}},
            {"$set": {
                "publish_state": "PUBLISHED",
                "published_at": datetime.now(timezone.utc),
            }},
        )
        logger.info(f"발행 완료 {len(acked_ids)}건")

    return len(acked_ids)


def main():
    logger.info(f"publisher 시작 kafka={KAFKA_BOOTSTRAP}")
    while True:
        try:
            sent = publish_batch()
        except (PyMongoError, KafkaException, BufferError) as e:
            logger.error(f"처리 실패 {type(e).__name__}: {e}")
            time.sleep(RETRY_INTERVAL)
            continue

        if sent == 0:
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()