"""Spark 2단계: Kafka 토픽 3개를 10초마다 읽어 detector.py로 L1~L8을 계산하고 경보를 보낸다."""
import json
import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from detector import FREEZE_TOPIC, HOLD_TOPIC, PROFILE_NAME, TICK_TOPIC, TRANSFER_TOPIC, Detector  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "fds.alerts.v1")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/checkpoints/detector")
# 체크포인트가 없는 첫 시작에서만 쓰인다. latest면 시작 이후 들어온 메시지만 본다.
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")  # 밀렸을 때 한 배치에 읽을 최대 메시지 수

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aegis.stream")

detector = Detector(
    mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true"),
    mongo_db=os.getenv("MONGO_DB", "aegis"),
    redis_host=os.getenv("REDIS_HOST", "localhost"),
)

spark = (
    SparkSession.builder.appName("aegis-fds-detector")
    .config("spark.sql.shuffle.partitions", "4")  # 기본값 200은 컨테이너 1개에 너무 많다
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Kafka: 거래(계기 ②), 보류(계기 ①), 동결(계기 ③)을 한 쿼리로 읽는다
messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join([TRANSFER_TOPIC, HOLD_TOPIC, FREEZE_TOPIC]))
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
    .select(col("topic"), col("value").cast("string").alias("value"))
)

# 2) 시계 신호: 새 메시지가 없는 동안에도 대기 목록(재분석 간격, 계기 ③)을 처리하도록 1초에 1줄
ticks = (
    spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    .select(lit(TICK_TOPIC).alias("topic"), lit(None).cast("string").alias("value"))
)


def process_batch(df, batch_id):
    started = time.time()
    rows = [(row.topic, row.value) for row in df.collect()]
    alerts, stats = detector.process(rows)

    # 3) 경보를 fds.alerts.v1로 보낸다. key를 지갑 주소로 두면 같은 지갑 경보는 순서대로 들어간다.
    if alerts:
        out = spark.createDataFrame(
            [(a["violator_address"], json.dumps(a, ensure_ascii=False)) for a in alerts],
            "key string, value string",
        )
        (
            out.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", ALERT_TOPIC)
            .save()
        )

    stats["seconds"] = round(time.time() - started, 2)
    # 할 일이 없던 배치(시계 신호만 있는 경우)는 로그를 남기지 않는다. 배치 시간이 10초보다 충분히 짧은지 여기서 본다.
    if stats["analyzed"] or stats["l5_recheck"] or stats["alerts"] or stats["cooling"] or stats["recheck_queued"]:
        logger.info(f"batch={batch_id} {json.dumps(stats)}")


logger.info(f"detector 시작 profile={PROFILE_NAME} trigger={TRIGGER} topics={TRANSFER_TOPIC},{HOLD_TOPIC},{FREEZE_TOPIC}")
(
    messages.unionByName(ticks)
    .writeStream.queryName("detector")
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=TRIGGER)
    .start()
)
spark.streams.awaitAnyTermination()
