"""Spark 2단계: Kafka 토픽 3개를 10초마다 읽어 detector.py로 L1~L8을 계산하고 경보를 보낸다."""
import json
import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)                   # detector.py
sys.path.insert(0, os.path.dirname(HERE))  # common/keys.py (fds 폴더 = 컨테이너의 /app)
from common.keys import ALERT_TOPIC, FREEZE_TOPIC, HOLD_TOPIC, TRANSFER_TOPIC  # noqa: E402
import crosscheck  # noqa: E402
from config import CROSSCHECK_ENABLED, PROFILE_NAME, TICK_TOPIC  # noqa: E402
from detector import Detector  # noqa: E402

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
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
    alerts, stats = detector.process(rows, batch_id=batch_id)

    # 2-1) L2·L6·L7 경보는 원본 거래를 Spark로 다시 계산해 근거 묶음(recompute)을 붙인다 (판단은 바꾸지 않음, 실패해도 경보는 나감)
    if alerts and CROSSCHECK_ENABLED:
        stats.update(crosscheck.attach(spark, detector.db, alerts, batch_id=batch_id))

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
    # 배치 성공 하트비트를 Redis에 기록 (관제 화면 연동)
    try:
        detector.r.set("fds:heartbeat:spark", int(time.time() * 1000))
    except Exception as e:
        logger.warning(f"spark heartbeat error: {e}")
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
