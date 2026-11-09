import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from rules import RULES

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transfer.events.v1")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "fds.alerts.v1")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/checkpoints/alerts")
# 체크포인트가 없는 첫 시작에서만 쓰인다. latest면 시작 이후 들어온 거래만 본다.
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
WATERMARK = "1 minute"  # 늦게 온 거래를 기다리는 시간

# Kafka 메시지 규격. 필드가 바뀌면 여기만 고친다.
SCHEMA = StructType([
    StructField("tx_hash", StringType()),
    StructField("log_index", IntegerType()),
    StructField("from", StringType()),
    StructField("to", StringType()),
    StructField("amount", DecimalType(18, 6)),
    StructField("block", LongType()),
    StructField("timestamp", TimestampType()),
])

spark = (
    SparkSession.builder.appName("aegis-fds")
    .config("spark.sql.shuffle.partitions", "4")  # 기본값 200은 컨테이너 1개에 너무 많다
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Kafka에서 읽기
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

# 2) 메시지 본문(JSON)을 필드로 풀기
parsed = raw.select(
    from_json(col("value").cast("string"), SCHEMA).alias("event")
).select("event.*")

# 3) 룰 계산 전 공통 정리
events = (
    parsed.dropna(subset=["tx_hash", "from", "to", "amount", "timestamp"])  # 잘못된 메시지 제외
    .withWatermark("timestamp", WATERMARK)
    .dropDuplicatesWithinWatermark(["tx_hash", "log_index"])  # Publisher 재전송 제거
)

# 4) 룰마다 계산하고 적발 결과를 경보 토픽으로 보내기
for name, (detect, output_mode) in RULES.items():
    alerts = detect(events)
    (
        # key를 지갑 주소로 두면 같은 지갑의 경보는 같은 파티션에 순서대로 들어간다
        alerts.select(
            col("violator_address").alias("key"),
            to_json(struct(*alerts.columns)).alias("value"),
        )
        .writeStream.queryName(name)
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERT_TOPIC)
        .outputMode(output_mode)
        .option("checkpointLocation", f"{CHECKPOINT}/{name}")
        .trigger(processingTime="1 second")
        .start()
    )

spark.streams.awaitAnyTermination()
