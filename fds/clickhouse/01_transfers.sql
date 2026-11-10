CREATE DATABASE IF NOT EXISTS aegis;

-- ③ 저장 테이블: 거래 원장 (통과 + 거절). Mongo transfers와 같은 내용, 영구 보관
CREATE TABLE IF NOT EXISTS aegis.transfers
(
    tx_hash     String,
    log_index   Int64,                     -- 값이 없으면 -1
    `from`      String,
    `to`        String,
    amount      Nullable(Decimal(18, 6)),  -- 잘못된 금액(422)은 NULL일 수 있음
    block       UInt64,
    timestamp   DateTime64(6, 'UTC'),      -- 거래 시각
    status      LowCardinality(String),    -- ACCEPTED / REJECTED
    http_code   UInt16,
    reason      LowCardinality(String),    -- 통과면 빈 문자열
    created_at  DateTime64(6, 'UTC'),      -- 거절 시각 (통과 거래는 거래 시각)
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)  -- ClickHouse에 들어온 시각
)
ENGINE = ReplacingMergeTree              -- 같은 메시지가 두 번 오면 나중에 하나로 합침
PARTITION BY toYYYYMM(timestamp)         -- 월별로 나눠 저장
ORDER BY (`from`, timestamp, tx_hash, log_index, status, reason, created_at);

-- ① 입구: Kafka 토픽 두 개를 읽기만 함 (저장 안 함)
CREATE TABLE IF NOT EXISTS aegis.transfers_kafka
(
    tx_hash    String,
    log_index  Nullable(Int64),
    `from`     String,
    `to`       String,
    amount     Nullable(Float64),
    block      Int64,
    timestamp  String,
    http_code  Nullable(UInt16),   -- 아래 4개는 거절 토픽에만 있음
    status     Nullable(String),
    reason     Nullable(String),
    created_at Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'transfer.events.v1,transfer.rejected.v1',
         kafka_group_name = 'clickhouse-transfers',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_skip_broken_messages = 100;

-- ② 옮기기: 새 메시지가 오면 형식을 바꿔 ③에 넣음
CREATE MATERIALIZED VIEW IF NOT EXISTS aegis.transfers_mv TO aegis.transfers AS
SELECT
    k.tx_hash AS tx_hash,
    ifNull(k.log_index, -1) AS log_index,
    k.`from` AS `from`,
    k.`to` AS `to`,
    toDecimal64OrNull(toString(k.amount), 6) AS amount,
    toUInt64(greatest(k.block, 0)) AS block,
    parseDateTime64BestEffort(k.timestamp, 6, 'UTC') AS timestamp,
    if(_topic = 'transfer.rejected.v1', ifNull(k.status, 'REJECTED'), 'ACCEPTED') AS status,
    if(_topic = 'transfer.rejected.v1', ifNull(k.http_code, 0), 200) AS http_code,
    ifNull(k.reason, '') AS reason,
    parseDateTime64BestEffort(coalesce(k.created_at, k.timestamp), 6, 'UTC') AS created_at
FROM aegis.transfers_kafka AS k;