-- 1단계 보류(R01~R06, HOLD)와 R07 차단(BLOCK) 기록. Mongo fds_holds는 14일 뒤 지워지므로 여기 영구 보관

-- ③ 저장 테이블
CREATE TABLE IF NOT EXISTS aegis.holds
(
    event        LowCardinality(String),               -- HOLD / BLOCK
    wallet       String,                               -- 보류(차단)된 지갑
    rule         LowCardinality(String),               -- 대표 룰 (R01~R07)
    rules        Array(LowCardinality(String)),        -- 같이 걸린 룰 전체 (BLOCK은 빈 배열)
    side         LowCardinality(String),               -- SENDER / RECEIVER (BLOCK은 빈 문자열)
    reason       LowCardinality(String),               -- BLOCK 사유 (HOLD는 빈 문자열)
    evidence     String,                               -- 근거 JSON 문자열
    held_at      DateTime64(6, 'UTC'),                 -- 보류 시각 (BLOCK은 Kafka 메시지 시각)
    hold_until   Nullable(DateTime64(6, 'UTC')),       -- 보류 해제 예정 시각 (BLOCK은 NULL)
    tx_hash      String,                               -- 보류를 일으킨 거래
    log_index    Int64,                                -- 값이 없으면 -1
    `from`       String,
    `to`         String,
    amount       Nullable(Decimal(18, 6)),
    block        UInt64,
    tx_timestamp DateTime64(6, 'UTC'),                 -- 거래 시각
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree                            -- 같은 메시지가 두 번 오면 나중에 하나로 합침
PARTITION BY toYYYYMM(held_at)
ORDER BY (wallet, held_at, event, rule, tx_hash, log_index);

-- ① 입구: fds.holds.v1을 읽기만 함
CREATE TABLE IF NOT EXISTS aegis.holds_kafka
(
    event      String,
    wallet     String,
    rule       String,
    rules      Array(String),        -- BLOCK에는 없음 → 빈 배열
    side       Nullable(String),
    reason     Nullable(String),
    evidence   Nullable(String),     -- JSON 객체를 문자열로 받음
    held_at    Nullable(String),
    hold_until Nullable(String),
    tx_hash    String,
    log_index  Nullable(Int64),
    `from`     String,
    `to`       String,
    amount     Nullable(Float64),
    block      Int64,
    timestamp  String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'fds.holds.v1',
         kafka_group_name = 'clickhouse-holds',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_skip_broken_messages = 100;

-- ② 옮기기
CREATE MATERIALIZED VIEW IF NOT EXISTS aegis.holds_mv TO aegis.holds AS
SELECT
    k.event AS event,
    k.wallet AS wallet,
    k.rule AS rule,
    k.rules AS rules,
    ifNull(k.side, '') AS side,
    ifNull(k.reason, '') AS reason,
    ifNull(k.evidence, '{}') AS evidence,
    ifNull(parseDateTime64BestEffortOrNull(k.held_at, 6, 'UTC'),
           toDateTime64(ifNull(_timestamp_ms, now64(3)), 6, 'UTC')) AS held_at,
    parseDateTime64BestEffortOrNull(k.hold_until, 6, 'UTC') AS hold_until,
    k.tx_hash AS tx_hash,
    ifNull(k.log_index, -1) AS log_index,
    k.`from` AS `from`,
    k.`to` AS `to`,
    toDecimal64OrNull(toString(k.amount), 6) AS amount,
    toUInt64(greatest(k.block, 0)) AS block,
    parseDateTime64BestEffort(k.timestamp, 6, 'UTC') AS tx_timestamp
FROM aegis.holds_kafka AS k;
