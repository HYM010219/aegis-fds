-- 동결 이력. 지금은 경보 소비자의 자동 동결(L1·L2·L3·L8)만 들어오고,
-- 나중에 웹의 직원 동결(source=MANUAL)과 해제(event=UNFREEZE)도 같은 토픽·형식으로 보내면 그대로 쌓임

-- ③ 저장 테이블
CREATE TABLE IF NOT EXISTS aegis.freezes
(
    event       LowCardinality(String),         -- FREEZE / UNFREEZE
    wallet      String,
    rule        LowCardinality(String),         -- 동결 사유 룰 (L1~L8)
    source      LowCardinality(String),         -- AUTO / MANUAL
    operator    String,                         -- 직원 ID (자동 동결은 빈 문자열)
    event_at    DateTime64(6, 'UTC'),           -- 동결(해제) 시각
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree                     -- 같은 메시지가 두 번 오면 나중에 하나로 합침
PARTITION BY toYYYYMM(event_at)
ORDER BY (wallet, event_at, event, rule);

-- ① 입구: fds.freezes.v1을 읽기만 함
CREATE TABLE IF NOT EXISTS aegis.freezes_kafka
(
    event       String,
    wallet      String,
    rule        Nullable(String),
    source      Nullable(String),
    operator    Nullable(String),
    frozen_at   Nullable(String),
    unfrozen_at Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'fds.freezes.v1',
         kafka_group_name = 'clickhouse-freezes',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_skip_broken_messages = 100;

-- ② 옮기기. 시각 필드가 없으면 Kafka 메시지 시각을 씀
CREATE MATERIALIZED VIEW IF NOT EXISTS aegis.freezes_mv TO aegis.freezes AS
SELECT
    k.event AS event,
    k.wallet AS wallet,
    ifNull(k.rule, '') AS rule,
    ifNull(k.source, 'AUTO') AS source,
    ifNull(k.operator, '') AS operator,
    ifNull(parseDateTime64BestEffortOrNull(coalesce(k.frozen_at, k.unfrozen_at), 6, 'UTC'),
           toDateTime64(ifNull(_timestamp_ms, now64(3)), 6, 'UTC')) AS event_at
FROM aegis.freezes_kafka AS k;
