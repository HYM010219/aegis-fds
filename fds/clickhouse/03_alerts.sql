-- Spark 2단계 경보(L1~L8) 원본. 같은 지갑 경보를 케이스로 합치기 전 기록이라 룰별·일별 통계에 씀

-- ③ 저장 테이블
CREATE TABLE IF NOT EXISTS aegis.alerts
(
    rule_id              LowCardinality(String),        -- L1~L8
    rule_name            LowCardinality(String),
    violator_address     String,                        -- 걸린 지갑
    counterparty_address String,                        -- 상대 지갑 (없으면 빈 문자열)
    risk_level           LowCardinality(String),        -- L5 등급 (CRITICAL/HIGH/MEDIUM, 없으면 빈 문자열)
    tx_hashes            Array(String),                 -- 근거 거래 (최대 MAX_HASHES개)
    first_tx_at          DateTime64(3, 'UTC'),
    last_tx_at           DateTime64(3, 'UTC'),
    evidence             String,                        -- 근거 JSON 문자열
    period_profile       LowCardinality(String),        -- demo / prod
    alerted_at           DateTime64(3, 'UTC'),          -- 경보 시각 (Kafka 메시지 시각)
    ingested_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree                             -- 같은 경보가 두 번 오면 나중에 하나로 합침
PARTITION BY toYYYYMM(alerted_at)
ORDER BY (violator_address, rule_id, first_tx_at, last_tx_at, counterparty_address, risk_level);

-- ① 입구: fds.alerts.v1을 읽기만 함
CREATE TABLE IF NOT EXISTS aegis.alerts_kafka
(
    rule_id              String,
    rule_name            Nullable(String),
    violator_address     String,
    counterparty_address Nullable(String),
    risk_level           Nullable(String),
    tx_hashes            Array(String),
    first_tx_at          String,
    last_tx_at           String,
    evidence             Nullable(String),   -- JSON 객체를 문자열로 받음
    period_profile       Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'fds.alerts.v1',
         kafka_group_name = 'clickhouse-alerts',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_skip_broken_messages = 100;

-- ② 옮기기. 토픽에 예전 형식(R01, R06 등) 경보가 남아 있어 L로 시작하는 룰만 저장
CREATE MATERIALIZED VIEW IF NOT EXISTS aegis.alerts_mv TO aegis.alerts AS
SELECT
    k.rule_id AS rule_id,
    ifNull(k.rule_name, '') AS rule_name,
    k.violator_address AS violator_address,
    ifNull(k.counterparty_address, '') AS counterparty_address,
    ifNull(k.risk_level, '') AS risk_level,
    k.tx_hashes AS tx_hashes,
    parseDateTime64BestEffort(k.first_tx_at, 3, 'UTC') AS first_tx_at,
    parseDateTime64BestEffort(k.last_tx_at, 3, 'UTC') AS last_tx_at,
    ifNull(k.evidence, '{}') AS evidence,
    ifNull(k.period_profile, '') AS period_profile,
    toDateTime64(ifNull(_timestamp_ms, now64(3)), 3, 'UTC') AS alerted_at
FROM aegis.alerts_kafka AS k
WHERE startsWith(k.rule_id, 'L');
