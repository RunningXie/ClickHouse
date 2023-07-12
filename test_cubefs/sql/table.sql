CREATE TABLE IF NOT EXISTS {test_database}.{test_table} UUID '{table_uuid}'
(
    appName String,
    timestamp DateTime64,
    name String,
    value Int64
)
ENGINE = ReplicatedMergeTree('/tables/default/{test_database}.{test_table}', '{replica}')
PARTITION BY appName
ORDER BY timestamp
TTL toDateTime(toUnixTimestamp(timestamp)) + toIntervalSecond({table_ttl_second}) TO VOLUME 'cold'
SETTINGS storage_policy = 'hot_cold_separation', replicated_deduplication_window=0;
