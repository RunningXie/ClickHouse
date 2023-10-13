CREATE TABLE if not exists mgbench.logs3 UUID '28f1c61c-2970-457a-bffe-454156ddcfec' (
  log_time     DateTime64,
  device_id    FixedString(15),
  device_name  LowCardinality(String),
  device_type  LowCardinality(String),
  device_floor UInt8,
  event_type   LowCardinality(String),
  event_unit   FixedString(1),
  event_value  Nullable(Float32)
)
ENGINE = ReplicatedMergeTree('/tables/mgbench/logs3', '{replica}')
ORDER BY (event_type, log_time)
SETTINGS storage_policy='cubefs_only';