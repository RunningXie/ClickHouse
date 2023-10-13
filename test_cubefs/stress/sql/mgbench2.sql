CREATE TABLE if not exists mgbench.logs2  UUID '28f1c61c-2970-457a-bffe-454156ddcfeb'(
  log_time    DateTime,
  client_ip   IPv4,
  request     String,
  status_code UInt16,
  object_size UInt64
)
ENGINE = ReplicatedMergeTree('/tables/mgbench/logs2', '{replica}')
ORDER BY log_time
SETTINGS storage_policy='cubefs_only';