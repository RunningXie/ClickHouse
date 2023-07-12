SELECT dt,
       COUNT(DISTINCT client_ip)
FROM (
  SELECT CAST(log_time AS DATE) AS dt,
         client_ip
  FROM mgbench.logs2
) AS r
GROUP BY dt
ORDER BY dt;