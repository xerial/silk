SELECT TD_TIME_FORMAT(time, 'HH', '{{timezone}}') AS "Time",
       usr AS "User",
       COUNT(*) AS "Total sum of Count"
FROM
  (SELECT time,
          coalesce(u, account_id) AS usr,
          account_id
   FROM
     (SELECT time,
             regexp_extract(USER, '([0-9]+)/.*', 1) AS u,
             account_id
      FROM presto_production.query_completion
      WHERE TD_TIME_RANGE(time, TD_TIME_ADD({{start}}, '-1d', '{{timezone}}'))) ss) s
WHERE usr IS NOT NULL
GROUP BY TD_TIME_FORMAT(time, 'HH', '{{timezone}}'), usr
ORDER BY time, usr ASC LIMIT 20000
