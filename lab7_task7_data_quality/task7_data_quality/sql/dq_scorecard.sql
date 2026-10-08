-- Data quality scorecard. Replace the catalog and schema if they differ.

-- 1) latest scorecard by layer and dimension
SELECT * FROM dbr_dev_ua5816bd.lena066636_dq_gold.dq_scorecard_latest ORDER BY layer, dimension;

-- 2) history of failed and warning checks
SELECT run_ts, layer, table_name, dimension, check_name, status, failed_rows, total_rows, details
FROM dbr_dev_ua5816bd.lena066636_dq_gold.dq_results
WHERE status <> 'PASS'
ORDER BY run_ts DESC;

-- 3) pass rate per run
SELECT run_id, min(run_ts) AS run_ts, count(*) AS checks,
       round(count_if(status = 'PASS') * 100.0 / count(*), 1) AS pass_pct
FROM dbr_dev_ua5816bd.lena066636_dq_gold.dq_results
GROUP BY run_id
ORDER BY run_ts DESC;

-- 4) quarantine reasons
SELECT rule, count(*) AS rows
FROM dbr_dev_ua5816bd.lena066636_dq_silver.orders_quarantine
LATERAL VIEW explode(dq_failed_rules) r AS rule
GROUP BY rule
ORDER BY rows DESC;

-- 5) alert query: blocking failures in the latest suite run (alert when failed > 0)
SELECT count_if(status = 'FAIL') AS failed
FROM dbr_dev_ua5816bd.lena066636_dq_gold.dq_results
WHERE run_id = (SELECT max_by(run_id, run_ts) FROM dbr_dev_ua5816bd.lena066636_dq_gold.dq_results WHERE run_id LIKE 'suite-%');
