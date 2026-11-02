-- CRM Analytics monitoring: saved queries for dashboards and SQL alerts.
-- Run collect_monitoring_events() in a separate scheduled job first.
-- Run each query separately. Named parameters:
--   monitoring_table: fully qualified Delta event table (catalog.schema.table)
--   pipeline_id: the Lakeflow pipeline UUID
--   dataset_id: expected Salesforce dataset ID (also detects missing events)
--   alert_window_minutes: positive integer, e.g. 15
--   stalled_after_minutes: positive integer longer than the read budget, e.g. 45
--   freshness_sla_seconds: positive source-version age limit, e.g. 86400
-- For scheduled SQL alerts, replace the selected query's named parameters with
-- reviewed literals; do not depend on interactive dashboard parameter bindings.
-- completed means validated SOURCE extraction. target_committed is always false.
-- Retries create separate attempts. Never sum their row counts as target rows.
-- Keep the event table/checkpoint separate from business targets and checkpoints.

-- 1. Latest lifecycle event for each extraction attempt (dashboard table).
WITH ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY attempt_id ORDER BY event_time DESC,
      CASE status WHEN 'failed' THEN 3 WHEN 'cancelled' THEN 2
                  WHEN 'completed' THEN 1 ELSE 0 END DESC, event_id DESC
  ) AS event_rank
  FROM IDENTIFIER(:monitoring_table)
)
SELECT dataset_id, version_id, attempt_id, status, event_time,
       expected_row_count, extracted_row_count, elapsed_seconds,
       extracted_row_count / nullif(elapsed_seconds, 0) AS rows_per_second,
       http_requests, query_requests, retry_count, retry_wait_seconds,
       page_queries, peak_page_rows, source_created_at, source_age_seconds,
       error_code, target_committed
FROM ranked
WHERE event_rank = 1
ORDER BY event_time DESC;

-- 2. Hourly extraction reliability and performance (dashboard time series).
WITH ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY attempt_id ORDER BY event_time DESC,
      CASE status WHEN 'failed' THEN 3 WHEN 'cancelled' THEN 2
                  WHEN 'completed' THEN 1 ELSE 0 END DESC, event_id DESC
  ) AS event_rank
  FROM IDENTIFIER(:monitoring_table)
)
SELECT date_trunc('hour', event_time) AS hour, dataset_id,
       count_if(status = 'completed') AS completed_extractions,
       count_if(status = 'failed') AS failed_attempts,
       count_if(status = 'cancelled') AS cancelled_attempts,
       avg(CASE WHEN status = 'completed' THEN elapsed_seconds END) AS avg_read_seconds,
       avg(CASE WHEN status = 'completed'
                THEN extracted_row_count / nullif(elapsed_seconds, 0) END) AS avg_rows_per_second,
       sum(retry_count) AS retries, sum(query_requests) AS query_requests
FROM ranked
WHERE event_rank = 1 AND status IN ('completed', 'failed', 'cancelled')
GROUP BY date_trunc('hour', event_time), dataset_id
ORDER BY hour DESC, dataset_id;

-- 3. SQL alert: failures > 0. Configure schedule and notification destination
-- separately; no messages or alerts are created by this file.
SELECT count(*) AS failures
FROM IDENTIFIER(:monitoring_table)
WHERE status = 'failed'
  AND event_time >= timestampadd(MINUTE, -CAST(:alert_window_minutes AS INT), current_timestamp());

-- 4. SQL alert: authentication_failures > 0. Repeated 401 requires the UC
-- connection owner to reauthorize as the same Salesforce integration user.
SELECT count(*) AS authentication_failures
FROM IDENTIFIER(:monitoring_table)
WHERE status = 'failed' AND error_code = 'AUTHENTICATION'
  AND event_time >= timestampadd(MINUTE, -CAST(:alert_window_minutes AS INT), current_timestamp());

-- 5. SQL alert: missing_or_old_source_version = 1. This checks the publication
-- age of the last fully extracted source version, not a Delta commit or a
-- scheduling heartbeat. Unchanged-version skips produce no extraction events.
-- Set an SLA appropriate to the Salesforce refresh schedule. Create one saved
-- query per expected dataset or join an operator-maintained expected-dataset list.
WITH ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY attempt_id ORDER BY event_time DESC,
      CASE status WHEN 'failed' THEN 3 WHEN 'cancelled' THEN 2
                  WHEN 'completed' THEN 1 ELSE 0 END DESC, event_id DESC
  ) AS event_rank
  FROM IDENTIFIER(:monitoring_table)
  WHERE dataset_id = :dataset_id
), source_version AS (
  SELECT max_by(source_created_at, event_time) AS last_source_created_at,
         max(event_time) AS last_extraction_at
  FROM ranked
  WHERE event_rank = 1 AND status = 'completed'
)
SELECT last_source_created_at, last_extraction_at,
       CASE WHEN last_source_created_at IS NULL
              OR last_source_created_at < timestampadd(
                SECOND, -CAST(:freshness_sla_seconds AS INT), current_timestamp())
            THEN 1 ELSE 0 END AS missing_or_old_source_version
FROM source_version;

-- 6. SQL alert: unfinished_attempts > 0. Hard process termination can leave
-- only a started event; this can also mean a still-running slow extraction.
WITH ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY attempt_id ORDER BY event_time DESC,
      CASE status WHEN 'failed' THEN 3 WHEN 'cancelled' THEN 2
                  WHEN 'completed' THEN 1 ELSE 0 END DESC, event_id DESC
  ) AS event_rank
  FROM IDENTIFIER(:monitoring_table)
)
SELECT count(*) AS unfinished_attempts
FROM ranked
WHERE event_rank = 1 AND status = 'started'
  AND started_at < timestampadd(MINUTE, -CAST(:stalled_after_minutes AS INT), current_timestamp());

-- 7. Authoritative pipeline lifecycle (dashboard table). Event-log visibility
-- requires the appropriate pipeline permissions. Pipeline state does not by
-- itself correlate an individual extraction attempt with a target commit.
SELECT timestamp AS event_time, origin.update_id AS update_id,
       details:update_progress.state::STRING AS pipeline_state
FROM event_log(:pipeline_id)
WHERE event_type = 'update_progress'
ORDER BY timestamp DESC;

-- 8. SQL alert: no_recent_completed_update = 1. Includes successful updates
-- which skipped unchanged versions. Choose a window longer than the pipeline
-- schedule plus normal runtime. Collector-job failures need their own Job alert.
SELECT CASE WHEN max(timestamp) IS NULL
                  OR max(timestamp) < timestampadd(
                    MINUTE, -CAST(:alert_window_minutes AS INT), current_timestamp())
            THEN 1 ELSE 0 END AS no_recent_completed_update
FROM event_log(:pipeline_id)
WHERE event_type = 'update_progress'
  AND details:update_progress.state::STRING = 'COMPLETED';

-- 9. SQL alert: failed_pipeline_updates > 0. Includes failures before extraction
-- starts, such as UC injection or schema discovery errors with no attempt file.
SELECT count(DISTINCT origin.update_id) AS failed_pipeline_updates
FROM event_log(:pipeline_id)
WHERE event_type = 'update_progress'
  AND details:update_progress.state::STRING = 'FAILED'
  AND timestamp >= timestampadd(MINUTE, -CAST(:alert_window_minutes AS INT), current_timestamp());
