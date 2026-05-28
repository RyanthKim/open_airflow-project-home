-- Silver Layer: User activity tracking
-- Dependencies: analytics.temp_event_logs
-- Output: analytics.user_activity
-- Description:
--   Aggregates raw events into per-user activity summaries.
--   Uses FIRST_VALUE/LAST_VALUE to capture session boundaries
--   and classifies users as active or inactive.

BEGIN;

DELETE FROM analytics.user_activity
WHERE activity_date >= DATEADD(day, -7, CURRENT_DATE);

INSERT INTO analytics.user_activity (
    user_id,
    activity_date,
    session_count,
    total_events,
    first_event_ts,
    last_event_ts,
    first_event_type,
    last_event_type,
    active_seconds,
    activity_status
)
SELECT
    user_id,
    event_date                                      AS activity_date,
    COUNT(DISTINCT
        DATE_PART(hour, event_ts)::INT * 100
        + FLOOR(DATE_PART(minute, event_ts) / 30)
    )                                               AS session_count,
    COUNT(*)                                        AS total_events,
    MIN(event_ts)                                   AS first_event_ts,
    MAX(event_ts)                                   AS last_event_ts,
    FIRST_VALUE(event_type) OVER (
        PARTITION BY user_id, event_date
        ORDER BY event_ts ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                               AS first_event_type,
    LAST_VALUE(event_type) OVER (
        PARTITION BY user_id, event_date
        ORDER BY event_ts ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                               AS last_event_type,
    DATEDIFF(second, MIN(event_ts), MAX(event_ts))  AS active_seconds,
    CASE
        WHEN COUNT(*) >= 10 THEN 'power_user'
        WHEN COUNT(*) >= 3  THEN 'active'
        ELSE 'low_activity'
    END                                             AS activity_status
FROM analytics.temp_event_logs
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE)
  AND user_id > 0
GROUP BY user_id, event_date;

COMMIT;
