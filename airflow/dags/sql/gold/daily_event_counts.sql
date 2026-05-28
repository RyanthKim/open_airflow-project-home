-- Gold Layer: Daily event count metrics
-- Dependencies: analytics.temp_event_logs
-- Output: analytics.daily_event_counts
-- Description:
--   Pivots event counts by type into a daily summary table.
--   Includes day-over-day comparison for trend detection.

BEGIN;

DELETE FROM analytics.daily_event_counts
WHERE metric_date >= DATEADD(day, -30, CURRENT_DATE);

INSERT INTO analytics.daily_event_counts (
    metric_date,
    total_events,
    user_events,
    system_events,
    api_events,
    unique_users,
    prev_day_total_events,
    dod_change_pct
)
WITH daily_pivot AS (
    SELECT
        event_date                                                          AS metric_date,
        COUNT(*)                                                            AS total_events,
        SUM(CASE WHEN event_source = 'user'   THEN 1 ELSE 0 END)           AS user_events,
        SUM(CASE WHEN event_source = 'system' THEN 1 ELSE 0 END)           AS system_events,
        SUM(CASE WHEN event_source = 'api'    THEN 1 ELSE 0 END)           AS api_events,
        COUNT(DISTINCT CASE WHEN user_id > 0 THEN user_id END)             AS unique_users
    FROM analytics.temp_event_logs
    WHERE event_date >= DATEADD(day, -30, CURRENT_DATE)
    GROUP BY event_date
),

with_comparison AS (
    SELECT
        metric_date,
        total_events,
        user_events,
        system_events,
        api_events,
        unique_users,
        LAG(total_events) OVER (ORDER BY metric_date) AS prev_day_total_events
    FROM daily_pivot
)

SELECT
    metric_date,
    total_events,
    user_events,
    system_events,
    api_events,
    unique_users,
    COALESCE(prev_day_total_events, 0)  AS prev_day_total_events,
    CASE
        WHEN prev_day_total_events IS NULL OR prev_day_total_events = 0 THEN 0.00
        ELSE ROUND(
            (total_events - prev_day_total_events)::FLOAT
            / prev_day_total_events * 100, 2
        )
    END AS dod_change_pct
FROM with_comparison;

COMMIT;
