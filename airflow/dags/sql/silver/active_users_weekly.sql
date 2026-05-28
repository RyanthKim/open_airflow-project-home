-- Silver Layer: Weekly active user aggregation
-- Dependencies: analytics.user_activity
-- Output: analytics.active_users_weekly
-- Description:
--   Computes weekly active user counts with rolling 7-day windows.
--   Breaks down by activity tier and plan tier for segmentation.

BEGIN;

DELETE FROM analytics.active_users_weekly
WHERE week_start >= DATE_TRUNC('week', DATEADD(day, -28, CURRENT_DATE));

INSERT INTO analytics.active_users_weekly (
    week_start,
    week_end,
    total_active_users,
    power_users,
    regular_active_users,
    low_activity_users,
    avg_sessions_per_user,
    avg_events_per_user
)
WITH daily_stats AS (
    SELECT
        DATE_TRUNC('week', activity_date)   AS week_start,
        user_id,
        activity_status,
        SUM(session_count)                  AS weekly_sessions,
        SUM(total_events)                   AS weekly_events
    FROM analytics.user_activity
    WHERE activity_date >= DATE_TRUNC('week', DATEADD(day, -28, CURRENT_DATE))
    GROUP BY DATE_TRUNC('week', activity_date), user_id, activity_status
)

SELECT
    week_start,
    DATEADD(day, 6, week_start)                                                     AS week_end,
    COUNT(DISTINCT user_id)                                                         AS total_active_users,
    COUNT(DISTINCT CASE WHEN activity_status = 'power_user'    THEN user_id END)    AS power_users,
    COUNT(DISTINCT CASE WHEN activity_status = 'active'        THEN user_id END)    AS regular_active_users,
    COUNT(DISTINCT CASE WHEN activity_status = 'low_activity'  THEN user_id END)    AS low_activity_users,
    ROUND(AVG(weekly_sessions), 2)                                                  AS avg_sessions_per_user,
    ROUND(AVG(weekly_events), 2)                                                    AS avg_events_per_user
FROM daily_stats
GROUP BY week_start;

COMMIT;
