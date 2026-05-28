-- Gold Layer: Daily user count metrics
-- Dependencies: analytics.user_activity, analytics.subscription_periods
-- Output: analytics.daily_user_counts
-- Description:
--   Produces a daily snapshot of user counts including new signups,
--   returning users, and cumulative totals. Uses LEAD() for
--   next-day comparison to detect growth trends.

BEGIN;

DELETE FROM analytics.daily_user_counts
WHERE metric_date >= DATEADD(day, -30, CURRENT_DATE);

INSERT INTO analytics.daily_user_counts (
    metric_date,
    new_users,
    returning_users,
    total_active_users,
    churned_users,
    cumulative_users,
    next_day_active_users,
    dod_change_pct
)
WITH daily_base AS (
    SELECT
        a.activity_date                 AS metric_date,
        COUNT(DISTINCT CASE
            WHEN sp.period_seq = 1
                 AND DATE(sp.period_start) = a.activity_date
            THEN a.user_id
        END)                            AS new_users,
        COUNT(DISTINCT CASE
            WHEN sp.period_seq > 1
                 OR DATE(sp.period_start) < a.activity_date
            THEN a.user_id
        END)                            AS returning_users,
        COUNT(DISTINCT a.user_id)       AS total_active_users,
        COUNT(DISTINCT CASE
            WHEN sp.transition_type = 'churn'
                 AND DATE(sp.period_end) = a.activity_date
            THEN a.user_id
        END)                            AS churned_users
    FROM analytics.user_activity a
    LEFT JOIN analytics.subscription_periods sp
        ON a.user_id = sp.user_id
        AND a.activity_date BETWEEN DATE(sp.period_start) AND DATE(sp.period_end)
    WHERE a.activity_date >= DATEADD(day, -30, CURRENT_DATE)
    GROUP BY a.activity_date
),

with_cumulative AS (
    SELECT
        metric_date,
        new_users,
        returning_users,
        total_active_users,
        churned_users,
        SUM(new_users) OVER (
            ORDER BY metric_date
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_users,
        LEAD(total_active_users) OVER (
            ORDER BY metric_date
        ) AS next_day_active_users
    FROM daily_base
)

SELECT
    metric_date,
    new_users,
    returning_users,
    total_active_users,
    churned_users,
    cumulative_users,
    next_day_active_users,
    CASE
        WHEN total_active_users = 0 THEN 0.00
        ELSE ROUND(
            (next_day_active_users - total_active_users)::FLOAT
            / total_active_users * 100, 2
        )
    END AS dod_change_pct
FROM with_cumulative;

COMMIT;
