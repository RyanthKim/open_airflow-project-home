-- Gold Layer: Daily revenue metrics
-- Dependencies: analytics.payment_billing, analytics.subscription_periods
-- Output: analytics.daily_revenue_counts
-- Description:
--   Computes daily revenue metrics including MRR, ARR, and breakdowns
--   by subscription tier. Categorizes revenue streams for financial
--   reporting and trend analysis.

BEGIN;

DELETE FROM analytics.daily_revenue_counts
WHERE metric_date >= DATEADD(day, -30, CURRENT_DATE);

INSERT INTO analytics.daily_revenue_counts (
    metric_date,
    total_revenue_cents,
    subscription_revenue_cents,
    addon_revenue_cents,
    refund_cents,
    net_revenue_cents,
    mrr_cents,
    arr_cents,
    free_tier_users,
    standard_tier_users,
    premium_tier_users,
    enterprise_tier_users,
    paying_users,
    arpu_cents
)
WITH daily_billing AS (
    SELECT
        invoice_date                AS metric_date,
        SUM(total_cents)            AS total_revenue_cents,
        SUM(CASE WHEN invoice_category = 'subscription' THEN total_cents ELSE 0 END)
                                    AS subscription_revenue_cents,
        SUM(CASE WHEN invoice_category = 'addon'        THEN total_cents ELSE 0 END)
                                    AS addon_revenue_cents,
        SUM(CASE WHEN invoice_category = 'refund'       THEN ABS(total_cents) ELSE 0 END)
                                    AS refund_cents,
        SUM(COALESCE(net_revenue_cents, 0))
                                    AS net_revenue_cents
    FROM analytics.payment_billing
    WHERE invoice_date >= DATEADD(day, -30, CURRENT_DATE)
      AND payment_status IN ('completed', 'refunded')
    GROUP BY invoice_date
),

daily_mrr AS (
    SELECT
        d.dt                        AS metric_date,
        SUM(sp.mrr_cents)           AS mrr_cents,
        COUNT(DISTINCT CASE WHEN sp.plan_tier = 'free'       THEN sp.user_id END) AS free_tier_users,
        COUNT(DISTINCT CASE WHEN sp.plan_tier = 'standard'   THEN sp.user_id END) AS standard_tier_users,
        COUNT(DISTINCT CASE WHEN sp.plan_tier = 'premium'    THEN sp.user_id END) AS premium_tier_users,
        COUNT(DISTINCT CASE WHEN sp.plan_tier = 'enterprise' THEN sp.user_id END) AS enterprise_tier_users,
        COUNT(DISTINCT CASE WHEN sp.plan_tier != 'free'      THEN sp.user_id END) AS paying_users
    FROM (
        SELECT (DATEADD(day, -30, CURRENT_DATE) + row_number) AS dt
        FROM (SELECT ROW_NUMBER() OVER () - 1 AS row_number FROM stl_scan LIMIT 31)
    ) d
    INNER JOIN analytics.subscription_periods sp
        ON  d.dt BETWEEN DATE(sp.period_start) AND DATE(sp.period_end)
        AND sp.is_active = TRUE
    GROUP BY d.dt
)

SELECT
    COALESCE(b.metric_date, m.metric_date)      AS metric_date,
    COALESCE(b.total_revenue_cents, 0)          AS total_revenue_cents,
    COALESCE(b.subscription_revenue_cents, 0)   AS subscription_revenue_cents,
    COALESCE(b.addon_revenue_cents, 0)          AS addon_revenue_cents,
    COALESCE(b.refund_cents, 0)                 AS refund_cents,
    COALESCE(b.net_revenue_cents, 0)            AS net_revenue_cents,
    COALESCE(m.mrr_cents, 0)                    AS mrr_cents,
    COALESCE(m.mrr_cents, 0) * 12               AS arr_cents,
    COALESCE(m.free_tier_users, 0)              AS free_tier_users,
    COALESCE(m.standard_tier_users, 0)          AS standard_tier_users,
    COALESCE(m.premium_tier_users, 0)           AS premium_tier_users,
    COALESCE(m.enterprise_tier_users, 0)        AS enterprise_tier_users,
    COALESCE(m.paying_users, 0)                 AS paying_users,
    CASE
        WHEN COALESCE(m.paying_users, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(m.mrr_cents, 0)::FLOAT / m.paying_users, 2)
    END                                         AS arpu_cents
FROM daily_billing b
FULL OUTER JOIN daily_mrr m
    ON b.metric_date = m.metric_date;

COMMIT;
