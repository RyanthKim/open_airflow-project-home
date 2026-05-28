-- Bronze Layer: Subscription period staging
-- Dependencies: analytics.temp_event_logs, raw.billing_transactions
-- Output: analytics.temp_subscription_periods
-- Description:
--   Joins event logs with billing data to derive subscription
--   state transitions. Uses LAG/LEAD to detect plan changes
--   and classify each transition type.

DROP TABLE IF EXISTS analytics.temp_subscription_periods;

CREATE TABLE analytics.temp_subscription_periods
DISTKEY (user_id)
SORTKEY (user_id, period_start)
AS
WITH subscription_events AS (
    SELECT
        e.user_id,
        e.event_ts,
        e.event_type,
        b.plan_id,
        b.amount_cents,
        b.currency,
        b.transaction_status,
        ROW_NUMBER() OVER (
            PARTITION BY e.user_id
            ORDER BY e.event_ts ASC
        ) AS event_seq
    FROM analytics.temp_event_logs e
    INNER JOIN raw.billing_transactions b
        ON e.user_id = b.user_id
        AND e.event_id = b.reference_event_id
    WHERE e.event_type IN (
        'subscription_created',
        'subscription_upgraded',
        'subscription_downgraded',
        'subscription_cancelled',
        'subscription_renewed'
    )
),

with_transitions AS (
    SELECT
        user_id,
        event_ts        AS period_start,
        event_type,
        plan_id,
        amount_cents,
        currency,
        transaction_status,
        event_seq,
        LAG(plan_id) OVER (
            PARTITION BY user_id ORDER BY event_seq
        ) AS prev_plan_id,
        LEAD(event_ts) OVER (
            PARTITION BY user_id ORDER BY event_seq
        ) AS period_end,
        CASE
            WHEN event_type = 'subscription_created'
                 AND LAG(event_type) OVER (PARTITION BY user_id ORDER BY event_seq) IS NULL
                THEN 'new'
            WHEN event_type = 'subscription_upgraded'   THEN 'upgrade'
            WHEN event_type = 'subscription_downgraded' THEN 'downgrade'
            WHEN event_type = 'subscription_cancelled'  THEN 'churn'
            WHEN event_type = 'subscription_renewed'    THEN 'renewal'
            ELSE 'other'
        END AS transition_type
    FROM subscription_events
)

SELECT
    user_id,
    plan_id,
    prev_plan_id,
    period_start,
    COALESCE(period_end, CURRENT_TIMESTAMP) AS period_end,
    transition_type,
    amount_cents,
    currency,
    transaction_status,
    event_seq,
    DATEDIFF(day, period_start, COALESCE(period_end, CURRENT_TIMESTAMP)) AS period_days
FROM with_transitions;
