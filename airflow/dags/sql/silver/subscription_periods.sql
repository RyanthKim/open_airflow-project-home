-- Silver Layer: Subscription period analysis
-- Dependencies: analytics.temp_subscription_periods, analytics.plan_info
-- Output: analytics.subscription_periods
-- Description:
--   Builds the full subscription timeline for each user using a
--   recursive CTE to walk the period chain. Classifies each period
--   with business-level transition labels (trial, paid, upgrade,
--   downgrade, churn) and computes running metrics.

BEGIN;

DELETE FROM analytics.subscription_periods
WHERE period_start >= DATEADD(day, -90, CURRENT_DATE);

INSERT INTO analytics.subscription_periods (
    user_id,
    period_seq,
    plan_id,
    plan_name,
    plan_tier,
    period_start,
    period_end,
    period_days,
    transition_type,
    prev_plan_tier,
    mrr_cents,
    is_trial,
    is_active,
    cumulative_revenue_cents
)
WITH RECURSIVE period_chain (
    user_id, period_seq, plan_id, period_start, period_end,
    transition_type, amount_cents, currency, prev_plan_id
) AS (
    -- Anchor: first subscription event per user
    SELECT
        user_id,
        1               AS period_seq,
        plan_id,
        period_start,
        period_end,
        transition_type,
        amount_cents,
        currency,
        prev_plan_id
    FROM analytics.temp_subscription_periods
    WHERE event_seq = 1
      AND period_start >= DATEADD(day, -90, CURRENT_DATE)

    UNION ALL

    -- Recursive: walk forward through each subsequent period
    SELECT
        t.user_id,
        pc.period_seq + 1   AS period_seq,
        t.plan_id,
        t.period_start,
        t.period_end,
        t.transition_type,
        t.amount_cents,
        t.currency,
        t.prev_plan_id
    FROM analytics.temp_subscription_periods t
    INNER JOIN period_chain pc
        ON  t.user_id   = pc.user_id
        AND t.period_start = pc.period_end
    WHERE pc.period_seq < 50   -- guard against runaway recursion
),

enriched_periods AS (
    SELECT
        pc.user_id,
        pc.period_seq,
        pc.plan_id,
        pi.plan_name,
        pi.plan_tier,
        pc.period_start,
        pc.period_end,
        DATEDIFF(day, pc.period_start, pc.period_end) AS period_days,
        pc.transition_type,
        -- Look up the previous plan tier for context
        prev_pi.plan_tier   AS prev_plan_tier,
        -- Normalize to monthly recurring revenue
        CASE
            WHEN pi.plan_tier = 'free'  THEN 0
            WHEN pc.currency  = 'USD'   THEN pc.amount_cents
            ELSE ROUND(pc.amount_cents * 1.0)   -- placeholder FX
        END AS mrr_cents,
        -- Trial detection
        CASE
            WHEN pc.transition_type = 'new'
                 AND pi.trial_days > 0
                 AND DATEDIFF(day, pc.period_start, pc.period_end) <= pi.trial_days
                THEN TRUE
            ELSE FALSE
        END AS is_trial,
        -- Active flag: not churned and period hasn't ended
        CASE
            WHEN pc.transition_type = 'churn'           THEN FALSE
            WHEN pc.period_end     <= CURRENT_TIMESTAMP THEN FALSE
            ELSE TRUE
        END AS is_active
    FROM period_chain pc
    LEFT JOIN analytics.plan_info pi
        ON pc.plan_id = pi.plan_id
    LEFT JOIN analytics.plan_info prev_pi
        ON pc.prev_plan_id = prev_pi.plan_id
),

with_cumulative AS (
    SELECT
        *,
        SUM(mrr_cents) OVER (
            PARTITION BY user_id
            ORDER BY period_seq
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_revenue_cents
    FROM enriched_periods
)

SELECT
    user_id,
    period_seq,
    plan_id,
    plan_name,
    plan_tier,
    period_start,
    period_end,
    period_days,
    transition_type,
    prev_plan_tier,
    mrr_cents,
    is_trial,
    is_active,
    cumulative_revenue_cents
FROM with_cumulative;

COMMIT;
