-- Silver Layer: Plan information master table
-- Dependencies: raw.plans, raw.plan_features, raw.plan_pricing
-- Output: analytics.plan_info
-- Description:
--   Builds a denormalized plan dimension table by joining plan
--   metadata, feature flags, and pricing tiers. Uses DELETE+INSERT
--   for idempotent full refreshes.

BEGIN;

DELETE FROM analytics.plan_info;

INSERT INTO analytics.plan_info (
    plan_id,
    plan_name,
    plan_tier,
    is_active,
    max_seats,
    has_sso,
    has_api_access,
    has_priority_support,
    monthly_price_cents,
    annual_price_cents,
    currency,
    trial_days,
    created_at,
    updated_at
)
WITH plan_base AS (
    SELECT
        p.plan_id,
        p.plan_name,
        COALESCE(p.plan_tier, 'standard')   AS plan_tier,
        COALESCE(p.is_active, TRUE)         AS is_active,
        COALESCE(p.max_seats, 1)            AS max_seats,
        p.created_at,
        p.updated_at
    FROM raw.plans p
    WHERE p.deleted_at IS NULL
),

plan_feature_flags AS (
    SELECT
        plan_id,
        MAX(CASE WHEN feature_key = 'sso'              THEN 1 ELSE 0 END) AS has_sso,
        MAX(CASE WHEN feature_key = 'api_access'        THEN 1 ELSE 0 END) AS has_api_access,
        MAX(CASE WHEN feature_key = 'priority_support'  THEN 1 ELSE 0 END) AS has_priority_support
    FROM raw.plan_features
    GROUP BY plan_id
),

plan_prices AS (
    SELECT
        plan_id,
        MAX(CASE WHEN billing_interval = 'month' THEN price_cents END) AS monthly_price_cents,
        MAX(CASE WHEN billing_interval = 'year'  THEN price_cents END) AS annual_price_cents,
        COALESCE(MAX(currency), 'USD')  AS currency,
        COALESCE(MAX(trial_days), 0)    AS trial_days
    FROM raw.plan_pricing
    WHERE effective_end IS NULL   -- current pricing only
    GROUP BY plan_id
)

SELECT
    b.plan_id,
    b.plan_name,
    b.plan_tier,
    b.is_active,
    b.max_seats,
    COALESCE(f.has_sso, 0)              AS has_sso,
    COALESCE(f.has_api_access, 0)       AS has_api_access,
    COALESCE(f.has_priority_support, 0) AS has_priority_support,
    COALESCE(pp.monthly_price_cents, 0) AS monthly_price_cents,
    COALESCE(pp.annual_price_cents, 0)  AS annual_price_cents,
    COALESCE(pp.currency, 'USD')        AS currency,
    COALESCE(pp.trial_days, 0)          AS trial_days,
    b.created_at,
    b.updated_at
FROM plan_base b
LEFT JOIN plan_feature_flags f  ON b.plan_id = f.plan_id
LEFT JOIN plan_prices pp        ON b.plan_id = pp.plan_id;

COMMIT;
