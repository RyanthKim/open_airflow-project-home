-- Silver Layer: Payment and billing data
-- Dependencies: raw.invoices, raw.invoice_line_items, raw.payment_methods
-- Output: analytics.payment_billing
-- Description:
--   Aggregates invoice and payment data with categorization by
--   invoice type (subscription, addon, refund). Handles multiple
--   currencies and tracks payment status lifecycle.

BEGIN;

DELETE FROM analytics.payment_billing
WHERE invoice_date >= DATEADD(day, -30, CURRENT_DATE);

INSERT INTO analytics.payment_billing (
    invoice_id,
    user_id,
    invoice_date,
    invoice_category,
    line_item_count,
    subtotal_cents,
    tax_cents,
    total_cents,
    currency,
    payment_status,
    payment_method_type,
    paid_at,
    refunded_at,
    net_revenue_cents
)
WITH categorized_items AS (
    SELECT
        li.invoice_id,
        li.line_item_id,
        li.amount_cents,
        li.quantity,
        li.amount_cents * li.quantity AS line_total_cents,
        CASE
            WHEN li.item_type = 'subscription'  THEN 'subscription'
            WHEN li.item_type = 'addon'         THEN 'addon'
            WHEN li.item_type = 'credit'        THEN 'refund'
            WHEN li.amount_cents < 0            THEN 'refund'
            ELSE 'other'
        END AS invoice_category
    FROM raw.invoice_line_items li
),

invoice_summary AS (
    SELECT
        ci.invoice_id,
        -- Use the dominant category (subscription > addon > refund > other)
        CASE
            WHEN SUM(CASE WHEN ci.invoice_category = 'subscription' THEN 1 ELSE 0 END) > 0
                THEN 'subscription'
            WHEN SUM(CASE WHEN ci.invoice_category = 'addon' THEN 1 ELSE 0 END) > 0
                THEN 'addon'
            WHEN SUM(CASE WHEN ci.invoice_category = 'refund' THEN 1 ELSE 0 END) > 0
                THEN 'refund'
            ELSE 'other'
        END AS invoice_category,
        COUNT(*)                    AS line_item_count,
        SUM(ci.line_total_cents)    AS subtotal_cents
    FROM categorized_items ci
    GROUP BY ci.invoice_id
),

payment_info AS (
    SELECT
        i.invoice_id,
        i.user_id,
        DATE(i.invoice_date)    AS invoice_date,
        i.tax_cents,
        i.currency,
        i.status                AS payment_status,
        i.paid_at,
        i.refunded_at,
        pm.method_type          AS payment_method_type
    FROM raw.invoices i
    LEFT JOIN raw.payment_methods pm
        ON i.payment_method_id = pm.payment_method_id
    WHERE i.invoice_date >= DATEADD(day, -30, CURRENT_DATE)
)

SELECT
    p.invoice_id,
    p.user_id,
    p.invoice_date,
    s.invoice_category,
    s.line_item_count,
    s.subtotal_cents,
    COALESCE(p.tax_cents, 0)                                        AS tax_cents,
    s.subtotal_cents + COALESCE(p.tax_cents, 0)                     AS total_cents,
    COALESCE(p.currency, 'USD')                                     AS currency,
    p.payment_status,
    COALESCE(p.payment_method_type, 'unknown')                      AS payment_method_type,
    p.paid_at,
    p.refunded_at,
    -- Net revenue: full amount if paid, zero if refunded, null if pending
    CASE
        WHEN p.payment_status = 'refunded'  THEN 0
        WHEN p.payment_status = 'completed' THEN s.subtotal_cents + COALESCE(p.tax_cents, 0)
        ELSE NULL
    END AS net_revenue_cents
FROM payment_info p
INNER JOIN invoice_summary s
    ON p.invoice_id = s.invoice_id;

COMMIT;
