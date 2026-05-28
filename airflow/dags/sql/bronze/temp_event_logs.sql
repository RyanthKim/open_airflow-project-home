-- Bronze Layer: Temporary event log aggregation
-- Dependencies: raw.user_events, raw.system_events, raw.api_events
-- Output: analytics.temp_event_logs
-- Description:
--   Combines multiple event sources into a single staging table.
--   Adds ordering indices and normalizes timestamps from epoch.
--   This table is materialized (not true TEMP) to survive across
--   Airflow task boundaries within a DAG run.

DROP TABLE IF EXISTS analytics.temp_event_logs;

CREATE TABLE analytics.temp_event_logs
DISTKEY (user_id)
SORTKEY (event_ts, event_source)
AS
WITH union_events AS (
    -- User-generated events (clicks, page views, form submissions)
    SELECT
        event_id,
        user_id,
        event_type,
        event_payload,
        epoch_ts,
        'user'          AS event_source,
        created_at      AS source_created_at
    FROM raw.user_events
    WHERE epoch_ts >= EXTRACT(EPOCH FROM DATEADD(day, -3, CURRENT_DATE))

    UNION ALL

    -- System-generated events (cron jobs, batch processes, alerts)
    SELECT
        event_id,
        COALESCE(user_id, -1)   AS user_id,
        event_type,
        event_payload,
        epoch_ts,
        'system'                AS event_source,
        created_at              AS source_created_at
    FROM raw.system_events
    WHERE epoch_ts >= EXTRACT(EPOCH FROM DATEADD(day, -3, CURRENT_DATE))

    UNION ALL

    -- External API events (webhooks, integrations, partner callbacks)
    SELECT
        event_id,
        COALESCE(user_id, -1)   AS user_id,
        event_type,
        event_payload,
        epoch_ts,
        'api'                   AS event_source,
        created_at              AS source_created_at
    FROM raw.api_events
    WHERE epoch_ts >= EXTRACT(EPOCH FROM DATEADD(day, -3, CURRENT_DATE))
),

indexed_events AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_payload,
        event_source,
        -- Convert epoch seconds to timestamp
        TIMESTAMP 'epoch' + epoch_ts * INTERVAL '1 second'     AS event_ts,
        DATE(TIMESTAMP 'epoch' + epoch_ts * INTERVAL '1 second') AS event_date,
        source_created_at,
        -- Forward index: oldest event first per user
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY epoch_ts ASC
        ) AS idx,
        -- Reverse index: newest event first per user
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY epoch_ts DESC
        ) AS idx_inverse,
        -- Total event count per user (useful for filtering)
        COUNT(*) OVER (
            PARTITION BY user_id
        ) AS user_event_count
    FROM union_events
)

SELECT
    event_id,
    user_id,
    event_type,
    event_payload,
    event_source,
    event_ts,
    event_date,
    source_created_at,
    idx,
    idx_inverse,
    user_event_count
FROM indexed_events;
