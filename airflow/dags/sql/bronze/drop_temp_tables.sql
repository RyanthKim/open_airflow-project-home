-- Bronze Layer: Cleanup temporary tables
-- Description:
--   Drops all intermediate staging tables created during the
--   bronze layer processing. Run as the final task in the DAG.

DROP TABLE IF EXISTS analytics.temp_event_logs;
DROP TABLE IF EXISTS analytics.temp_subscription_periods;
