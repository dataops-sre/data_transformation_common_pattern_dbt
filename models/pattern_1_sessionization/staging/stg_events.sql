{{ config(materialized='view') }}

SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    metadata,
    date
FROM READ_CSV_AUTO(
    'data/pattern_1_sessionization_raw_data/date=*/events.csv',
    hive_partitioning = TRUE
)
