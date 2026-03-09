{{ config(materialized='incremental', unique_key='event_id') }}

SELECT
    source_events.event_id,
    source_events.user_id,
    source_events.event_time,
    source_events.event_type,
    source_events.metadata,
    source_events.date
FROM {{ ref('stg_events') }} AS source_events

{% if is_incremental() %}
    WHERE source_events.date > (
        SELECT MAX(target.date)
        FROM {{ this }} AS target
    )
{% endif %}
