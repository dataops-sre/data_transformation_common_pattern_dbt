{{ config(materialized='incremental', unique_key='session_id') }}

WITH s_e AS (
    SELECT
        source_data.user_id,
        source_data.session_id,
        COUNT(source_data.event_id) AS number_of_events,
        MIN(source_data.event_time) AS session_start,
        MAX(source_data.event_time) AS session_end
    FROM {{ ref("stg_sessionized_events") }} AS source_data
    GROUP BY source_data.user_id, source_data.session_id
    --{% if is_incremental() %}

    --{% endif %}
)

SELECT * FROM s_e
