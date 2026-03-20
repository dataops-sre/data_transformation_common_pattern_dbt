{{ config(materialized='incremental', unique_key='event_id') }}

--event_id,user_id,event_time,event_type,metadata
--f2c4737f-890d-4e3e-9466-c7b3e8e0298e,user_infrequent_66,2026-02-18 17:39:00,scroll,path_/3
--3ba3f518-7d49-4adf-b547-5136b415def2,user_infrequent_75,2026-02-18 16:08:00,page_view,path_/3
WITH s AS (
    SELECT
        source_data.event_id,
        source_data.user_id,
        source_data.event_type,
        source_data.metadata,
        source_data.event_time,
        DATEDIFF(
            'minute',
            source_data.event_time,
            LAG(source_data.event_time) OVER (
                PARTITION BY source_data.user_id
                ORDER BY source_data.event_time
            )
        ) AS d_diff,

        CASE
            WHEN d_diff IS NULL OR d_diff > 30 THEN 1
            ELSE 0
        END AS is_new_session
    FROM {{ ref('raw_events') }} AS source_data

    {% if is_incremental() %}
        WHERE source_data.event_time >= (
            SELECT MAX(target.event_time)
            FROM {{ this }} AS target
        )
    {% endif %}
),

u AS (
    SELECT
        event_id,
        user_id,
        event_type,
        metadata,
        event_time,
        SUM(is_new_session) OVER (
            PARTITION BY user_id ORDER BY event_time
        ) AS session_group

    FROM s
),

t AS (
    SELECT
        event_id,
        user_id,
        event_type,
        metadata,
        event_time,
        DATEDIFF(
            'minute',
            event_time,
            MIN(event_time) OVER (
                PARTITION BY user_id, session_group ORDER BY event_time
            )
        ) AS minutes_from_start,
        MD5(
            CONCAT(
                user_id,
                '_',
                session_group,
                '_',
                FLOOR(minutes_from_start / 480)
            )
        ) AS session_id
    FROM u
)

SELECT
    event_id,
    user_id,
    event_type,
    metadata,
    event_time,
    session_id
FROM t
