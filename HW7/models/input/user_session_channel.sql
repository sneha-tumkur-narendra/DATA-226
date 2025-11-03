WITH __dbt__cte__user_session_channel AS (
    SELECT
     userId,
     sessionId,
     channel
    FROM {{ source('raw', 'user_session_channel') }}
    WHERE sessionId IS NOT NULL
)

SELECT *
FROM __dbt__cte__user_session_channel
