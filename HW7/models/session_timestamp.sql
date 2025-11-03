WITH __dbt__cte__session_timestamp AS (
    SELECT
     sessionId,
     ts
    FROM {{ source('raw', 'session_timestamp') }}
    WHERE sessionId IS NOT NULL
)

SELECT * FROM __dbt__cte__session_timestamp
